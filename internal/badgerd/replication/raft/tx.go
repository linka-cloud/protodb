// Copyright 2023 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	"github.com/planetscale/vtprotobuf/protohelpers"
	"go.linka.cloud/grpc-toolkit/logger"

	"go.linka.cloud/protodb/internal/badgerd/pending"

	"go.linka.cloud/protodb/internal/badgerd/replication"

	"go.linka.cloud/protodb/internal/badgerd/replication/raft/pb"
)

var _ replication.Tx = (*tx)(nil)

type tx struct {
	id     string
	db     replication.DB
	r      *Raft
	readTs uint64
	w      pending.Writes
}

func (t *tx) New(_ context.Context, tx *badger.Txn) error {
	t.id = uuid.New().String()
	t.readTs = tx.ReadTs()
	t.w = pending.New(t.db.Path(), tx, t.db.MaxBatchCount(), t.db.MaxBatchSize(), int(t.db.ValueThreshold()))
	return nil
}

func (t *tx) Iterator(opt badger.IteratorOptions) pending.Iterator {
	return t.w.Iterator(opt.Prefix, opt.Reverse)
}

func (t *tx) Get(ctx context.Context, key []byte) (pending.Item, error) {
	return t.w.Get(key)
}

func (t *tx) Set(_ context.Context, key, value []byte, expiresAt uint64) error {
	t.w.Set(&badger.Entry{Key: key, Value: value, ExpiresAt: expiresAt})
	return nil
}

func (t *tx) Delete(_ context.Context, key []byte) error {
	t.w.Delete(key)
	return nil
}

const maxSize = replication.MaxMsgSize - 1024

func (t *tx) Commit(_ context.Context, at uint64) error {
	defer t.w.Close()
	// use an empty context to avoid the being cancelled
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 1)

	batch := &pb.Batch{TxID: t.id}
	batch.Ops = append(batch.Ops, &pb.Op{Action: &pb.Op_New{New: &pb.New{At: at}}})
	entry := &pb.Entry{Cmd: &pb.Entry_Batch{Batch: batch}}
	// the raft messages channel has a hard limit of 4096, so we need to restrict the number of concurrent goroutines
	sz := entry.SizeVT()
	if err := t.w.Replay(func(e *badger.Entry) error {
		var op *pb.Op
		if e.UserMeta != 0 {
			op = &pb.Op{Action: &pb.Op_Delete{Delete: &pb.Delete{Key: e.Key}}}
		} else {
			op = &pb.Op{Action: &pb.Op_Set{Set: &pb.Set{Key: e.Key, Value: e.Value, ExpiresAt: e.ExpiresAt}}}
		}
		l := op.SizeVT()
		if sz+1+l+protohelpers.SizeOfVarint(uint64(l)) < maxSize {
			batch.Ops = append(batch.Ops, op)
			sz += 1 + l + protohelpers.SizeOfVarint(uint64(l))
			return nil
		}
		buf, err := entry.MarshalVT()
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- t.r.node.Replicate(ctx, buf)
		}()
		batch.Ops = []*pb.Op{op}
		sz = entry.SizeVT()
		return nil
	}); err != nil {
		return err
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	var err error
	for e := range errs {
		if e != nil {
			err = e
		}
	}
	if err != nil {
		return err
	}
	batch.Ops = append(batch.Ops, &pb.Op{Action: &pb.Op_Commit{Commit: &pb.Commit{}}})
	buf, err := entry.MarshalVT()
	if err != nil {
		return err
	}
	return t.r.node.Replicate(ctx, buf)
}

func (t *tx) Close(ctx context.Context) error {
	if err := t.w.Close(); err != nil {
		logger.C(ctx).Warnf("raft: failed to close transaction: %v", err)
	}
	e := &pb.Entry{Cmd: &pb.Entry_Batch{Batch: &pb.Batch{TxID: t.id, Ops: []*pb.Op{{Action: &pb.Op_Discard{Discard: &pb.Discard{}}}}}}}
	b, err := e.MarshalVT()
	if err != nil {
		return err
	}
	if err := t.r.node.Replicate(context.Background(), b); err != nil {
		logger.C(ctx).Warnf("raft: failed to discard transaction: %v", err)
	}
	return nil
}
