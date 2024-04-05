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

	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	"go.linka.cloud/grpc-toolkit/logger"
	"golang.org/x/sync/errgroup"

	"go.linka.cloud/protodb/internal/db/pending"
	"go.linka.cloud/protodb/internal/replication"
	"go.linka.cloud/protodb/internal/replication/raft/pb"
)

var _ replication.Tx = (*tx)(nil)

type tx struct {
	id     string
	r      *Raft
	readTs uint64
	w      pending.IterableMergedWrites
}

func (t *tx) Iterator(tx *badger.Txn, readTs uint64, opt badger.IteratorOptions) pending.Iterator {
	return t.w.MergedIterator(tx, readTs, opt)
}

func (t *tx) New(_ context.Context, readTs uint64) error {
	t.id = uuid.New().String()
	t.readTs = readTs
	t.w = pending.New(t.r.db.Path(), t.r.db.MaxBatchCount(), t.r.db.MaxBatchSize(), int(t.r.db.ValueThreshold()), func(key []byte) {})
	return nil
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

func (t *tx) Commit(ctx context.Context, at uint64) error {
	defer t.w.Close()
	tx := &pb.Tx{
		ReadAt:   t.readTs,
		CommitAt: at,
	}
	// return t.replicate(ctx, &pb.Op{TxID: t.id, Action: &pb.Op_Commit{Commit: &pb.Commit{At: at}}})
	if err := t.w.Replay(func(e *badger.Entry) error {
		if e.UserMeta != 0 {
			tx.Deletes = append(tx.Deletes, &pb.Delete{Key: e.Key})
		} else {
			tx.Sets = append(tx.Sets, &pb.Set{Key: e.Key, Value: e.Value, ExpiresAt: e.ExpiresAt})
		}
		return nil
	}); err != nil {
		return err
	}
	b, err := tx.MarshalVT()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)

	// the raft messages channel has a hard limit of 4096, so we need to restrict the number of concurrent goroutines
	g.SetLimit(1024)
	for i := 0; i < len(b); i += maxSize {
		i := i
		g.Go(func() error {
			end := i + maxSize
			if end > len(b) {
				end = len(b)
			}
			c := &pb.Chunk{
				TxID:   t.id,
				Total:  uint64(len(b)),
				Offset: uint64(i),
				Data:   b[i:end],
			}
			e := &pb.Entry{Cmd: &pb.Entry_Chunk{Chunk: c}}
			b, err := e.MarshalVT()
			if err != nil {
				return err
			}
			return t.r.node.Replicate(ctx, b)
		})
	}
	if err = g.Wait(); err == nil {
		return nil
	}

	return err
}

func (t *tx) Close(ctx context.Context) error {
	if err := t.w.Close(); err != nil {
		logger.C(ctx).Warnf("raft: failed to close transaction: %v", err)
	}
	c := &pb.Chunk{
		TxID:    t.id,
		Discard: true,
	}
	e := &pb.Entry{Cmd: &pb.Entry_Chunk{Chunk: c}}
	b, err := e.MarshalVT()
	if err != nil {
		return err
	}
	if err := t.r.node.Replicate(ctx, b); err != nil {
		logger.C(ctx).Warnf("raft: failed to discard transaction: %v", err)
	}
	return nil
}
