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

package gossip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v3"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.uber.org/multierr"

	"go.linka.cloud/protodb/internal/badgerd/pending"
	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

type stream struct {
	c pb.ReplicationService_ReplicateClient
	n string
}

func (r *Gossip) NewTx(ctx context.Context) (replication.Tx, error) {
	log := logger.C(ctx)
	var cs []*stream
	if r.mode == replication.ModeAsync {
		ctx = context.Background()
	}
	for _, v := range r.clients() {
		log.Infof("Starting replicated transaction with %v", v.name)
		c, err := v.repl.Replicate(ctx)
		if err != nil {
			return nil, err
		}
		cs = append(cs, &stream{n: v.name, c: c})
		log.Infof("Started replicated transaction with %v", v.name)
	}
	return &tx{db: r.db, mode: r.mode, cs: cs}, nil
}

type tx struct {
	db     replication.DB
	mode   replication.Mode
	cs     []*stream
	cmu    sync.RWMutex
	count  atomic.Uint64
	readTs uint64
	w      pending.Writes
	o      sync.Once
}

func (r *tx) streams() []*stream {
	r.cmu.RLock()
	defer r.cmu.RUnlock()
	return append([]*stream{}, r.cs...)
}

func (r *tx) removeSteam(s *stream) {
	r.cmu.Lock()
	defer r.cmu.Unlock()
	for i, v := range r.cs {
		if v != s {
			continue
		}
		r.cs = append(r.cs[:i], r.cs[i+1:]...)
		break
	}
}

func (r *tx) hasStreams(s *stream) bool {
	r.cmu.RLock()
	defer r.cmu.RUnlock()
	for _, v := range r.cs {
		if v == s {
			return true
		}
	}
	return false
}

func (r *tx) New(ctx context.Context, tx *badger.Txn, readTracker pending.ReadTracker) error {
	if err := r.do(ctx, &pb.Op{Action: &pb.Op_New{New: &pb.New{At: tx.ReadTs()}}}); err != nil {
		return err
	}
	r.readTs = tx.ReadTs()
	r.w = pending.New(r.db.Path(), tx, r.db.MaxBatchCount(), r.db.MaxBatchSize(), int(r.db.ValueThreshold()), readTracker)
	return nil
}

func (r *tx) Iterator(opt badger.IteratorOptions) pending.Iterator {
	return r.w.Iterator(opt.Prefix, opt.Reverse)
}

func (r *tx) Get(ctx context.Context, key []byte) (pending.Item, error) {
	return r.w.Get(key)
}

func (r *tx) Set(ctx context.Context, key, value []byte, expiresAt uint64) error {
	r.w.Set(&badger.Entry{
		Key:      key,
		Value:    value,
		UserMeta: 0,
	})
	return r.do(ctx, &pb.Op{Action: &pb.Op_Set{Set: &pb.Set{Key: key, Value: value, ExpiresAt: expiresAt}}})
}

func (r *tx) Delete(ctx context.Context, key []byte) error {
	r.w.Delete(key)
	return r.do(ctx, &pb.Op{Action: &pb.Op_Delete{Delete: &pb.Delete{Key: key}}})
}

func (r *tx) Commit(ctx context.Context, at uint64) error {
	b := r.db.NewWriteBatchAt(r.readTs)
	defer b.Cancel()
	if err := r.w.Replay(func(e *badger.Entry) error {
		if e.UserMeta != 0 {
			return b.DeleteAt(e.Key, at)
		}
		return b.SetEntryAt(e, at)
	}); err != nil {
		return err
	}
	if err := b.Flush(); err != nil {
		return err
	}
	if err := r.do(ctx, &pb.Op{Action: &pb.Op_Commit{Commit: &pb.Commit{At: at}}}); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (r *tx) Close(ctx context.Context) error {
	if err := r.cancel(ctx); err != nil {
		return err
	}
	return nil
}

func (r *tx) cancel(_ context.Context) error {
	var merr error
	r.o.Do(func() {
		merr := r.w.Close()
		for _, v := range r.cs {
			merr = multierr.Append(merr, v.c.CloseSend())
		}
	})
	return merr
}

func (r *tx) do(ctx context.Context, msg *pb.Op) error {
	msg.ID = r.count.Load()
	defer r.count.Add(1)
	log := logger.C(ctx)
	switch r.mode {
	case replication.ModeAsync:
		go func() {
			if err := r.send(ctx, msg, cb(log, nil)); err != nil {
				log.WithError(err).Errorf("failed to send replication message")
			}
		}()
		return nil
	case replication.ModeSync:
		errs := make(chan error, 1)
		if err := r.send(ctx, msg, cb(log, errs)); err != nil {
			return err
		}
		return <-errs
	}
	return nil
}

type result struct {
	s   *stream
	err error
}

func (r *tx) send(_ context.Context, msg *pb.Op, cb func(err error)) error {
	cs := r.streams()
	sres := make(chan *result, len(cs))
	ares := make(chan *result, len(cs))
	for _, v := range cs {
		go func(v *stream) {
			err := v.c.Send(msg.CloneVT())
			sres <- &result{err: err, s: v}
			_, err = v.c.Recv()
			ares <- &result{err: err, s: v}
		}(v)
	}
	var err error
	for range cs {
		res := <-sres
		err = r.handleErr(res, err)
	}
	if err != nil {
		return err
	}
	go func() {
		var err error
		for range cs {
			res := <-ares
			err = r.handleErr(res, err)
		}
		if cb != nil {
			cb(err)
		}
	}()
	return nil
}

func cb(log logger.Logger, errs chan<- error) func(err error) {
	return func(err error) {
		if errs != nil {
			errs <- err
		}
		if err != nil {
			log.WithError(err).Errorf("failed to send replication message")
		}
	}
}

func (r *tx) handleErr(res *result, err error) error {
	if res.err != nil && r.hasStreams(res.s) {
		r.removeSteam(res.s)
		return multierr.Append(err, fmt.Errorf("%s: %w", res.s.n, res.err))
	}
	return err
}
