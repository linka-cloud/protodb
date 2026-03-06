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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v3"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protowire"

	"go.linka.cloud/protodb/internal/badgerd/pending"
	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/badgerd/replication/async"
	"go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

type stream struct {
	q *async.Queue[*pb.ReplicateRequest, *pb.Ack]
	s pb.ReplicationService_ReplicateClient
	n string
}

func (r *Gossip) NewTx(ctx context.Context) (replication.Tx, error) {
	log := logger.C(ctx)
	var cs []*stream
	for _, v := range r.clients() {
		log.Infof("Starting replicated transaction with %v", v.name)
		c, err := v.repl.Replicate(ctx)
		if err != nil {
			return nil, err
		}
		s := &stream{n: v.name}
		if r.mode == replication.ModeAsync {
			s.q = async.NewQueue(c)
		} else {
			s.s = c
		}
		cs = append(cs, s)
		log.Infof("Started replicated transaction with %v", v.name)
	}
	return &tx{db: r.db, mode: r.mode, cs: cs}, nil
}

type tx struct {
	db     replication.DB
	mode   replication.Mode
	cs     []*stream
	cmu    sync.RWMutex
	bmu    sync.Mutex
	swg    sync.WaitGroup
	count  atomic.Uint64
	readTs uint64
	w      pending.Writes
	buf    *pb.ReplicateRequest
	bufSz  int
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
	return slices.Contains(r.cs, s)
}

func (r *tx) New(ctx context.Context, tx *badger.Txn) error {
	if err := r.do(ctx, &pb.Op{Action: &pb.Op_New{New: &pb.New{At: tx.ReadTs()}}}); err != nil {
		return err
	}
	r.readTs = tx.ReadTs()
	r.w = pending.New(r.db.Path(), tx, r.db.MaxBatchCount(), r.db.MaxBatchSize(), int(r.db.ValueThreshold()))
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
	return r.doSized(ctx, &pb.Op{Action: &pb.Op_Set{Set: &pb.Set{Key: key, Value: value, ExpiresAt: expiresAt}}}, setOpWireSize(key, value))
}

func (r *tx) Delete(ctx context.Context, key []byte) error {
	r.w.Delete(key)
	return r.doSized(ctx, &pb.Op{Action: &pb.Op_Delete{Delete: &pb.Delete{Key: key}}}, deleteOpWireSize(key))
}

func (r *tx) Commit(ctx context.Context, at uint64) error {
	if r.mode == replication.ModeAsync {
		r.swg.Add(1)
		go func() {
			defer r.swg.Done()
			if err := r.do(ctx, &pb.Op{Action: &pb.Op_Commit{Commit: &pb.Commit{At: at}}}); err != nil {
				logger.C(ctx).WithError(err).Error("failed to replicate commit")
			}
		}()
	} else {
		if err := r.do(ctx, &pb.Op{Action: &pb.Op_Commit{Commit: &pb.Commit{At: at}}}); err != nil {
			return err
		}
	}
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
	return b.Flush()
}

func (r *tx) Close(ctx context.Context) error {
	if err := r.cancel(ctx); err != nil {
		return err
	}
	return nil
}

func (r *tx) cancel(ctx context.Context) error {
	var merr error
	r.o.Do(func() {
		merr := r.w.Close()
		r.swg.Wait()
		cs := r.streams()
		ch := make(chan error, len(cs))
		for _, v := range cs {
			go func(v *stream) {
				var err error
				if r.mode == replication.ModeAsync {
					err = v.q.Close()
				} else {
					err = v.s.CloseSend()
					ch <- err
				}
				if err != nil {
					logger.C(ctx).WithError(err).WithField("peer", v.n).Error("failed to close replication stream")
				}
			}(v)
		}
		if r.mode == replication.ModeAsync {
			return
		}
		for range cs {
			merr = multierr.Append(merr, <-ch)
		}
	})
	return merr
}

func (r *tx) do(ctx context.Context, msg *pb.Op) error {
	return r.doSized(ctx, msg, opWireSize(msg))
}

func (r *tx) doSized(ctx context.Context, msg *pb.Op, sz int) error {
	msg.ID = r.count.Load()
	defer r.count.Add(1)
	if _, ok := msg.Action.(*pb.Op_New); ok {
		if err := r.flush(ctx); err != nil {
			return err
		}
		return r.send(ctx, &pb.ReplicateRequest{Ops: []*pb.Op{msg}})
	}
	if _, ok := msg.Action.(*pb.Op_Commit); ok {
		if r.mode == replication.ModeSync {
			r.bmu.Lock()
			if r.buf == nil {
				r.buf = &pb.ReplicateRequest{}
			}
			r.buf.Ops = append(r.buf.Ops, msg)
			req := r.buf
			r.buf = nil
			r.bufSz = 0
			r.bmu.Unlock()
			return r.send(ctx, req)
		}
		if err := r.flush(ctx); err != nil {
			return err
		}
		return r.send(ctx, &pb.ReplicateRequest{Ops: []*pb.Op{msg}})
	}
	r.bmu.Lock()
	if r.buf == nil {
		r.buf = &pb.ReplicateRequest{}
	}
	r.buf.Ops = append(r.buf.Ops, msg)
	r.bufSz += sz
	flush := false
	if r.bufSz >= replication.MaxMsgSize {
		flush = true
	}
	r.bmu.Unlock()
	if flush {
		return r.flush(ctx)
	}
	return nil
}

func (r *tx) flush(ctx context.Context) error {
	r.bmu.Lock()
	if r.buf == nil || len(r.buf.Ops) == 0 {
		r.bmu.Unlock()
		return nil
	}
	req := r.buf
	r.buf = nil
	r.bufSz = 0
	r.bmu.Unlock()
	return r.send(ctx, req)
}

func opWireSize(op *pb.Op) int {
	s := op.SizeVT()
	return 1 + protowire.SizeVarint(uint64(s)) + s
}

func setOpWireSize(key, value []byte) int {
	// 1 byte op field tag + varint(message length) + upper-bound payload size.
	// We intentionally overestimate a bit to avoid exceeding MaxMsgSize.
	s := 64 + len(key) + len(value)
	return 1 + protowire.SizeVarint(uint64(s)) + s
}

func deleteOpWireSize(key []byte) int {
	s := 32 + len(key)
	return 1 + protowire.SizeVarint(uint64(s)) + s
}

func (r *tx) send(ctx context.Context, req *pb.ReplicateRequest) error {
	if r.mode == replication.ModeAsync {
		for _, v := range r.streams() {
			a := v.q.Send(req)
			r.swg.Add(1)
			go func(v *stream, a async.Async[*pb.Ack]) {
				defer r.swg.Done()
				if err := r.handleErr(ctx, async.NewResult(v, a.WaitSent())); err != nil {
					return
				}
				if _, err := a.Wait(); err != nil {
					r.handleErr(ctx, async.NewResult(v, err))
				}
			}(v, a)
		}
		return nil
	}
	cs := r.streams()
	ch := make(chan error, len(cs))
	for _, v := range cs {
		go func(v *stream) {
			switch r.mode {
			case replication.ModeSync:
				err := v.s.Send(req)
				if err == nil {
					_, err = v.s.Recv()
				}
				ch <- r.handleErr(ctx, async.NewResult(v, err))
			}
		}(v)
	}
	for range cs {
		if err := <-ch; err != nil {
			return err
		}
	}
	return nil
}

func (r *tx) handleErr(ctx context.Context, res *async.Result[*stream]) error {
	if res.Err() != nil && r.hasStreams(res.Value()) && !errors.Is(res.Err(), io.EOF) {
		logger.C(ctx).WithField("peer", res.Value().n).WithError(res.Err()).Error("failed to send replication message: removing peer")
		r.removeSteam(res.Value())
		return fmt.Errorf("%s: %w", res.Value().n, res.Err())
	}
	return nil
}
