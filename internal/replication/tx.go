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

package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"go.linka.cloud/grpc-toolkit/logger"
	"go.uber.org/multierr"

	"go.linka.cloud/protodb/internal/replication/pb"
)

type stream struct {
	c pb.ReplicationService_ReplicateClient
	n string
}

func (r *Repl) NewTx(ctx context.Context) (*Tx, error) {
	log := logger.C(ctx)
	var cs []*stream
	if r.mode > ModeSync {
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
	return &Tx{mode: r.mode, cs: cs}, nil
}

type Tx struct {
	mode  Mode
	cs    []*stream
	cmu   sync.RWMutex
	count atomic.Uint64
}

func (r *Tx) streams() []*stream {
	r.cmu.RLock()
	defer r.cmu.RUnlock()
	return append([]*stream{}, r.cs...)
}

func (r *Tx) removeSteam(s *stream) {
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

func (r *Tx) hasStreams(s *stream) bool {
	r.cmu.RLock()
	defer r.cmu.RUnlock()
	for _, v := range r.cs {
		if v == s {
			return true
		}
	}
	return false
}

func (r *Tx) New(ctx context.Context, at uint64) error {
	return r.do(ctx, &pb.Op{Action: &pb.Op_New{New: &pb.New{At: at}}})
}

func (r *Tx) Set(ctx context.Context, key, value []byte, expiresAt uint64) error {
	return r.do(ctx, &pb.Op{Action: &pb.Op_Set{Set: &pb.Set{Key: key, Value: value, ExpiresAt: expiresAt}}})
}

func (r *Tx) Delete(ctx context.Context, key []byte) error {
	return r.do(ctx, &pb.Op{Action: &pb.Op_Delete{Delete: &pb.Delete{Key: key}}})
}

func (r *Tx) Commit(ctx context.Context, at uint64) error {
	if err := r.do(ctx, &pb.Op{Action: &pb.Op_Commit{Commit: &pb.Commit{At: at}}}); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (r *Tx) cancel(_ context.Context) error {
	var merr error
	for _, v := range r.cs {
		if err := v.c.CloseSend(); err != nil {
			merr = multierr.Append(merr, err)
		}
	}
	return merr
}

func (r *Tx) do(ctx context.Context, msg *pb.Op) error {
	if r == nil {
		return nil
	}
	msg.ID = r.count.Load()
	defer r.count.Add(1)
	log := logger.C(ctx)
	switch r.mode {
	case ModeAsync:
		go func() {
			if err := r.send(ctx, msg, cb(log, nil)); err != nil {
				log.WithError(err).Errorf("failed to send replication message")
			}
		}()
		return nil
	case ModeSync:
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

func (r *Tx) send(_ context.Context, msg *pb.Op, cb func(err error)) error {
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

func (r *Tx) handleErr(res *result, err error) error {
	if res.err != nil && r.hasStreams(res.s) {
		r.removeSteam(res.s)
		return multierr.Append(err, fmt.Errorf("%s: %w", res.s.n, res.err))
	}
	return err
}
