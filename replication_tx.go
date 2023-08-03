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

package protodb

import (
	"context"
	"errors"
	"io"

	"go.linka.cloud/grpc/logger"
	"go.uber.org/multierr"

	repl2 "go.linka.cloud/protodb/internal/replication"
)

func (r *repl) newTx(ctx context.Context) (*replTx, error) {
	var cs []repl2.ReplicationService_ReplicateClient
	for _, c := range r.clients() {
		c, err := c.Replicate(ctx)
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	return &replTx{mode: r.mode, cs: cs}, nil
}

type replTx struct {
	mode ReplicationMode
	cs   []repl2.ReplicationService_ReplicateClient
}

func (r *replTx) new(ctx context.Context, at uint64) error {
	return r.do(ctx, &repl2.Op{Action: &repl2.Op_New{New: &repl2.New{At: at}}})
}

func (r *replTx) set(ctx context.Context, key, value []byte, expiresAt uint64) error {
	return r.do(ctx, &repl2.Op{Action: &repl2.Op_Set{Set: &repl2.Set{Key: key, Value: value, ExpiresAt: expiresAt}}})
}

func (r *replTx) delete(ctx context.Context, key []byte) error {
	return r.do(ctx, &repl2.Op{Action: &repl2.Op_Delete{Delete: &repl2.Delete{Key: key}}})
}

func (r *replTx) commit(ctx context.Context, at uint64) error {
	if err := r.do(ctx, &repl2.Op{Action: &repl2.Op_Commit{Commit: &repl2.Commit{At: at}}}); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func (r *replTx) cancel(_ context.Context) error {
	var merr error
	for _, c := range r.cs {
		if err := c.CloseSend(); err != nil {
			merr = multierr.Append(merr, err)
		}
	}
	return merr
}

func (r *replTx) do(ctx context.Context, msg *repl2.Op) error {
	log := logger.C(ctx)
	switch r.mode {
	case ReplicationModeNone:
		return nil
	case ReplicationModeAsync:
		go func() {
			if err := r.send(ctx, msg, cb(log, nil)); err != nil {
				log.WithError(err).Errorf("failed to send replication message")
			}
		}()
		return nil
	case ReplicationModeSent:
		if err := r.send(ctx, msg, cb(log, nil)); err != nil {
			return err
		}
	case ReplicationModeSync:
		errs := make(chan error, 1)
		if err := r.send(ctx, msg, cb(log, errs)); err != nil {
			return err
		}
		return <-errs
	}
	return nil
}

func (r *replTx) send(_ context.Context, msg *repl2.Op, cb func(err error)) error {
	serrs := make(chan error, len(r.cs))
	aerrs := make(chan error, len(r.cs))
	for _, v := range r.cs {
		go func(v repl2.ReplicationService_ReplicateClient) {
			serrs <- v.Send(msg.CloneVT())
			_, err := v.Recv()
			aerrs <- err
		}(v)
	}
	var err error
	for range r.cs {
		err = multierr.Append(err, <-serrs)
	}
	if err != nil {
		return err
	}
	go func() {
		var err error
		for range r.cs {
			err = multierr.Append(err, <-aerrs)
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
