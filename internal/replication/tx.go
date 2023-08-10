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
	"io"

	"go.linka.cloud/grpc-toolkit/logger"
	"go.uber.org/multierr"

	"go.linka.cloud/protodb/internal/replication/pb"
)

func (r *Repl) NewTx(ctx context.Context) (*Tx, error) {
	log := logger.C(ctx)
	var cs []pb.ReplicationService_ReplicateClient
	if r.mode < ModeSync {
		ctx = context.Background()
	}
	for _, v := range r.clients() {
		log.Infof("Starting replicated transaction with %v", v.name)
		c, err := v.repl.Replicate(ctx)
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
		log.Infof("Started replicated transaction with %v", v.name)
	}
	return &Tx{mode: r.mode, cs: cs, sync: make(chan struct{}, 1)}, nil
}

type Tx struct {
	mode Mode
	cs   []pb.ReplicationService_ReplicateClient
	sync chan struct{}
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
	for _, c := range r.cs {
		if err := c.CloseSend(); err != nil {
			merr = multierr.Append(merr, err)
		}
	}
	return merr
}

func (r *Tx) do(ctx context.Context, msg *pb.Op) error {
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

func (r *Tx) send(_ context.Context, msg *pb.Op, cb func(err error)) error {
	serrs := make(chan error, len(r.cs))
	aerrs := make(chan error, len(r.cs))
	for _, v := range r.cs {
		go func(v pb.ReplicationService_ReplicateClient) {
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
