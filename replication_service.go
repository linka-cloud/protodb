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

	"github.com/dgraph-io/badger/v3"
	gerrs "go.linka.cloud/grpc-toolkit/errors"
	"go.linka.cloud/grpc-toolkit/logger"
	"google.golang.org/grpc/peer"

	repl2 "go.linka.cloud/protodb/internal/replication"
	"go.linka.cloud/protodb/pb"
)

type replicationClient struct {
	repl2.ReplicationServiceClient
	name string
}

func (r *repl) Init(req *repl2.InitRequest, ss repl2.ReplicationService_InitServer) error {
	if !r.isLeader() {
		return gerrs.FailedPreconditionf("cannot initialize from non-le")
	}
	p, ok := peer.FromContext(ss.Context())
	if !ok {
		return gerrs.Internalf("cannot get peer from context")
	}
	log := logger.C(ss.Context())
	log.Infof("Initializing replication request from %s", p.Addr)
	// TODO(adphi): check that peer is not the leader
	m := r.db.bdb.MaxVersion()
	if req.Since > m {
		return gerrs.FailedPreconditionf("invalid replication version %d, max is %d", req.Since, m)
	}
	if req.Since == m {
		return nil
	}
	s := r.db.bdb.NewStreamAt(m)
	s.LogPrefix = "Init replication"
	s.SinceTs = req.Since
	_, err := s.Backup(&replWriter{ss: ss}, req.Since)
	return err
}

func (r *repl) Replicate(ss repl2.ReplicationService_ReplicateServer) error {
	if r.isLeader() {
		return gerrs.FailedPreconditionf("cannot replicate to leader")
	}
	log := logger.C(ss.Context())
	p, ok := peer.FromContext(ss.Context())
	if !ok {
		return gerrs.Internalf("cannot get peer from context")
	}
	log.Infof("Replicating from %s", p.Addr)
	var tx *badger.Txn
	for {
		op, err := ss.Recv()
		if err != nil {
			return err
		}
		switch a := op.Action.(type) {
		case *repl2.Op_New:
			log.Debugf("new transaction at %d", a.New.At)
			if tx != nil {
				return gerrs.InvalidArgumentf("transaction already started")
			}
			tx = r.db.bdb.NewTransactionAt(a.New.At, true)
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
		case *repl2.Op_Set:
			log.WithField("key", string(a.Set.Key)).Debugf("set key")
			if tx == nil {
				return gerrs.InvalidArgumentf("transaction not started")
			}
			if err := tx.SetEntry(&badger.Entry{
				Key:       a.Set.Key,
				Value:     a.Set.Value,
				ExpiresAt: a.Set.ExpiresAt,
			}); err != nil {
				return gerrs.Internalf("failed to set key %s: %v", a.Set.Key, err)
			}
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
		case *repl2.Op_Delete:
			log.WithField("key", string(a.Delete.Key)).Debugf("delete key")
			if tx == nil {
				return gerrs.InvalidArgumentf("transaction not started")
			}
			if err := tx.Delete(a.Delete.Key); err != nil {
				return gerrs.Internalf("failed to delete key %s: %v", a.Delete.Key, err)
			}
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
		case *repl2.Op_Commit:
			log.Debugf("commit transaction at %d", a.Commit.At)
			if tx == nil {
				return gerrs.InvalidArgumentf("transaction not started")
			}
			errs := make(chan error, 1)
			if err := tx.CommitAt(a.Commit.At, func(err error) {
				errs <- err
			}); err != nil {
				return gerrs.Internalf("failed to commit transaction: %v", err)
			}
			if err := <-errs; err != nil {
				return gerrs.Internalf("failed to commit transaction: %v", err)
			}
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send response: %v", err)
			}
			r.db.orc.Lock()
			r.db.orc.txnMark.Done(a.Commit.At)
			r.db.orc.readMark.Done(a.Commit.At)
			r.db.orc.nextTxnTs = a.Commit.At + 1
			r.db.orc.Unlock()
			return nil
		}
	}
}

func (r *repl) Alive(ss repl2.ReplicationService_AliveServer) error {
	for {
		if _, err := ss.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := ss.Send(&repl2.Ack{}); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (r *repl) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Get(ctx, req)
	}
	return p.Get(ctx, req)
}

func (r *repl) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Set(ctx, req)
	}
	return p.Set(ctx, req)
}

func (r *repl) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Delete(ctx, req)
	}
	return p.Delete(ctx, req)
}

func (r *repl) Tx(ss pb.ProtoDB_TxServer) error {
	p, ok, err := r.maybeLeaderProxy(ss.Context(), false)
	if err != nil {
		return err
	}
	if !ok {
		return r.h.Tx(ss)
	}
	return p.Tx(ss)
}

func (r *repl) Watch(req *pb.WatchRequest, ss pb.ProtoDB_WatchServer) error {
	p, ok, err := r.maybeLeaderProxy(ss.Context(), true)
	if err != nil {
		return err
	}
	if !ok {
		return r.h.Watch(req, ss)
	}
	return p.Watch(req, ss)
}

func (r *repl) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Register(ctx, req)
	}
	return p.Register(ctx, req)
}

func (r *repl) Descriptors(ctx context.Context, req *pb.DescriptorsRequest) (*pb.DescriptorsResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Descriptors(ctx, req)
	}
	return p.Descriptors(ctx, req)
}

func (r *repl) FileDescriptors(ctx context.Context, req *pb.FileDescriptorsRequest) (*pb.FileDescriptorsResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.FileDescriptors(ctx, req)
	}
	return p.FileDescriptors(ctx, req)
}

func (r *repl) maybeLeaderProxy(ctx context.Context, read bool) (pb.ProtoDBServer, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.l || (read && r.mode == ReplicationModeSync) {
		return nil, false, nil
	}
	c, ok := r.ccs[r.cle]
	if !ok {
		return nil, false, gerrs.Internalf("no leader connection")
	}
	logger.C(ctx).Infof("proxying to leader %s", r.cle)
	return pb.NewProtoDBProxy(pb.NewProtoDBClient(c)), true, nil
}
