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
	"io"
	"net"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	gerrs "go.linka.cloud/grpc-toolkit/errors"
	"go.linka.cloud/grpc-toolkit/logger"
	"google.golang.org/grpc/peer"

	"go.linka.cloud/protodb/internal/db/pending"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/replication"
	pb2 "go.linka.cloud/protodb/internal/replication/gossip/pb"
	"go.linka.cloud/protodb/pb"
)

func (r *Gossip) Init(req *pb2.InitRequest, ss pb2.ReplicationService_InitServer) error {
	if !r.IsLeader() {
		return gerrs.FailedPreconditionf("cannot initialize from non-leader")
	}
	p, ok := peer.FromContext(ss.Context())
	if !ok {
		return gerrs.Internalf("cannot get peer from context")
	}
	log := logger.C(ss.Context())
	log.Infof("Initializing replication request from %s", p.Addr)
	addr, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return gerrs.Internalf("failed to split host port: %v", err)
	}
	var found bool
	for i := 0; i < 100; i++ {
		for _, v := range r.clients() {
			if found = addr == v.addr.String(); found {
				break
			}
		}
		if found {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		return gerrs.Abortedf("node %s is not in the cluster", addr)
	}
	m := r.db.MaxVersion()
	if req.Since > m {
		return gerrs.FailedPreconditionf("invalid replication version %d, max is %d", req.Since, m)
	}
	if req.Since == m {
		return nil
	}
	return r.db.Stream(ss.Context(), m, req.Since, &writer{ss: ss})
}

func (r *Gossip) Replicate(ss pb2.ReplicationService_ReplicateServer) error {
	if r.IsLeader() {
		return gerrs.FailedPreconditionf("cannot replicate to leader")
	}
	log := logger.C(ss.Context()).WithField("node", r.name)
	p, ok := peer.FromContext(ss.Context())
	if !ok {
		return gerrs.Internalf("cannot get peer from context")
	}
	log.Infof("Replicating from %s", p.Addr)
	var (
		batch  *badger.WriteBatch
		reload bool
	)
	w := pending.New(r.db.Path(), r.db.MaxBatchCount(), r.db.MaxBatchSize(), int(r.db.ValueThreshold()), func(key []byte) {})
	defer func() {
		w.Close()
		if batch != nil {
			batch.Cancel()
		}
		if !reload {
			return
		}
		go func() {
			if err := r.db.LoadDescriptors(context.Background()); err != nil {
				log.Errorf("failed to reload descriptors: %v", err)
			}
		}()
	}()
	// message can come in any order in async replication mode, so we need to keep track of the commit message
	var (
		cmsg *pb2.Op_Commit
		cid  uint64
	)
	commit := func() error {
		if r.txnMark.DoneUntil() < cmsg.Commit.At-1 {
			log.Infof("waiting for transaction %d to be done", cmsg.Commit.At-1)
		}
		if err := r.txnMark.WaitForMark(ss.Context(), cmsg.Commit.At-1); err != nil {
			return err
		}
		r.txnMark.Begin(cmsg.Commit.At)
		defer r.txnMark.Done(cmsg.Commit.At)
		log.Infof("commit transaction at %d", cmsg.Commit.At)
		batch = r.db.NewWriteBatchAt(cmsg.Commit.At)
		err := w.Replay(func(e *badger.Entry) error {
			if e.UserMeta != 0 {
				return batch.DeleteAt(e.Key, cmsg.Commit.At)
			}
			return batch.SetEntryAt(e, cmsg.Commit.At)
		})
		if err != nil {
			return gerrs.Internalf("failed to write transaction: %v", err)
		}
		if err := batch.Flush(); err != nil {
			return gerrs.Internalf("failed to flush transaction: %v", err)
		}
		r.db.SetVersion(cmsg.Commit.At)
		if err := ss.Send(&pb2.Ack{}); err != nil {
			return gerrs.Internalf("failed to send response: %v", err)
		}
		m := r.meta.Load().CloneVT()
		m.LocalVersion = cmsg.Commit.At
		r.meta.Store(m)
		go func() {
			b, err := m.CloneVT().MarshalVT()
			if err != nil {
				log.Errorf("failed to marshal meta: %v", err)
				return
			}
			if err := r.updateMeta(r.ctx, b); err != nil {
				log.Errorf("failed to update meta: %v", err)
			}
		}()
		return nil
	}
	count := uint64(0)
	for {
		op, err := ss.Recv()
		if err != nil {
			return err
		}
		switch a := op.Action.(type) {
		case *pb2.Op_New:
			log.Debugf("new transaction at %d", a.New.At)
			if err := ss.Send(&pb2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
			if cmsg != nil && count == cid {
				return commit()
			}
		case *pb2.Op_Set:
			log.WithField("key", string(a.Set.Key)).Tracef("set key")
			if strings.HasPrefix(string(a.Set.Key), protodb.Descriptors) {
				reload = true
			}
			w.Set(&badger.Entry{
				Key:       a.Set.Key,
				Value:     a.Set.Value,
				ExpiresAt: a.Set.ExpiresAt,
			})
			if err := ss.Send(&pb2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
			if cmsg != nil && count == cid {
				return commit()
			}
		case *pb2.Op_Delete:
			log.WithField("key", string(a.Delete.Key)).Tracef("delete key")
			w.Delete(a.Delete.Key)
			if err := ss.Send(&pb2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
			if cmsg != nil && count == cid {
				return commit()
			}
		case *pb2.Op_Commit:
			cmsg = a
			cid = op.ID
			if op.ID == count {
				return commit()
			}
		}
		count++
	}
}

func (r *Gossip) Alive(ss pb2.ReplicationService_AliveServer) error {
	for {
		if _, err := ss.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := ss.Send(&pb2.Ack{}); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (r *Gossip) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Get(ctx, req)
	}
	return p.Get(ctx, req)
}

func (r *Gossip) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Set(ctx, req)
	}
	return p.Set(ctx, req)
}

func (r *Gossip) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Delete(ctx, req)
	}
	return p.Delete(ctx, req)
}

func (r *Gossip) Tx(ss pb.ProtoDB_TxServer) error {
	p, ok, err := r.maybeLeaderProxy(ss.Context(), false)
	if err != nil {
		return err
	}
	if !ok {
		return r.h.Tx(ss)
	}
	return p.Tx(ss)
}

func (r *Gossip) Watch(req *pb.WatchRequest, ss pb.ProtoDB_WatchServer) error {
	p, ok, err := r.maybeLeaderProxy(ss.Context(), true)
	if err != nil {
		return err
	}
	if !ok {
		return r.h.Watch(req, ss)
	}
	return p.Watch(req, ss)
}

func (r *Gossip) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Register(ctx, req)
	}
	return p.Register(ctx, req)
}

func (r *Gossip) Descriptors(ctx context.Context, req *pb.DescriptorsRequest) (*pb.DescriptorsResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.Descriptors(ctx, req)
	}
	return p.Descriptors(ctx, req)
}

func (r *Gossip) FileDescriptors(ctx context.Context, req *pb.FileDescriptorsRequest) (*pb.FileDescriptorsResponse, error) {
	p, ok, err := r.maybeLeaderProxy(ctx, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return r.h.FileDescriptors(ctx, req)
	}
	return p.FileDescriptors(ctx, req)
}

func (r *Gossip) maybeLeaderProxy(ctx context.Context, read bool) (pb.ProtoDBServer, bool, error) {
	if r.leading.Load() || (read && r.mode == replication.ModeSync) {
		return nil, false, nil
	}
	n, ok := r.nodes.Load(r.leaderName.Load())
	if !ok {
		return nil, false, gerrs.Internalf("no leader connection")
	}
	logger.C(ctx).Infof("proxying to leader %s", r.leaderName)

	return n.proxy, true, nil
}
