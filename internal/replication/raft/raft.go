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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/klauspost/compress/zstd"
	"github.com/shaj13/raft"
	"github.com/shaj13/raft/transport"
	"github.com/shaj13/raft/transport/raftgrpc"
	"go.linka.cloud/grpc-toolkit/logger"
	pubsub "go.linka.cloud/pubsub/typed"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"go.linka.cloud/protodb/internal/replication"
	"go.linka.cloud/protodb/internal/replication/raft/pb"
	"go.linka.cloud/protodb/internal/server"
	pb2 "go.linka.cloud/protodb/pb"
)

var (
	_ raft.StateMachine       = (*Raft)(nil)
	_ replication.Replication = (*Raft)(nil)
)

var defaultOptions = replication.Options{
	Addrs:    []string{"localhost"},
	GRPCPort: 7081,
	// Tick:     10 * time.Millisecond,
}

type buffer struct {
	data   []byte
	writen int
}

type Raft struct {
	db replication.DB

	node *raft.Node
	srv  *grpc.Server
	lis  net.Listener

	sch   chan raft.StateType
	pub   pubsub.Publisher[string]
	dopts []grpc.DialOption
	conns map[uint64]grpc.ClientConnInterface
	cmu   sync.RWMutex

	ready atomic.Bool
	snap  atomic.Bool
	// txns  map[uint64]*pendingTx

	cancel context.CancelFunc
	g      *errgroup.Group

	buffs map[string]*buffer
}

func New(ctx context.Context, db replication.DB, ropts ...replication.Option) (replication.Replication, error) {
	o := defaultOptions
	for _, opt := range ropts {
		opt(&o)
	}
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if o.Name == "" {
		o.Name = h
	}

	ch := make(chan raft.StateType, 1)

	opts := []raft.Option{
		raft.WithStateDIR(filepath.Join(db.Path(), "raft")),
		raft.WithPreVote(),
		raft.WithCheckQuorum(),
		raft.WithMaxSizePerMsg(4 * 1024 * 1024),
		// raft.WithHeartbeatTick(10),
		// raft.WithElectionTick(100),
		raft.WithPipelining(),
		raft.WithDisableProposalForwarding(),
		raft.WithStateChangeCh(ch),
	}
	if o.Tick != 0 {
		opts = append(opts, raft.WithTickInterval(o.Tick))
	}
	tlsConfig, err := o.TLS()
	if err != nil {
		return nil, err
	}
	if len(o.StartOptions) == 0 {
		o.StartOptions = append(o.StartOptions, raft.WithFallback(raft.WithInitCluster(), raft.WithRestart()))
	}
	// if o.SnapshotInterval != 0 {
	// 	opts = append(opts, raft.WithSnapshotInterval(o.SnapshotInterval))
	// }
	var (
		sopts []grpc.ServerOption
		dopts []grpc.DialOption
	)
	if tlsConfig != nil {
		sopts = append(sopts, grpc.Creds(credentials.NewTLS(tlsConfig)))
		dopts = append(dopts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		sopts = append(sopts, grpc.Creds(insecure.NewCredentials()))
		dopts = append(dopts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	raftgrpc.Register(raftgrpc.WithDialOptions(dopts...))

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	n := &Raft{
		db:     db,
		sch:    ch,
		dopts:  dopts,
		conns:  make(map[uint64]grpc.ClientConnInterface),
		cancel: cancel,
		// txns:   make(map[uint64]*pendingTx),
		g:     g,
		pub:   pubsub.NewPublisher[string](time.Second, 2),
		buffs: make(map[string]*buffer),
	}
	n.node = raft.NewNode(n, transport.GRPC, opts...)

	n.lis, err = net.Listen("tcp", fmt.Sprintf(":%d", o.GRPCPort))
	if err != nil {
		return nil, err
	}
	n.srv = grpc.NewServer(sopts...)
	raftgrpc.RegisterHandler(n.srv, n.node.Handler())
	s, err := server.NewServer(db)
	if err != nil {
		return nil, err
	}
	pb2.RegisterProtoDBServer(n.srv, s)
	// TODO(adphi): make that configurable
	return n, n.run(ctx, append([]raft.StartOption{raft.WithAddress(fmt.Sprintf(":%d", o.GRPCPort))}, o.StartOptions...)...)
}

func (r *Raft) Apply(bytes []byte) error {
	var e pb.Entry
	if err := e.UnmarshalVT(bytes); err != nil {
		return err
	}
	switch c := e.Cmd.(type) {
	// case *pb.Entry_Tx:
	case *pb.Entry_Chunk:
		if c.Chunk.Discard {
			delete(r.buffs, c.Chunk.TxID)
			return nil
		}
		buff, ok := r.buffs[c.Chunk.TxID]
		if !ok {
			buff = &buffer{data: make([]byte, c.Chunk.Total)}
			r.buffs[c.Chunk.TxID] = buff
		}
		buff.writen += copy(buff.data[c.Chunk.Offset:], c.Chunk.Data)
		if uint64(buff.writen) < c.Chunk.Total {
			return nil
		}
		defer func() {
			delete(r.buffs, c.Chunk.TxID)
		}()
		var tx pb.Tx
		if err := tx.UnmarshalVT(buff.data); err != nil {
			return err
		}
		start := time.Now()
		logger.C(context.Background()).Infof("writing transaction at %d (set: %d, deletes: %d)", tx.CommitAt, len(tx.Sets), len(tx.Deletes))
		b := r.db.NewWriteBatchAt(tx.ReadAt)
		defer b.Cancel()
		for _, v := range tx.Sets {
			if err := b.SetEntryAt(&badger.Entry{
				Key:       v.Key,
				Value:     v.Value,
				ExpiresAt: v.ExpiresAt,
			}, tx.CommitAt); err != nil {
				return err
			}
		}
		for _, v := range tx.Deletes {
			if err := b.DeleteAt(v.Key, tx.CommitAt); err != nil {
				return err
			}
		}
		if err := b.Flush(); err != nil {
			return err
		}
		logger.C(context.Background()).Infof("transaction at %d done in %v", tx.CommitAt, time.Since(start))
		if r.IsLeader() {
			return nil
		}
		r.db.SetVersion(tx.CommitAt)
		return nil
	default:
		return nil
	}
}

func (r *Raft) Snapshot() (io.ReadCloser, error) {
	if !r.ready.Load() {
		return nil, fmt.Errorf("%w: not ready", raft.ErrFailedPrecondition)
	}
	if r.snap.Load() {
		return nil, raft.ErrAlreadySnapshotting
	}
	if len(r.buffs) != 0 {
		return nil, fmt.Errorf("%w: cannot snaphot while there is ongoing transactions", raft.ErrFailedPrecondition)
	}
	r.snap.Store(true)
	r.db.MaxVersion()
	rd, wr := io.Pipe()
	start := time.Now()
	e, err := zstd.NewWriter(wr, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(1)))
	if err != nil {
		r.snap.Store(false)
		return nil, err
	}
	go func() {
		defer r.snap.Store(false)
		defer rd.Close()
		defer wr.Close()
		defer e.Close()
		if err := r.db.Stream(context.Background(), 0, r.db.MaxVersion(), e); err != nil {
			logger.C(context.Background()).WithError(err).Error("raft: snapshot failed")
			return
		}
		logger.C(context.Background()).Infof("raft: snapshot done in %v", time.Since(start))
	}()
	return rd, nil
}

func (r *Raft) Restore(closer io.ReadCloser) error {
	log := logger.C(context.Background())
	log.Infof("raft: restoring snapshot")
	defer closer.Close()
	start := time.Now()
	d, err := zstd.NewReader(closer)
	if err != nil {
		return err
	}
	defer d.Close()
	log.Infof("raft: dropping all local data")
	// TODO(adphi): create backup before in case of something goes wrong
	if err := r.db.Drop(); err != nil {
		return err
	}
	log.Infof("raft: loading snapshot")
	v, err := r.db.Load(context.Background(), d)
	if err != nil {
		log.Errorf("raft: failed to load snapshot: %v", err)
		return err
	}
	// TODO(adphi): delete the backup
	log.Infof("raft: snapshot restored to %v in %v", v, time.Since(start))
	return nil
}

func (r *Raft) IsLeader() bool {
	return r.node.Leader() == r.node.Whoami()
}

func (r *Raft) LeaderConn() (grpc.ClientConnInterface, bool) {
	r.cmu.Lock()
	defer r.cmu.Unlock()
	id := r.node.Leader()
	c, ok := r.conns[id]
	if ok {
		return c.(*grpc.ClientConn), true
	}
	var addr string
	for _, v := range r.node.Members() {
		if v.ID() == r.node.Leader() {
			addr = v.Address()
		}
	}
	if addr == "" {
		return nil, false
	}
	var err error
	r.conns[id], err = grpc.Dial(addr, r.dopts...)
	if err != nil {
		logger.C(context.Background()).WithError(err).Error("failed to dial leader")
		return nil, false
	}
	return r.conns[id], true
}

func (r *Raft) CurrentLeader() string {
	for _, v := range r.node.Members() {
		if v.ID() == r.node.Leader() {
			return v.Address()
		}
	}
	return ""
}

func (r *Raft) Subscribe() <-chan string {
	ch := r.pub.Subscribe()
	return ch
}

func (r *Raft) LinearizableReads() bool {
	if err := r.node.LinearizableRead(context.Background()); err != nil {
		return false
	}
	return true
}

func (r *Raft) NewTx(ctx context.Context) (replication.Tx, error) {
	if err := r.node.LinearizableRead(ctx); err != nil {
		return nil, err
	}
	return &tx{r: r}, nil
}

func (r *Raft) Close() error {
	r.cancel()
	r.srv.Stop()
	r.node.Shutdown(context.Background())
	return r.g.Wait()
}

func (r *Raft) run(ctx context.Context, opts ...raft.StartOption) error {
	log := logger.C(ctx)
	ready := make(chan struct{})
	r.g.Go(func() error {
		if err := r.srv.Serve(r.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.WithError(err).Error("raft/gRPC failed to serve")
			return err
		}
		return nil
	})
	r.g.Go(func() error {
		defer close(r.sch)
		if err := r.node.Start(opts...); err != nil && !errors.Is(err, raft.ErrNodeStopped) {
			log.WithError(err).Error("raft/Node failed to start")
			return err
		}
		log.Infof("raft/Node: stopped")
		return nil
	})
	r.g.Go(func() error {
		closed := false
		for {
			select {
			case _, ok := <-r.sch:
				if !ok {
					return nil
				}
				if !closed && r.node.Leader() != 0 {
					close(ready)
					closed = true
				}
				var l string
				for _, v := range r.node.Members() {
					if v.ID() == r.node.Leader() {
						l = v.Address()
						break
					}
				}
				r.pub.Publish(l)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	go func() {
		defer r.db.Close()
		if err := r.g.Wait(); err != nil {
			logger.C(ctx).WithError(err).Error("raft stopped")
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
	}
	if err := r.node.LinearizableRead(ctx); err != nil {
		return err
	}
	r.ready.Store(true)
	return nil
}
