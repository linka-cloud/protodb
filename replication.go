// Copyright 2022 Linka Cloud  All rights reserved.
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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"go.linka.cloud/grpc/logger"
	"go.linka.cloud/grpc/service"
	le "go.linka.cloud/leaderelection"
	"go.linka.cloud/leaderelection/gossip"

	repl2 "go.linka.cloud/protodb/internal/replication"
)

const (
	ReplicationModeNone ReplicationMode = iota
	ReplicationModeAsync
	ReplicationModeSent
	ReplicationModeSync
)

type ReplicationMode uint8

func (m ReplicationMode) String() string {
	switch m {
	case ReplicationModeNone:
		return "none"
	case ReplicationModeAsync:
		return "async"
	case ReplicationModeSent:
		return "sent"
	case ReplicationModeSync:
		return "sync"
	default:
		return "unknown"
	}
}

var _ repl2.ReplicationServiceServer = (*repl)(nil)

type repl struct {
	repl2.UnsafeReplicationServiceServer

	mu   sync.RWMutex
	name string
	mode ReplicationMode

	l      bool
	cle    string
	db     *db
	cs     map[string]repl2.ReplicationServiceClient
	cancel context.CancelFunc

	le    *le.LeaderElector
	svc   service.Service
	nch   chan memberlist.NodeEvent
	ready chan struct{}
}

func newRepl(ctx context.Context, db *db, o replOptions) (*repl, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	r := &repl{
		mode:  o.mode,
		l:     false,
		db:    db,
		cs:    nil,
		name:  fmt.Sprintf("%s:%d", h, o.gossipPort),
		ready: make(chan struct{}),
		nch:   make(chan memberlist.NodeEvent, 10),
	}
	ctx, r.cancel = context.WithCancel(ctx)

	if len(o.addrs) == 0 {
		return nil, errors.New("replication: missing cluster addresses")
	}

	if o.grpcPort == 0 {
		return nil, errors.New("replication: missing grpc port")
	}

	if o.gossipPort == 0 {
		return nil, errors.New("replication: missing gossip port")
	}

	r.svc, err = service.New(
		service.WithName("protodb-replication"),
		service.WithAddress(fmt.Sprintf(":%d", o.grpcPort)),
	)
	if err != nil {
		return nil, err
	}

	repl2.RegisterReplicationServiceServer(r.svc, r)

	tick := 100 * time.Millisecond

	c := memberlist.DefaultLocalConfig()
	c.Name = r.name
	c.BindPort = o.gossipPort
	c.RetransmitMult = len(o.addrs)
	c.PushPullInterval = 0
	c.GossipVerifyIncoming = true
	c.GossipVerifyOutgoing = true
	c.DisableTcpPings = true
	c.GossipInterval = tick
	c.Events = &memberlist.ChannelEventDelegate{Ch: r.nch}
	m, err := (&repl2.Meta{GRPCPort: uint32(o.grpcPort)}).MarshalVT()
	if err != nil {
		return nil, err
	}
	l, err := gossip.New(ctx, c, "protodb", r.name, m, o.addrs...)
	if err != nil {
		return nil, err
	}
	log := logger.C(ctx).WithFields("name", "protodb-replication", "node", c.Name)
	ctx = logger.Set(ctx, log)
	lec := le.Config{
		Name:            "protodb",
		Lock:            l,
		LeaseDuration:   15 * tick,
		RenewDeadline:   10 * tick,
		RetryPeriod:     2 * tick,
		ReleaseOnCancel: true,
		Callbacks: le.Callbacks{
			OnStartedLeading: r.OnStartedLeading(ctx),
			OnStoppedLeading: r.OnStoppedLeading(ctx),
			OnNewLeader:      r.OnNewLeader(ctx),
		},
	}
	log.Infof("starting memberlist: %v", o.addrs)
	r.le, err = le.New(lec)
	if err != nil {
		return nil, err
	}
	r.run(ctx)
	<-r.ready
	if err := r.init(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *repl) init(ctx context.Context) error {
	if r.isLeader() {
		logger.C(ctx).Info("leader: skipping replication init")
		return nil
	}
	var c repl2.ReplicationServiceClient
	for {
		var ok bool
		c, ok = r.leader()
		if ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	since := r.db.bdb.MaxVersion()
	logger.C(ctx).Infof("initializing replication from leader since %d", since)
	ss, err := c.Init(ctx, &repl2.InitRequest{Since: since})
	if err != nil {
		return err
	}
	if err := r.db.bdb.Load(&replReader{ss: ss}, 1024); err != nil {
		return err
	}
	v := r.db.bdb.MaxVersion()
	r.db.orc.txnMark.Done(v)
	r.db.orc.readMark.Done(v)
	r.db.orc.nextTxnTs = v + 1
	logger.C(ctx).Infof("inital replication from leader from %d to %d done", since, v)
	return nil
}

func (r *repl) run(ctx context.Context) {
	log := logger.C(ctx)
	go r.handleEvents(ctx)
	go func() {
		r.le.Run(ctx)
		if err := r.db.Close(); err != nil {
			log.Errorf("failed to close db: %v", err)
		}
	}()
	go func() {
		if err := r.svc.Start(); err != nil {
			if !errors.Is(err, context.Canceled) {
				r.db.Close()
			}
			log.Errorf("failed to start grpc server: %v", err)
		}
	}()
}

func (r *repl) clients() []repl2.ReplicationServiceClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var clients []repl2.ReplicationServiceClient
	for _, v := range r.cs {
		clients = append(clients, v)
	}
	logger.StandardLogger().Debugf("replication clients: %d", len(clients))
	return clients
}

func (r *repl) leader() (repl2.ReplicationServiceClient, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.l {
		return nil, false
	}
	c, ok := r.cs[r.cle]
	return c, ok
}

func (r *repl) isLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.l
}

func (r *repl) currentLeader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cle
}

func (r *repl) close() {
	r.cancel()
}

type replWriter struct {
	ss repl2.ReplicationService_InitServer
}

func (w *replWriter) Write(p []byte) (n int, err error) {
	if err := w.ss.Send(&repl2.InitResponse{
		Data: p,
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}

type replReader struct {
	ss   repl2.ReplicationService_InitClient
	buff []byte
}

func (r *replReader) Read(p []byte) (n int, err error) {
	for {
		if len(r.buff) > 0 && len(p) > 0 {
			n := copy(p, r.buff)
			r.buff = r.buff[n:]
			return n, nil
		}
		msg, err := r.ss.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, err
		}
		if len(msg.Data) > len(p) {
			r.buff = msg.Data[len(p):]
			msg.Data = msg.Data[:len(p)]
		}
		n := copy(p, msg.Data)
		return n, nil
	}
}
