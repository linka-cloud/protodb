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
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"go.linka.cloud/grpc-toolkit/interceptors/validation"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/grpc-toolkit/service"
	"go.linka.cloud/leaderelection/gossip"
	pubsub "go.linka.cloud/pubsub/typed"
	"google.golang.org/grpc"

	pb2 "go.linka.cloud/protodb/internal/replication/pb"
	"go.linka.cloud/protodb/internal/server"
	"go.linka.cloud/protodb/pb"
)

// maxMsgSize is the maximum message size accepted by the gRPC server.
// It should be 4MiB, but we set it to 4MB to avoid exceeding the allowed size
const maxMsgSize = 4 * 1000 * 1000

const (
	ModeSync Mode = iota
	ModeAsync
)

type Mode uint8

func (m Mode) String() string {
	switch m {
	case ModeAsync:
		return "async"
	case ModeSync:
		return "sync"
	default:
		return "unknown"
	}
}

var _ pb2.ReplicationServiceServer = (*Repl)(nil)

type Repl struct {
	pb2.UnsafeReplicationServiceServer
	pb.UnsafeProtoDBServer

	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.RWMutex
	name string
	mode Mode

	// we use atomic to avoid locking as these values are read very often
	leading    *Atomic[bool]
	leaderName *Atomic[string]
	meta       Atomic[*pb2.Meta]

	version uint64
	db      DB
	nodes   Map[*node]

	svc       service.Service
	h         pb.ProtoDBServer
	events    chan memberlist.NodeEvent
	once      sync.Once
	ready     chan struct{}
	g         gossip.Lock
	co        sync.Once
	pub       pubsub.Publisher[string]
	converged chan struct{}

	bootNodes  []string
	maxVersion uint64
}

func New(ctx context.Context, db DB, opts ...Option) (*Repl, error) {
	o := defaultOptions
	for _, v := range opts {
		v(&o)
	}
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if o.name == "" {
		o.name = h
	}

	r := &Repl{
		mode:       o.mode,
		leading:    NewAtomic(false),
		leaderName: NewAtomic(""),
		db:         db,
		name:       o.name,
		ready:      make(chan struct{}),
		events:     make(chan memberlist.NodeEvent, 10),
		converged:  make(chan struct{}),
		version:    db.MaxVersion(),
		pub:        pubsub.NewPublisher[string](time.Second, 2),
	}
	r.meta.Store(&pb2.Meta{GRPCPort: uint32(o.grpcPort), LocalVersion: r.version})
	r.ctx, r.cancel = context.WithCancel(ctx)

	if len(o.addrs) == 0 {
		return nil, errors.New("replication: missing cluster addresses")
	}

	if o.grpcPort == 0 {
		return nil, errors.New("replication: missing grpc port")
	}

	if o.gossipPort == 0 {
		return nil, errors.New("replication: missing gossip port")
	}

	log := logger.C(ctx).WithFields("name", "protodb-replication", "node", r.name)

	r.ctx = logger.Set(r.ctx, log)
	ctx = logger.Set(ctx, log)

	r.bootNodes, err = r.loadNodes()
	if err != nil {
		return nil, fmt.Errorf("replication: failed to load saved nodes: %w", err)
	}

	serverStarted := make(chan struct{})
	r.svc, err = service.New(
		service.WithContext(ctx),
		service.WithName("protodb-replication"),
		service.WithAddress(fmt.Sprintf(":%d", o.grpcPort)),
		service.WithInterceptors(validation.NewInterceptors(true)),
		service.WithAfterStart(func() error {
			close(serverStarted)
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}

	pb2.RegisterReplicationServiceServer(r.svc, r)
	r.h, err = server.NewServer(r.db)
	if err != nil {
		return nil, err
	}
	pb.RegisterProtoDBServer(r.svc, r)

	go func() {
		log.Infof("starting server")
		if err := r.svc.Start(); err != nil {
			r.cancel()
			if !errors.Is(err, context.Canceled) {
				r.db.Close()
			}
			log.Errorf("failed to run grpc server: %v", err)
		}
	}()

	c := memberlist.DefaultLocalConfig()
	c.Name = r.name
	c.BindPort = o.gossipPort
	c.RetransmitMult = 4
	c.PushPullInterval = 0
	c.DisableTcpPings = true
	c.GossipInterval = o.tick
	c.Events = &memberlist.ChannelEventDelegate{Ch: r.events}
	c.DeadNodeReclaimTime = time.Second
	c.GossipToTheDeadTime = time.Second
	m, err := r.meta.Load().CloneVT().MarshalVT()
	if err != nil {
		return nil, err
	}

	select {
	case <-serverStarted:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	log.Infof("starting memberlist: %v", o.addrs)
	r.g, err = gossip.New(ctx, c, "protodb", r.name, m, o.addrs...)
	if err != nil {
		return nil, err
	}

	r.run(ctx)
	<-r.ready
	log.Infof("init replication")
	if err := r.init(ctx); err != nil {
		return nil, err
	}
	if err := r.db.LoadDescriptors(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repl) init(ctx context.Context) error {
	log := logger.C(ctx)
	if r.IsLeader() {
		log.Info("leader: skipping replication init")
		return nil
	}
	var c pb2.ReplicationServiceClient
	for {
		var ok bool
		c, ok = r.leaderClient()
		if ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	log.Infof("initializing replication from leader since %d", r.version)
	ss, err := c.Init(ctx, &pb2.InitRequest{Since: r.version})
	if err != nil {
		return err
	}
	if err := r.db.Load(ctx, &reader{ss: ss}); err != nil {
		return err
	}
	return nil
}

func (r *Repl) run(ctx context.Context) {
	log := logger.C(ctx)
	if len(r.bootNodes) == 0 && len(r.g.Memberlist().Members()) == 0 {
		close(r.converged)
	}
	go r.handleEvents(ctx)
	go func() {
		log.Infof("waiting for leader or version to converge")
		<-r.converged
		log.Infof("leader or version converged")
		time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond + 100*time.Millisecond)
		r.Elect(ctx)
	}()
}

func (r *Repl) Subscribe() <-chan string {
	ch := r.pub.Subscribe()
	return ch
}

func (r *Repl) clients() []*node {
	nodes := r.nodes.Values()
	logger.StandardLogger().Debugf("replication clients: %d", len(nodes))
	return nodes
}

func (r *Repl) leaderClient() (pb2.ReplicationServiceClient, bool) {
	if r.leading.Load() {
		return nil, false
	}
	c, ok := r.nodes.Load(r.leaderName.Load())
	if !ok {
		return nil, false
	}
	return c.repl, true
}

func (r *Repl) LeaderConn() (grpc.ClientConnInterface, bool) {
	if r.leading.Load() {
		return nil, false
	}
	c, ok := r.nodes.Load(r.leaderName.Load())
	if !ok {
		return nil, false
	}
	return c.cc, true
}

func (r *Repl) IsLeader() bool {
	return r.leading.Load()
}

func (r *Repl) HasLeader() bool {
	return r.leaderName.Load() != ""
}

func (r *Repl) CurrentLeader() string {
	return r.leaderName.Load()
}

func (r *Repl) Mode() Mode {
	return r.mode
}

func (r *Repl) Close() (err error) {
	r.co.Do(func() {
		r.cancel()
		r.g.Close()
		err = r.svc.Stop()
	})
	return
}
