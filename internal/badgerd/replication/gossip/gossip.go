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
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
	"github.com/hashicorp/memberlist"
	"go.linka.cloud/grpc-toolkit/interceptors/validation"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/grpc-toolkit/service"
	pubsub "go.linka.cloud/pubsub/typed"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	"go.linka.cloud/protodb/internal/badgerd/dns"
	"go.linka.cloud/protodb/internal/badgerd/replication"
	pb2 "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
	"go.linka.cloud/protodb/pb"
)

var (
	_ pb2.ReplicationServiceServer = (*Gossip)(nil)
	_ replication.Replication      = (*Gossip)(nil)
)

var defaultOptions = replication.Options{
	Addrs:      []string{"localhost"},
	GossipPort: 7080,
	GRPCPort:   7081,
	Tick:       250 * time.Millisecond,
}

type Gossip struct {
	pb2.UnsafeReplicationServiceServer
	pb.UnsafeProtoDBServer

	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.RWMutex
	name string
	mode replication.Mode

	// we use atomic to avoid locking as these values are read very often
	leading    *Atomic[bool]
	leaderName *Atomic[string]
	meta       Atomic[*pb2.Meta]

	version uint64
	db      replication.DB
	nodes   Map[*node]

	svc       service.Service
	h         pb.ProtoDBServer
	events    chan memberlist.NodeEvent
	once      sync.Once
	ready     chan struct{}
	list      *memberlist.Memberlist
	d         *delegate
	co        sync.Once
	pub       pubsub.Publisher[string]
	converged chan struct{}

	bootNodes  []string
	maxVersion uint64

	txnMark   *y.WaterMark
	txnCloser *z.Closer
}

func New(ctx context.Context, db replication.DB, opts ...replication.Option) (replication.Replication, error) {
	o := defaultOptions
	for _, v := range opts {
		v(&o)
	}
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if o.Name == "" {
		o.Name = h
	}

	r := &Gossip{
		mode:       o.Mode,
		leading:    NewAtomic(false),
		leaderName: NewAtomic(""),
		db:         db,
		name:       o.Name,
		ready:      make(chan struct{}),
		events:     make(chan memberlist.NodeEvent, 10),
		converged:  make(chan struct{}),
		version:    db.MaxVersion(),
		pub:        pubsub.NewPublisher[string](time.Second, 2),
		txnMark:    &y.WaterMark{Name: "gossip.TxnReplicationTimestamp"},
		txnCloser:  z.NewCloser(1),
	}
	r.txnMark.Init(r.txnCloser)
	r.meta.Store(&pb2.Meta{GRPCPort: uint32(o.GRPCPort), LocalVersion: r.version})
	r.ctx, r.cancel = context.WithCancel(ctx)

	if len(o.Addrs) == 0 {
		return nil, errors.New("replication: missing cluster addresses")
	}

	if o.GRPCPort == 0 {
		return nil, errors.New("replication: missing grpc port")
	}

	if o.GossipPort == 0 {
		return nil, errors.New("replication: missing gossip port")
	}

	log := logger.C(ctx).WithFields("name", "replication", "node", r.name)

	r.ctx = logger.Set(r.ctx, log)
	ctx = logger.Set(ctx, log)

	r.bootNodes, err = r.loadNodes()
	if err != nil {
		return nil, fmt.Errorf("replication: failed to load saved nodes: %w", err)
	}

	serverStarted := make(chan struct{})
	tls, err := o.TLS()
	if err != nil {
		return nil, err
	}
	r.svc, err = service.New(
		service.WithContext(ctx),
		service.WithName("protodb-replication"),
		// TODO(adphi): make that configurable
		service.WithAddress(fmt.Sprintf(":%d", o.GRPCPort)),
		service.WithTLSConfig(tls),
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

	for _, v := range o.ExtraServices {
		v(r.svc)
	}

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

	m, err := r.meta.Load().CloneVT().MarshalVT()
	if err != nil {
		return nil, err
	}
	r.d = &delegate{meta: m}

	c := memberlist.DefaultLocalConfig()
	c.Name = r.name
	c.BindPort = o.GossipPort
	c.RetransmitMult = 4
	c.PushPullInterval = 0
	c.DisableTcpPings = true
	c.GossipInterval = o.Tick
	c.Events = &memberlist.ChannelEventDelegate{Ch: r.events}
	c.DeadNodeReclaimTime = time.Second
	c.GossipToTheDeadTime = time.Second
	c.Delegate = r.d
	c.Logger = newLogger(ctx)
	if o.EncryptionKey != "" {
		h := sha256.New()
		if _, err := h.Write([]byte(o.EncryptionKey)); err != nil {
			return nil, err
		}
		c.SecretKey = h.Sum(nil)
	}
	log.Infof("starting memberlist: %v", o.Addrs)
	p := dns.NewProvider(ctx, dns.MiekgdnsResolverType)
	if err := p.Resolve(ctx, o.Addrs); err != nil {
		return nil, err
	}
	o.Addrs = p.Addresses()
	if c.RetransmitMult < len(o.Addrs) {
		c.RetransmitMult = len(o.Addrs)
	}

	select {
	case <-serverStarted:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	r.list, err = memberlist.Create(c)
	if err != nil {
		return nil, err
	}
	r.list.LocalNode().Meta = m
	if _, err := r.list.Join(o.Addrs); err != nil {
		return nil, err
	}

	r.run(ctx)
	<-r.ready
	log.Infof("init replication")
	if err := r.init(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Gossip) init(ctx context.Context) error {
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
	v, err := r.db.Load(ctx, &reader{ss: ss})
	if err != nil {
		return err
	}
	r.txnMark.Done(v)
	logger.C(ctx).Infof("initial replication from leader to %d done", v)
	return nil
}

func (r *Gossip) run(ctx context.Context) {
	log := logger.C(ctx)
	if len(r.bootNodes) == 0 && len(r.list.Members()) == 0 {
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

func (r *Gossip) Subscribe() <-chan string {
	ch := r.pub.Subscribe()
	return ch
}

func (r *Gossip) clients() []*node {
	nodes := r.nodes.Values()
	logger.StandardLogger().Debugf("replication clients: %d", len(nodes))
	return nodes
}

func (r *Gossip) leaderClient() (pb2.ReplicationServiceClient, bool) {
	if r.leading.Load() {
		return nil, false
	}
	c, ok := r.nodes.Load(r.leaderName.Load())
	if !ok {
		return nil, false
	}
	return c.repl, true
}

func (r *Gossip) updateMeta(_ context.Context, b []byte) error {
	r.d.SetMeta(b)
	return r.list.UpdateNode(time.Second)
}

func (r *Gossip) LeaderConn() (grpc.ClientConnInterface, bool) {
	if r.leading.Load() {
		return nil, false
	}
	c, ok := r.nodes.Load(r.leaderName.Load())
	if !ok {
		return nil, false
	}
	return c.cc, true
}

func (r *Gossip) IsLeader() bool {
	return r.leading.Load()
}

func (r *Gossip) HasLeader() bool {
	return r.leaderName.Load() != ""
}

func (r *Gossip) CurrentLeader() string {
	return r.leaderName.Load()
}

func (r *Gossip) Mode() replication.Mode {
	return r.mode
}

func (r *Gossip) LinearizableReads() bool {
	return r.mode == replication.ModeSync
}

func (r *Gossip) Close() (err error) {
	r.co.Do(func() {
		r.cancel()
		r.txnCloser.Done()
		err = multierr.Combine(
			r.svc.Stop(),
			r.list.Leave(time.Second),
			r.list.Shutdown(),
		)
	})
	return
}
