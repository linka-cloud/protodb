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
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"go.linka.cloud/grpc-toolkit/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	pb2 "go.linka.cloud/protodb/internal/replication/pb"
	"go.linka.cloud/protodb/pb"
)

type node struct {
	name  string
	meta  *pb2.Meta
	addr  net.IP
	cc    *grpc.ClientConn
	repl  pb2.ReplicationServiceClient
	proxy pb.ProtoDBServer
	// elect pb2.ReplicationService_ElectionClient
}

func (r *Repl) handleEvents(ctx context.Context) {
	for e := range r.events {
		go r.handleEvent(ctx, e)
	}
}

func (r *Repl) handleEvent(ctx context.Context, e memberlist.NodeEvent) {
	log := logger.C(ctx).WithField("component", "replication")
	r.mu.Lock()
	defer r.mu.Unlock()
	// log.Infof("event: %v", e)
	switch e.Event {
	case memberlist.NodeJoin, memberlist.NodeUpdate:
		if e.Node.Name == "" {
			log.Warnf("node joined with empty name")
			return
		}
		if e.Node.Name == r.name {
			return
		}
		if e.Event == memberlist.NodeJoin {
			log.Infof("node joined: %s %v", e.Node.Name, e.Node.Addr)
		} else {
			log.Infof("node updated: %s %v", e.Node.Name, e.Node.Addr)
		}
		m := &pb2.Meta{}
		if err := m.UnmarshalVT(e.Node.Meta); err != nil {
			log.Errorf("failed to unmarshal node meta: %v", err)
			return
		}

		if err := r.writeNodes(); err != nil {
			log.Errorf("failed to write nodes: %v", err)
		}
		for i, v := range r.bootNodes {
			if v != e.Node.Name {
				continue
			}
			r.bootNodes = append(r.bootNodes[:i], r.bootNodes[i+1:]...)
			log.Infof("known node %s is now online (version: %d) (lower: %v) (leader: %v)", e.Node.Name, m.LocalVersion, m.LocalVersion <= r.version, m.IsLeader)
			if len(r.bootNodes) == 0 {
				log.Infof("all known nodes are online")
			} else {
				log.Infof("waiting for %d nodes to be online %v", len(r.bootNodes), r.bootNodes)
			}
		}
		if m.LocalVersion >= r.maxVersion {
			r.maxVersion = m.LocalVersion
		}
		if r.maxVersion > r.version && len(r.bootNodes) == 0 {
			log.Infof("local version %d is behind max version %d: waiting for leader", r.version, r.maxVersion)
		}
		if (r.maxVersion <= r.version && len(r.bootNodes) == 0) || m.IsLeader {
			select {
			case <-r.converged:
			default:
				if m.IsLeader {
					log.Infof("node %s is the leader", e.Node.Name)
				} else {
					log.Infof("joining leader election")
				}
				close(r.converged)
			}
		}
		if v, ok := r.nodes.Load(e.Node.Name); ok {
			if v.addr.String() == e.Node.Addr.String() && v.cc.GetState() == connectivity.Ready {
				log.Infof("already connected to %s %v", e.Node.Name, e.Node.Addr)
				v.meta = m
				if e.Node.Name == r.leaderName.Load() {
					r.setReady()
				}
				return
			}
			log.Infof("closing connection to %s %v", e.Node.Name, e.Node.Addr)
			if err := v.cc.Close(); err != nil {
				log.Errorf("failed to close connection to %s: %v", e.Node.Name, err)
			}
			r.nodes.Delete(e.Node.Name)
		}
		c, err := grpc.Dial(fmt.Sprintf("%v:%d", e.Node.Addr, m.GRPCPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Errorf("failed to dial: %v", err)
			return
		}
		if c.GetState() != connectivity.Ready {
			log.Warnf("connection to %s is not ready", e.Node.Name)
		}
		cs := pb2.NewReplicationServiceClient(c)
		r.nodes.Store(e.Node.Name, &node{
			name:  e.Node.Name,
			addr:  e.Node.Addr,
			meta:  m,
			cc:    c,
			repl:  cs,
			proxy: pb.NewProtoDBProxy(pb.NewProtoDBClient(c)),
		})
		log.Infof("connected to %s", e.Node.Name)
		if e.Node.Name == r.leaderName.Load() {
			r.setReady()
		}
		go func() {
			defer func() {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.nodes.Delete(e.Node.Name)
				log.Infof("disconnected from %s", e.Node.Name)
				if e.Node.Name == r.leaderName.Load() {
					go r.Elect(ctx)
				}
			}()
			ss, err := cs.Alive(ctx)
			if err != nil {
				log.Errorf("failed to get alive stream: %v", err)
				return
			}
			for {
				if err := ss.Send(&pb2.Ack{}); err != nil {
					if errors.Is(err, io.EOF) {
						return
					}
					log.Errorf("failed to send ack: %v", err)
					return
				}
				if _, err := ss.Recv(); err != nil {
					if errors.Is(err, io.EOF) {
						return
					}
					log.Errorf("failed to recv ack: %v", err)
					return
				}
				time.Sleep(time.Second)
			}
		}()
	case memberlist.NodeLeave:
		log.Infof("node left: %s", e.Node.Name)
		if v, ok := r.nodes.Load(e.Node.Name); ok {
			if err := v.cc.Close(); err != nil {
				log.Errorf("failed to close connection: %v", err)
			}
			r.nodes.Delete(e.Node.Name)
		}
		if err := r.writeNodes(); err != nil {
			log.Errorf("failed to write nodes: %v", err)
		}
		if r.leaderName.Load() == e.Node.Name && !r.leading.Load() {
			r.leaderName.Store("")
			go r.Elect(ctx)
		}
	}
}

func (r *Repl) onStoppedLeading(ctx context.Context) {
	logger.C(ctx).Info("stopped leading")
	meta := r.meta.Load().CloneVT()
	meta.IsLeader = false
	r.meta.Store(meta)
	b, err := meta.MarshalVT()
	if err != nil {
		logger.C(ctx).Errorf("failed to marshal meta: %v", err)
	}
	// it may be an empty meta...
	if err := r.g.UpdateMeta(ctx, b); err != nil {
		logger.C(ctx).Errorf("failed to update meta: %v", err)
	}
	if err := r.g.Memberlist().Leave(time.Second); err != nil {
		logger.C(ctx).Errorf("failed to leave memberlist: %v", err)
	}
	if err := r.db.Close(); err != nil {
		logger.C(ctx).Errorf("failed to close db: %v", err)
	}
}

func (r *Repl) onNewLeader(ctx context.Context, identity string) {
	log := logger.C(ctx)
	if identity == "" {
		log.Warnf("empty leader")
		return
	}
	leader := identity == r.name
	log.Infof("new leader: %s (us: %v)", identity, leader)
	r.leaderName.Store(identity)
	r.leading.Store(leader)
	meta := r.meta.Load().CloneVT()
	meta.IsLeader = leader
	r.meta.Store(meta)
	if leader {
		b, err := meta.CloneVT().MarshalVT()
		if err != nil {
			log.Errorf("failed to marshal meta: %v", err)
			return
		}
		log.Infof("broadcasting leadership")
		if err := r.g.UpdateMeta(ctx, b); err != nil {
			log.Errorf("failed to update node: %v", err)
		}
		if err := r.db.LoadDescriptors(ctx); err != nil {
			log.Errorf("failed to load proto descriptors: %v", err)
		}
	}
	r.pub.Publish(identity)
	if _, ok := r.leaderClient(); identity != r.name && !ok {
		log.Warnf("missing client for leader: %s", identity)
	}
	r.setReady()
}

func (r *Repl) setReady() {
	r.once.Do(func() {
		logger.C(r.ctx).Info("setting ready")
		close(r.ready)
	})
}

func (r *Repl) writeNodes() error {
	var nodes []string
	for _, v := range r.g.Memberlist().Members() {
		nodes = append(nodes, v.Name)
	}
	if r.db.InMemory() {
		return nil
	}
	p := filepath.Join(r.db.Path(), "nodes")
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte(strings.Join(nodes, "\n"))); err != nil {
		return err
	}
	return f.Sync()
}

func (r *Repl) loadNodes() ([]string, error) {
	if r.db.InMemory() {
		return nil, nil
	}
	p := filepath.Join(r.db.Path(), "nodes")
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var nodes []string
	for _, v := range strings.Split(string(b), "\n") {
		if v != "" && v != r.name {
			nodes = append(nodes, v)
		}
	}
	return nodes, nil
}
