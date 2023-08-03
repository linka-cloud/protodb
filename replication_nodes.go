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
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/memberlist"
	"go.linka.cloud/grpc-toolkit/logger"
	"google.golang.org/grpc"

	repl2 "go.linka.cloud/protodb/internal/replication"
)

func (r *repl) handleEvents(ctx context.Context) {
	for e := range r.nch {
		r.handleEvent(ctx, e)
	}
}

func (r *repl) handleEvent(ctx context.Context, e memberlist.NodeEvent) {
	log := logger.C(ctx).WithField("component", "replication")
	r.mu.Lock()
	defer r.mu.Unlock()
	switch e.Event {
	case memberlist.NodeJoin:
		if e.Node.Name == "" {
			log.Warnf("node joined with empty name")
			return
		}
		if e.Node.Name == r.name {
			return
		}
		log.Infof("node joined: %s", e.Node.Name)
		m := &repl2.Meta{}
		if err := m.UnmarshalVT(e.Node.Meta); err != nil {
			log.Errorf("failed to unmarshal node meta: %v", err)
			return
		}
		if _, ok := r.cs[e.Node.Name]; ok {
			return
		}
		c, err := grpc.Dial(fmt.Sprintf("%v:%d", e.Node.Addr, m.GRPCPort), grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Errorf("failed to dial: %v", err)
			return
		}
		if r.cs == nil {
			r.cs = make(map[string]repl2.ReplicationServiceClient)
		}
		cs := repl2.NewReplicationServiceClient(c)
		r.cs[e.Node.Name] = cs
		if r.ccs == nil {
			r.ccs = make(map[string]*grpc.ClientConn)
		}
		r.ccs[e.Node.Name] = c
		log.Infof("connected to %s", e.Node.Name)
		if e.Node.Name == r.cle {
			r.maybeClose(r.ready)
		}
		go func() {
			ss, err := cs.Alive(ctx)
			defer func() {
				r.mu.Lock()
				delete(r.cs, e.Node.Name)
				delete(r.ccs, e.Node.Name)
				r.mu.Unlock()
				log.Infof("disconnected from %s", e.Node.Name)
			}()
			if err != nil {
				log.Errorf("failed to get alive stream: %v", err)
				return
			}
			for {
				if err := ss.Send(&repl2.Ack{}); err != nil {
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
		if r.cs == nil {
			return
		}
		if v, ok := r.ccs[e.Node.Name]; ok {
			if err := v.Close(); err != nil {
				log.Errorf("failed to close connection: %v", err)
			}
			delete(r.ccs, e.Node.Name)
		}
		delete(r.cs, e.Node.Name)
	case memberlist.NodeUpdate:
		log.Infof("node updated: %s", e.Node.Name)
	}
}

func (r *repl) OnStartedLeading(_ context.Context) func(ctx context.Context) {
	return func(ctx context.Context) {
		logger.C(ctx).Info("started leading")
	}
}

func (r *repl) OnStoppedLeading(ctx context.Context) func() {
	return func() {
		logger.C(ctx).Info("stopped leading")
		if err := r.close(); err != nil {
			logger.C(ctx).Errorf("failed to close: %v", err)
		}
		if err := r.db.Close(); err != nil {
			logger.C(ctx).Errorf("failed to close db: %v", err)
		}
	}
}

func (r *repl) OnNewLeader(ctx context.Context) func(leader string) {
	return func(identity string) {
		log := logger.C(ctx)
		if identity == "" {
			log.Warnf("empty leader")
			return
		}
		log.Infof("new leader: %s", identity)
		r.mu.Lock()
		r.cle = identity
		r.l = identity == r.name
		r.mu.Unlock()
		if _, ok := r.leader(); identity != r.name && !ok {
			log.Warnf("missing client for leader: %s", identity)
			return
		}
		// if identity == r.name {
		// 	r.mu.Lock()
		// 	defer r.mu.Unlock()
		// 	for k, v := range r.ccs {
		// 		if v.GetState() != connectivity.Ready {
		// 			log.Warnf("client %s not ready, deleting", k)
		// 			if err := v.Close(); err != nil {
		// 				log.Errorf("failed to close connection: %v", err)
		// 			}
		// 			delete(r.ccs, k)
		// 			delete(r.cs, k)
		// 		}
		// 	}
		// 	// 	if err := r.db.load(ctx); err != nil {
		// 	// 		log.Errorf("failed to load protobuf schemas: %v", err)
		// 	// 		return
		// 	// 	}
		// }
		r.maybeClose(r.ready)
	}
}

func (r *repl) maybeClose(ch chan struct{}) {
	r.once.Do(func() {
		close(ch)
	})
}
