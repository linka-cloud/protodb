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
	"fmt"

	"github.com/hashicorp/memberlist"
	"go.linka.cloud/grpc/logger"
	"google.golang.org/grpc"

	repl2 "go.linka.cloud/protodb/internal/replication"
)

func (r *repl) handleEvents(ctx context.Context) {
	log := logger.C(ctx).WithField("component", "replication")
	for e := range r.nch {
		switch e.Event {
		case memberlist.NodeJoin:
			if e.Node.Name == r.name {
				continue
			}
			r.mu.Lock()
			log.Infof("node joined: %s", e.Node.Name)
			m := &repl2.Meta{}
			if err := m.UnmarshalVT(e.Node.Meta); err != nil {
				r.mu.Unlock()
				log.Errorf("failed to unmarshal node meta: %v", err)
				continue
			}
			c, err := grpc.Dial(fmt.Sprintf("%v:%d", e.Node.Addr, m.GRPCPort), grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				r.mu.Unlock()
				log.Errorf("failed to dial: %v", err)
				continue
			}
			if r.cs == nil {
				r.cs = make(map[string]repl2.ReplicationServiceClient)
			}
			r.cs[e.Node.Name] = repl2.NewReplicationServiceClient(c)
			log.Infof("connected to %s", e.Node.Name)
			r.mu.Unlock()
			if e.Node.Name == r.currentLeader() {
				maybeClose(r.ready)
			}
		case memberlist.NodeLeave:
			log.Infof("node left: %s", e.Node.Name)
			r.mu.Lock()
			if r.cs == nil {
				r.mu.Unlock()
				continue
			}
			delete(r.cs, e.Node.Name)
			r.mu.Unlock()
		case memberlist.NodeUpdate:
			log.Infof("node updated: %s", e.Node.Name)
		}
	}
}

func (r *repl) OnStartedLeading(ctx context.Context) func(ctx context.Context) {
	return func(ctx context.Context) {
		logger.C(ctx).Info("started leading")
	}
}

func (r *repl) OnStoppedLeading(ctx context.Context) func() {
	return func() {
		logger.C(ctx).Info("stopped leading")
		if err := r.db.Close(); err != nil {
			logger.C(ctx).Errorf("failed to close db: %v", err)
		}
	}
}

func (r *repl) OnNewLeader(ctx context.Context) func(leader string) {
	return func(identity string) {
		log := logger.C(ctx)
		log.Infof("new leader: %s", identity)
		r.mu.Lock()
		r.cle = identity
		r.l = identity == r.name
		r.mu.Unlock()
		if _, ok := r.leader(); identity != r.name && !ok {
			log.Warnf("missing client for leader: %s", identity)
			return
		}
		maybeClose(r.ready)
	}
}

func maybeClose[T any](ch chan T) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
