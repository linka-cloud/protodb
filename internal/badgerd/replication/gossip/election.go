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
	"strings"

	"go.linka.cloud/grpc-toolkit/logger"

	pb2 "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func (r *Gossip) Elect(ctx context.Context) {
	meta := r.meta.Load().CloneVT()
	log := logger.C(ctx)
	nodes := r.clients()
	log.Info("starting election")
	count := 0
	var n *node
	for _, v := range nodes {
		if v.meta.IsLeader {
			n = v
		}
	}
	if n != nil {
		log.Infof("found leader %s", n.name)
		r.onNewLeader(ctx, n.name)
		return
	}
	for _, v := range nodes {
		if v.meta.LocalVersion < meta.LocalVersion || strings.Compare(v.name, r.name) > 0 {
			continue
		}
		count++
		log.Infof("sending election message to %s", v.name)
		res, err := v.repl.Election(ctx, &pb2.Message{
			Name: r.name,
			Type: pb2.ElectionTypeElection,
			Meta: meta.CloneVT(),
		})
		if err != nil {
			logger.C(ctx).WithError(err).Error("failed to send election message to %s", v.name)
			return
		}
		switch res.Type {
		case pb2.ElectionTypeOk:
			log.Infof("received ok from %s", res.Name)
			return
		case pb2.ElectionTypeSkip:
			log.Infof("received skip from %s", res.Name)
			continue
		case pb2.ElectionTypeNone:
			// this should not happen
		case pb2.ElectionTypeLeader:
			log.Infof("received leader from %s", res.Name)
			r.onNewLeader(ctx, res.Name)
			return
		}
	}
	if count != 0 {
		// we are not the leader
		return
	}
	// we are the leader
	log.Infof("we are the leader")
	r.onNewLeader(ctx, r.name)
	for _, v := range nodes {
		log.Infof("sending leader message to %s", v.name)
		if _, err := v.repl.Election(ctx, &pb2.Message{Type: pb2.ElectionTypeLeader, Name: r.name, Meta: meta.CloneVT()}); err != nil {
			log.WithError(err).Errorf("failed to send leader message to %s", v.name)
		}
	}
}

func (r *Gossip) Election(_ context.Context, req *pb2.Message) (*pb2.Message, error) {
	meta := r.meta.Load().CloneVT()
	log := logger.C(r.ctx)
	var t pb2.ElectionType
	select {
	case <-r.ready:
		switch req.Type {
		case pb2.ElectionTypeElection:
			if r.IsLeader() {
				log.Infof("sending leader message to %s", req.Name)
				return &pb2.Message{Type: pb2.ElectionTypeLeader, Name: r.name, Meta: meta.CloneVT()}, nil
			}
			// send ok if we have a higher version
			if req.Meta.LocalVersion < meta.LocalVersion || strings.Compare(req.Name, r.name) > 0 {
				t = pb2.ElectionTypeOk
			}
			go r.Elect(context.Background())
		case pb2.ElectionTypeLeader:
			if r.IsLeader() && req.Name != r.name {
				go r.onStoppedLeading(r.ctx)
				return &pb2.Message{Name: r.name, Meta: meta.CloneVT()}, nil
			}
			r.onNewLeader(r.ctx, req.Name)
			return &pb2.Message{Name: r.name, Meta: meta.CloneVT()}, nil
		}
	default:
		if req.Type == pb2.ElectionTypeLeader {
			r.onNewLeader(r.ctx, req.Name)
		} else {
			t = pb2.ElectionTypeSkip
		}
	}
	return &pb2.Message{Type: t, Name: r.name, Meta: meta.CloneVT()}, nil
}
