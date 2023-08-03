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

var defaultReplicationOptions = replOptions{
	addrs:      []string{"localhost"},
	gossipPort: 7080,
	grpcPort:   7081,
	// TODO(adphi): certificates, gossip encryption, etc.
}

type ReplicationOption func(o *replOptions)

type replOptions struct {
	addrs      []string
	gossipPort int
	grpcPort   int
	mode       ReplicationMode
}

func WithAddrs(addrs ...string) ReplicationOption {
	return func(o *replOptions) {
		o.addrs = addrs
	}
}

func WithGossipPort(port int) ReplicationOption {
	return func(o *replOptions) {
		o.gossipPort = port
	}
}

func WithGRPCPort(port int) ReplicationOption {
	return func(o *replOptions) {
		o.grpcPort = port
	}
}
