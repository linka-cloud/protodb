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
	"time"
)

var defaultOptions = options{
	addrs:      []string{"localhost"},
	gossipPort: 7080,
	grpcPort:   7081,
	tick:       500 * time.Millisecond,
}

type Option func(o *options)

type options struct {
	mode       Mode
	name       string
	addrs      []string
	gossipPort int
	grpcPort   int
	tick       time.Duration

	// TODO(adphi): replication service.Options, e.g. peer tls, server tls, etc.
	// TODO(adphi): protodb service.Options, e.g. peer tls, server tls, etc.
	// TODO(adphi): gossip encryption key & verification
}

func WithMode(mode Mode) Option {
	return func(o *options) {
		o.mode = mode
	}
}

func WithName(name string) Option {
	return func(o *options) {
		o.name = name
	}
}

func WithAddrs(addrs ...string) Option {
	return func(o *options) {
		o.addrs = addrs
	}
}

func WithGossipPort(port int) Option {
	return func(o *options) {
		o.gossipPort = port
	}
}

func WithGRPCPort(port int) Option {
	return func(o *options) {
		o.grpcPort = port
	}
}

func WithTick(ms int) Option {
	return func(o *options) {
		if ms > 100 {
			o.tick = time.Duration(ms) * time.Millisecond
		}
	}
}
