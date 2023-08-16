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
	"crypto/tls"
	"errors"
	"time"
)

var defaultOptions = options{
	addrs:      []string{"localhost"},
	gossipPort: 7080,
	grpcPort:   7081,
	tick:       250 * time.Millisecond,
}

type Option func(o *options)

type options struct {
	mode       Mode
	name       string
	addrs      []string
	gossipPort int
	grpcPort   int
	tick       time.Duration

	serverCert []byte
	serverKey  []byte
	tlsConfig  *tls.Config

	encryptionKey string
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

func WithServerCert(cert []byte) Option {
	return func(o *options) {
		o.serverCert = cert
	}
}

func WithServerKey(key []byte) Option {
	return func(o *options) {
		o.serverKey = key
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = config
	}
}

func WithEncryptionKey(key string) Option {
	return func(o *options) {
		o.encryptionKey = key
	}
}

func (o *options) tls() (*tls.Config, error) {
	if o.tlsConfig != nil {
		return o.tlsConfig, nil
	}
	if o.serverKey == nil && o.serverCert == nil {
		return nil, nil
	}
	if o.serverKey == nil {
		return nil, errors.New("missing server key")
	}
	if o.serverCert == nil {
		return nil, errors.New("missing server certificate")
	}
	cert, err := tls.X509KeyPair(o.serverCert, o.serverKey)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}
