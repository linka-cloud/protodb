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
	"crypto/x509"
	"errors"
	"time"

	"github.com/shaj13/raft"
)

type Option func(o *Options)

type Options struct {
	Mode     Mode
	Name     string
	Addrs    []string
	GRPCPort int
	Tick     time.Duration

	GossipPort    int
	EncryptionKey string

	StartOptions []raft.StartOption

	serverCert []byte
	serverKey  []byte

	clientCert []byte
	clientKey  []byte
	clientCA   []byte

	tlsConfig *tls.Config
}

func WithMode(mode Mode) Option {
	return func(o *Options) {
		o.Mode = mode
	}
}

func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

func WithAddrs(addrs ...string) Option {
	return func(o *Options) {
		o.Addrs = addrs
	}
}

func WithGossipPort(port int) Option {
	return func(o *Options) {
		o.GossipPort = port
	}
}

func WithGRPCPort(port int) Option {
	return func(o *Options) {
		o.GRPCPort = port
	}
}

func WithTick(ms int) Option {
	return func(o *Options) {
		if ms > 100 {
			o.Tick = time.Duration(ms) * time.Millisecond
		}
	}
}

func WithRaftStartOptions(opts ...raft.StartOption) Option {
	return func(o *Options) {
		o.StartOptions = append(o.StartOptions, opts...)
	}
}

func WithServerCert(cert []byte) Option {
	return func(o *Options) {
		o.serverCert = cert
	}
}

func WithServerKey(key []byte) Option {
	return func(o *Options) {
		o.serverKey = key
	}
}

func WithClientCert(cert []byte) Option {
	return func(o *Options) {
		o.clientCert = cert
	}
}

func WithClientKey(key []byte) Option {
	return func(o *Options) {
		o.clientKey = key
	}
}

func WithClientCA(ca []byte) Option {
	return func(o *Options) {
		o.clientCA = ca
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *Options) {
		o.tlsConfig = config
	}
}

func WithEncryptionKey(key string) Option {
	return func(o *Options) {
		o.EncryptionKey = key
	}
}

func (o *Options) TLS() (*tls.Config, error) {
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
	var (
		clientCA   *x509.CertPool
		clientCert tls.Certificate
		auth       tls.ClientAuthType
	)
	if o.clientCA != nil {
		clientCA = x509.NewCertPool()
		clientCA.AppendCertsFromPEM(o.clientCA)
		auth = tls.RequireAndVerifyClientCert
	}
	if o.clientCert != nil && o.clientKey != nil {
		clientCert, err = tls.X509KeyPair(o.clientCert, o.clientKey)
		if err != nil {
			return nil, err
		}
	}
	// TODO(adphi): client certs
	return &tls.Config{
		Certificates: []tls.Certificate{cert, clientCert},
		ClientCAs:    clientCA,
		ClientAuth:   auth,
	}, nil
}
