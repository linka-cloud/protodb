// Copyright 2021 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-defaults. DO NOT EDIT.

package pb

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
)

var (
	_ = errors.New("")
	_ = io.EOF
	_ = context.Background()
)

var _ ReplicationServiceServer = (*proxyReplicationService)(nil)

func NewReplicationServiceProxy(c ReplicationServiceClient, opts ...grpc.CallOption) ReplicationServiceServer {
	return &proxyReplicationService{c: c, opts: opts}
}

type proxyReplicationService struct {
	c    ReplicationServiceClient
	opts []grpc.CallOption
}

// Init proxies call to backend server
func (x *proxyReplicationService) Init(req *InitRequest, s ReplicationService_InitServer) error {
	cs, err := x.c.Init(s.Context(), req, x.opts...)
	if err != nil {
		return err
	}
	for {
		res, err := cs.Recv()
		if err != nil {
			return err
		}
		if err := s.Send(res); err != nil {
			return err
		}
	}
}

// Replicate proxies call to backend server
func (x *proxyReplicationService) Replicate(s ReplicationService_ReplicateServer) error {
	cs, err := x.c.Replicate(s.Context(), x.opts...)
	if err != nil {
		return err
	}
	errs := make(chan error, 2)
	recv := func() error {
		for {
			req, err := s.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := cs.Send(req); err != nil {
				return err
			}
		}
	}
	send := func() error {
		for {
			res, err := cs.Recv()
			if err != nil {
				return err
			}
			if err := s.Send(res); err != nil {
				return err
			}
		}
	}
	go func() {
		errs <- recv()
	}()
	go func() {
		errs <- send()
	}()
	return <-errs
}

// Alive proxies call to backend server
func (x *proxyReplicationService) Alive(s ReplicationService_AliveServer) error {
	cs, err := x.c.Alive(s.Context(), x.opts...)
	if err != nil {
		return err
	}
	errs := make(chan error, 2)
	recv := func() error {
		for {
			req, err := s.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := cs.Send(req); err != nil {
				return err
			}
		}
	}
	send := func() error {
		for {
			res, err := cs.Recv()
			if err != nil {
				return err
			}
			if err := s.Send(res); err != nil {
				return err
			}
		}
	}
	go func() {
		errs <- recv()
	}()
	go func() {
		errs <- send()
	}()
	return <-errs
}

// Election proxies call to backend server
func (x *proxyReplicationService) Election(ctx context.Context, req *Message) (*Message, error) {
	return x.c.Election(ctx, req, x.opts...)
}

func (x *proxyReplicationService) mustEmbedUnimplementedReplicationServiceServer() {}
