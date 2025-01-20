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

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.29.3
// source: internal/badgerd/replication/gossip/pb/gossip.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ReplicationService_Init_FullMethodName      = "/linka.cloud.protodb.internal.replication.gossip.ReplicationService/Init"
	ReplicationService_Replicate_FullMethodName = "/linka.cloud.protodb.internal.replication.gossip.ReplicationService/Replicate"
	ReplicationService_Alive_FullMethodName     = "/linka.cloud.protodb.internal.replication.gossip.ReplicationService/Alive"
	ReplicationService_Election_FullMethodName  = "/linka.cloud.protodb.internal.replication.gossip.ReplicationService/Election"
)

// ReplicationServiceClient is the client API for ReplicationService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ReplicationServiceClient interface {
	Init(ctx context.Context, in *InitRequest, opts ...grpc.CallOption) (ReplicationService_InitClient, error)
	Replicate(ctx context.Context, opts ...grpc.CallOption) (ReplicationService_ReplicateClient, error)
	Alive(ctx context.Context, opts ...grpc.CallOption) (ReplicationService_AliveClient, error)
	Election(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Message, error)
}

type replicationServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewReplicationServiceClient(cc grpc.ClientConnInterface) ReplicationServiceClient {
	return &replicationServiceClient{cc}
}

func (c *replicationServiceClient) Init(ctx context.Context, in *InitRequest, opts ...grpc.CallOption) (ReplicationService_InitClient, error) {
	stream, err := c.cc.NewStream(ctx, &ReplicationService_ServiceDesc.Streams[0], ReplicationService_Init_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &replicationServiceInitClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ReplicationService_InitClient interface {
	Recv() (*InitResponse, error)
	grpc.ClientStream
}

type replicationServiceInitClient struct {
	grpc.ClientStream
}

func (x *replicationServiceInitClient) Recv() (*InitResponse, error) {
	m := new(InitResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *replicationServiceClient) Replicate(ctx context.Context, opts ...grpc.CallOption) (ReplicationService_ReplicateClient, error) {
	stream, err := c.cc.NewStream(ctx, &ReplicationService_ServiceDesc.Streams[1], ReplicationService_Replicate_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &replicationServiceReplicateClient{stream}
	return x, nil
}

type ReplicationService_ReplicateClient interface {
	Send(*Op) error
	Recv() (*Ack, error)
	grpc.ClientStream
}

type replicationServiceReplicateClient struct {
	grpc.ClientStream
}

func (x *replicationServiceReplicateClient) Send(m *Op) error {
	return x.ClientStream.SendMsg(m)
}

func (x *replicationServiceReplicateClient) Recv() (*Ack, error) {
	m := new(Ack)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *replicationServiceClient) Alive(ctx context.Context, opts ...grpc.CallOption) (ReplicationService_AliveClient, error) {
	stream, err := c.cc.NewStream(ctx, &ReplicationService_ServiceDesc.Streams[2], ReplicationService_Alive_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &replicationServiceAliveClient{stream}
	return x, nil
}

type ReplicationService_AliveClient interface {
	Send(*Ack) error
	Recv() (*Ack, error)
	grpc.ClientStream
}

type replicationServiceAliveClient struct {
	grpc.ClientStream
}

func (x *replicationServiceAliveClient) Send(m *Ack) error {
	return x.ClientStream.SendMsg(m)
}

func (x *replicationServiceAliveClient) Recv() (*Ack, error) {
	m := new(Ack)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *replicationServiceClient) Election(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Message, error) {
	out := new(Message)
	err := c.cc.Invoke(ctx, ReplicationService_Election_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReplicationServiceServer is the server API for ReplicationService service.
// All implementations must embed UnimplementedReplicationServiceServer
// for forward compatibility
type ReplicationServiceServer interface {
	Init(*InitRequest, ReplicationService_InitServer) error
	Replicate(ReplicationService_ReplicateServer) error
	Alive(ReplicationService_AliveServer) error
	Election(context.Context, *Message) (*Message, error)
	mustEmbedUnimplementedReplicationServiceServer()
}

// UnimplementedReplicationServiceServer must be embedded to have forward compatible implementations.
type UnimplementedReplicationServiceServer struct {
}

func (UnimplementedReplicationServiceServer) Init(*InitRequest, ReplicationService_InitServer) error {
	return status.Errorf(codes.Unimplemented, "method Init not implemented")
}
func (UnimplementedReplicationServiceServer) Replicate(ReplicationService_ReplicateServer) error {
	return status.Errorf(codes.Unimplemented, "method Replicate not implemented")
}
func (UnimplementedReplicationServiceServer) Alive(ReplicationService_AliveServer) error {
	return status.Errorf(codes.Unimplemented, "method Alive not implemented")
}
func (UnimplementedReplicationServiceServer) Election(context.Context, *Message) (*Message, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Election not implemented")
}
func (UnimplementedReplicationServiceServer) mustEmbedUnimplementedReplicationServiceServer() {}

// UnsafeReplicationServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ReplicationServiceServer will
// result in compilation errors.
type UnsafeReplicationServiceServer interface {
	mustEmbedUnimplementedReplicationServiceServer()
}

func RegisterReplicationServiceServer(s grpc.ServiceRegistrar, srv ReplicationServiceServer) {
	s.RegisterService(&ReplicationService_ServiceDesc, srv)
}

func _ReplicationService_Init_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(InitRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ReplicationServiceServer).Init(m, &replicationServiceInitServer{stream})
}

type ReplicationService_InitServer interface {
	Send(*InitResponse) error
	grpc.ServerStream
}

type replicationServiceInitServer struct {
	grpc.ServerStream
}

func (x *replicationServiceInitServer) Send(m *InitResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _ReplicationService_Replicate_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ReplicationServiceServer).Replicate(&replicationServiceReplicateServer{stream})
}

type ReplicationService_ReplicateServer interface {
	Send(*Ack) error
	Recv() (*Op, error)
	grpc.ServerStream
}

type replicationServiceReplicateServer struct {
	grpc.ServerStream
}

func (x *replicationServiceReplicateServer) Send(m *Ack) error {
	return x.ServerStream.SendMsg(m)
}

func (x *replicationServiceReplicateServer) Recv() (*Op, error) {
	m := new(Op)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _ReplicationService_Alive_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ReplicationServiceServer).Alive(&replicationServiceAliveServer{stream})
}

type ReplicationService_AliveServer interface {
	Send(*Ack) error
	Recv() (*Ack, error)
	grpc.ServerStream
}

type replicationServiceAliveServer struct {
	grpc.ServerStream
}

func (x *replicationServiceAliveServer) Send(m *Ack) error {
	return x.ServerStream.SendMsg(m)
}

func (x *replicationServiceAliveServer) Recv() (*Ack, error) {
	m := new(Ack)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _ReplicationService_Election_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ReplicationServiceServer).Election(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ReplicationService_Election_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ReplicationServiceServer).Election(ctx, req.(*Message))
	}
	return interceptor(ctx, in, info, handler)
}

// ReplicationService_ServiceDesc is the grpc.ServiceDesc for ReplicationService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ReplicationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "linka.cloud.protodb.internal.replication.gossip.ReplicationService",
	HandlerType: (*ReplicationServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Election",
			Handler:    _ReplicationService_Election_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Init",
			Handler:       _ReplicationService_Init_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "Replicate",
			Handler:       _ReplicationService_Replicate_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "Alive",
			Handler:       _ReplicationService_Alive_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "internal/badgerd/replication/gossip/pb/gossip.proto",
}
