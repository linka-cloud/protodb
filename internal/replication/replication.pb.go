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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.23.3
// source: internal/replication/replication.proto

package replication

import (
	_ "github.com/alta/protopatch/patch/gopb"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type InitRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Since uint64 `protobuf:"varint,1,opt,name=since,proto3" json:"since,omitempty"`
}

func (x *InitRequest) Reset() {
	*x = InitRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InitRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InitRequest) ProtoMessage() {}

func (x *InitRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InitRequest.ProtoReflect.Descriptor instead.
func (*InitRequest) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{0}
}

func (x *InitRequest) GetSince() uint64 {
	if x != nil {
		return x.Since
	}
	return 0
}

type InitResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Last uint64 `protobuf:"varint,1,opt,name=last,proto3" json:"last,omitempty"`
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *InitResponse) Reset() {
	*x = InitResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InitResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InitResponse) ProtoMessage() {}

func (x *InitResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InitResponse.ProtoReflect.Descriptor instead.
func (*InitResponse) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{1}
}

func (x *InitResponse) GetLast() uint64 {
	if x != nil {
		return x.Last
	}
	return 0
}

func (x *InitResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type Op struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Action:
	//
	//	*Op_New
	//	*Op_Set
	//	*Op_Delete
	//	*Op_Commit
	Action isOp_Action `protobuf_oneof:"action"`
}

func (x *Op) Reset() {
	*x = Op{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Op) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Op) ProtoMessage() {}

func (x *Op) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Op.ProtoReflect.Descriptor instead.
func (*Op) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{2}
}

func (m *Op) GetAction() isOp_Action {
	if m != nil {
		return m.Action
	}
	return nil
}

func (x *Op) GetNew() *New {
	if x, ok := x.GetAction().(*Op_New); ok {
		return x.New
	}
	return nil
}

func (x *Op) GetSet() *Set {
	if x, ok := x.GetAction().(*Op_Set); ok {
		return x.Set
	}
	return nil
}

func (x *Op) GetDelete() *Delete {
	if x, ok := x.GetAction().(*Op_Delete); ok {
		return x.Delete
	}
	return nil
}

func (x *Op) GetCommit() *Commit {
	if x, ok := x.GetAction().(*Op_Commit); ok {
		return x.Commit
	}
	return nil
}

type isOp_Action interface {
	isOp_Action()
}

type Op_New struct {
	New *New `protobuf:"bytes,1,opt,name=new,proto3,oneof"`
}

type Op_Set struct {
	Set *Set `protobuf:"bytes,2,opt,name=set,proto3,oneof"`
}

type Op_Delete struct {
	Delete *Delete `protobuf:"bytes,3,opt,name=delete,proto3,oneof"`
}

type Op_Commit struct {
	Commit *Commit `protobuf:"bytes,4,opt,name=commit,proto3,oneof"`
}

func (*Op_New) isOp_Action() {}

func (*Op_Set) isOp_Action() {}

func (*Op_Delete) isOp_Action() {}

func (*Op_Commit) isOp_Action() {}

type New struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	At uint64 `protobuf:"varint,1,opt,name=at,proto3" json:"at,omitempty"`
}

func (x *New) Reset() {
	*x = New{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *New) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*New) ProtoMessage() {}

func (x *New) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use New.ProtoReflect.Descriptor instead.
func (*New) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{3}
}

func (x *New) GetAt() uint64 {
	if x != nil {
		return x.At
	}
	return 0
}

type Set struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value     []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	ExpiresAt uint64 `protobuf:"varint,3,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at,omitempty"`
}

func (x *Set) Reset() {
	*x = Set{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Set) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Set) ProtoMessage() {}

func (x *Set) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Set.ProtoReflect.Descriptor instead.
func (*Set) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{4}
}

func (x *Set) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Set) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Set) GetExpiresAt() uint64 {
	if x != nil {
		return x.ExpiresAt
	}
	return 0
}

type Delete struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *Delete) Reset() {
	*x = Delete{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Delete) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Delete) ProtoMessage() {}

func (x *Delete) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Delete.ProtoReflect.Descriptor instead.
func (*Delete) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{5}
}

func (x *Delete) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type Commit struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	At uint64 `protobuf:"varint,1,opt,name=at,proto3" json:"at,omitempty"`
}

func (x *Commit) Reset() {
	*x = Commit{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Commit) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Commit) ProtoMessage() {}

func (x *Commit) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Commit.ProtoReflect.Descriptor instead.
func (*Commit) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{6}
}

func (x *Commit) GetAt() uint64 {
	if x != nil {
		return x.At
	}
	return 0
}

type Ack struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Ack) Reset() {
	*x = Ack{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Ack) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Ack) ProtoMessage() {}

func (x *Ack) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Ack.ProtoReflect.Descriptor instead.
func (*Ack) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{7}
}

type Meta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	GRPCPort uint32 `protobuf:"varint,1,opt,name=grpc_port,json=grpcPort,proto3" json:"grpc_port,omitempty"`
}

func (x *Meta) Reset() {
	*x = Meta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_replication_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Meta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Meta) ProtoMessage() {}

func (x *Meta) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_replication_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Meta.ProtoReflect.Descriptor instead.
func (*Meta) Descriptor() ([]byte, []int) {
	return file_internal_replication_replication_proto_rawDescGZIP(), []int{8}
}

func (x *Meta) GetGRPCPort() uint32 {
	if x != nil {
		return x.GRPCPort
	}
	return 0
}

var File_internal_replication_replication_proto protoreflect.FileDescriptor

var file_internal_replication_replication_proto_rawDesc = []byte{
	0x0a, 0x26, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x28, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e,
	0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e,
	0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x1a, 0x0e, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2f, 0x67, 0x6f, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x23, 0x0a, 0x0b, 0x49, 0x6e, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x69, 0x6e, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x05, 0x73, 0x69, 0x6e, 0x63, 0x65, 0x22, 0x36, 0x0a, 0x0c, 0x49, 0x6e, 0x69, 0x74, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6c, 0x61, 0x73, 0x74, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x6c, 0x61, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22,
	0xac, 0x02, 0x0a, 0x02, 0x4f, 0x70, 0x12, 0x41, 0x0a, 0x03, 0x6e, 0x65, 0x77, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75,
	0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e,
	0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x4e,
	0x65, 0x77, 0x48, 0x00, 0x52, 0x03, 0x6e, 0x65, 0x77, 0x12, 0x41, 0x0a, 0x03, 0x73, 0x65, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63,
	0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74,
	0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x53, 0x65, 0x74, 0x48, 0x00, 0x52, 0x03, 0x73, 0x65, 0x74, 0x12, 0x4a, 0x0a, 0x06,
	0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x30, 0x2e, 0x6c,
	0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c,
	0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x48, 0x00,
	0x52, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x4a, 0x0a, 0x06, 0x63, 0x6f, 0x6d, 0x6d,
	0x69, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x30, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61,
	0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x48, 0x00, 0x52, 0x06, 0x63, 0x6f,
	0x6d, 0x6d, 0x69, 0x74, 0x42, 0x08, 0x0a, 0x06, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x15,
	0x0a, 0x03, 0x4e, 0x65, 0x77, 0x12, 0x0e, 0x0a, 0x02, 0x61, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x02, 0x61, 0x74, 0x22, 0x4c, 0x0a, 0x03, 0x53, 0x65, 0x74, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x73, 0x5f,
	0x61, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65,
	0x73, 0x41, 0x74, 0x22, 0x1a, 0x0a, 0x06, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x10, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22,
	0x18, 0x0a, 0x06, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x61, 0x74, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x02, 0x61, 0x74, 0x22, 0x05, 0x0a, 0x03, 0x41, 0x63, 0x6b,
	0x22, 0x33, 0x0a, 0x04, 0x4d, 0x65, 0x74, 0x61, 0x12, 0x2b, 0x0a, 0x09, 0x67, 0x72, 0x70, 0x63,
	0x5f, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x42, 0x0e, 0xca, 0xb5, 0x03,
	0x0a, 0x0a, 0x08, 0x47, 0x52, 0x50, 0x43, 0x50, 0x6f, 0x72, 0x74, 0x52, 0x08, 0x67, 0x72, 0x70,
	0x63, 0x50, 0x6f, 0x72, 0x74, 0x32, 0xff, 0x01, 0x0a, 0x12, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x79, 0x0a, 0x04,
	0x49, 0x6e, 0x69, 0x74, 0x12, 0x35, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f,
	0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72,
	0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x49, 0x6e, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x36, 0x2e, 0x6c, 0x69,
	0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64,
	0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x49, 0x6e, 0x69, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x12, 0x6e, 0x0a, 0x09, 0x52, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x65, 0x12, 0x2c, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f,
	0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72,
	0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x4f, 0x70, 0x1a, 0x2d, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61,
	0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x41, 0x63,
	0x6b, 0x22, 0x00, 0x28, 0x01, 0x30, 0x01, 0x42, 0x3f, 0xca, 0xb5, 0x03, 0x02, 0x08, 0x01, 0x5a,
	0x37, 0x67, 0x6f, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c,
	0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x3b, 0x72, 0x65, 0x70,
	0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_replication_replication_proto_rawDescOnce sync.Once
	file_internal_replication_replication_proto_rawDescData = file_internal_replication_replication_proto_rawDesc
)

func file_internal_replication_replication_proto_rawDescGZIP() []byte {
	file_internal_replication_replication_proto_rawDescOnce.Do(func() {
		file_internal_replication_replication_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_replication_replication_proto_rawDescData)
	})
	return file_internal_replication_replication_proto_rawDescData
}

var file_internal_replication_replication_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_internal_replication_replication_proto_goTypes = []interface{}{
	(*InitRequest)(nil),  // 0: linka.cloud.protodb.internal.replication.InitRequest
	(*InitResponse)(nil), // 1: linka.cloud.protodb.internal.replication.InitResponse
	(*Op)(nil),           // 2: linka.cloud.protodb.internal.replication.Op
	(*New)(nil),          // 3: linka.cloud.protodb.internal.replication.New
	(*Set)(nil),          // 4: linka.cloud.protodb.internal.replication.Set
	(*Delete)(nil),       // 5: linka.cloud.protodb.internal.replication.Delete
	(*Commit)(nil),       // 6: linka.cloud.protodb.internal.replication.Commit
	(*Ack)(nil),          // 7: linka.cloud.protodb.internal.replication.Ack
	(*Meta)(nil),         // 8: linka.cloud.protodb.internal.replication.Meta
}
var file_internal_replication_replication_proto_depIdxs = []int32{
	3, // 0: linka.cloud.protodb.internal.replication.Op.new:type_name -> linka.cloud.protodb.internal.replication.New
	4, // 1: linka.cloud.protodb.internal.replication.Op.set:type_name -> linka.cloud.protodb.internal.replication.Set
	5, // 2: linka.cloud.protodb.internal.replication.Op.delete:type_name -> linka.cloud.protodb.internal.replication.Delete
	6, // 3: linka.cloud.protodb.internal.replication.Op.commit:type_name -> linka.cloud.protodb.internal.replication.Commit
	0, // 4: linka.cloud.protodb.internal.replication.ReplicationService.Init:input_type -> linka.cloud.protodb.internal.replication.InitRequest
	2, // 5: linka.cloud.protodb.internal.replication.ReplicationService.Replicate:input_type -> linka.cloud.protodb.internal.replication.Op
	1, // 6: linka.cloud.protodb.internal.replication.ReplicationService.Init:output_type -> linka.cloud.protodb.internal.replication.InitResponse
	7, // 7: linka.cloud.protodb.internal.replication.ReplicationService.Replicate:output_type -> linka.cloud.protodb.internal.replication.Ack
	6, // [6:8] is the sub-list for method output_type
	4, // [4:6] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_internal_replication_replication_proto_init() }
func file_internal_replication_replication_proto_init() {
	if File_internal_replication_replication_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_replication_replication_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InitRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InitResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Op); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*New); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Set); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Delete); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Commit); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Ack); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_replication_replication_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Meta); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_internal_replication_replication_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*Op_New)(nil),
		(*Op_Set)(nil),
		(*Op_Delete)(nil),
		(*Op_Commit)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_replication_replication_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_internal_replication_replication_proto_goTypes,
		DependencyIndexes: file_internal_replication_replication_proto_depIdxs,
		MessageInfos:      file_internal_replication_replication_proto_msgTypes,
	}.Build()
	File_internal_replication_replication_proto = out.File
	file_internal_replication_replication_proto_rawDesc = nil
	file_internal_replication_replication_proto_goTypes = nil
	file_internal_replication_replication_proto_depIdxs = nil
}
