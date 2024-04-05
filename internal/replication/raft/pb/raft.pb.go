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
// 	protoc-gen-go v1.33.0
// 	protoc        v4.25.3
// source: internal/replication/raft/pb/raft.proto

package pb

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
		mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Set) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Set) ProtoMessage() {}

func (x *Set) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[0]
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
	return file_internal_replication_raft_pb_raft_proto_rawDescGZIP(), []int{0}
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
		mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Delete) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Delete) ProtoMessage() {}

func (x *Delete) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[1]
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
	return file_internal_replication_raft_pb_raft_proto_rawDescGZIP(), []int{1}
}

func (x *Delete) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Cmd:
	//
	//	*Entry_Chunk
	Cmd isEntry_Cmd `protobuf_oneof:"cmd"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_internal_replication_raft_pb_raft_proto_rawDescGZIP(), []int{2}
}

func (m *Entry) GetCmd() isEntry_Cmd {
	if m != nil {
		return m.Cmd
	}
	return nil
}

func (x *Entry) GetChunk() *Chunk {
	if x, ok := x.GetCmd().(*Entry_Chunk); ok {
		return x.Chunk
	}
	return nil
}

type isEntry_Cmd interface {
	isEntry_Cmd()
}

type Entry_Chunk struct {
	Chunk *Chunk `protobuf:"bytes,1,opt,name=chunk,proto3,oneof"`
}

func (*Entry_Chunk) isEntry_Cmd() {}

type Tx struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ReadAt   uint64    `protobuf:"varint,1,opt,name=readAt,proto3" json:"readAt,omitempty"`
	CommitAt uint64    `protobuf:"varint,2,opt,name=commitAt,proto3" json:"commitAt,omitempty"`
	Sets     []*Set    `protobuf:"bytes,3,rep,name=sets,proto3" json:"sets,omitempty"`
	Deletes  []*Delete `protobuf:"bytes,4,rep,name=deletes,proto3" json:"deletes,omitempty"`
}

func (x *Tx) Reset() {
	*x = Tx{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Tx) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tx) ProtoMessage() {}

func (x *Tx) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tx.ProtoReflect.Descriptor instead.
func (*Tx) Descriptor() ([]byte, []int) {
	return file_internal_replication_raft_pb_raft_proto_rawDescGZIP(), []int{3}
}

func (x *Tx) GetReadAt() uint64 {
	if x != nil {
		return x.ReadAt
	}
	return 0
}

func (x *Tx) GetCommitAt() uint64 {
	if x != nil {
		return x.CommitAt
	}
	return 0
}

func (x *Tx) GetSets() []*Set {
	if x != nil {
		return x.Sets
	}
	return nil
}

func (x *Tx) GetDeletes() []*Delete {
	if x != nil {
		return x.Deletes
	}
	return nil
}

type Chunk struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TxID    string `protobuf:"bytes,1,opt,name=tx_id,json=txId,proto3" json:"tx_id,omitempty"`
	Offset  uint64 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	Total   uint64 `protobuf:"varint,3,opt,name=total,proto3" json:"total,omitempty"`
	Data    []byte `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
	Discard bool   `protobuf:"varint,5,opt,name=discard,proto3" json:"discard,omitempty"`
}

func (x *Chunk) Reset() {
	*x = Chunk{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Chunk) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Chunk) ProtoMessage() {}

func (x *Chunk) ProtoReflect() protoreflect.Message {
	mi := &file_internal_replication_raft_pb_raft_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Chunk.ProtoReflect.Descriptor instead.
func (*Chunk) Descriptor() ([]byte, []int) {
	return file_internal_replication_raft_pb_raft_proto_rawDescGZIP(), []int{4}
}

func (x *Chunk) GetTxID() string {
	if x != nil {
		return x.TxID
	}
	return ""
}

func (x *Chunk) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *Chunk) GetTotal() uint64 {
	if x != nil {
		return x.Total
	}
	return 0
}

func (x *Chunk) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *Chunk) GetDiscard() bool {
	if x != nil {
		return x.Discard
	}
	return false
}

var File_internal_replication_raft_pb_raft_proto protoreflect.FileDescriptor

var file_internal_replication_raft_pb_raft_proto_rawDesc = []byte{
	0x0a, 0x27, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x72, 0x61, 0x66, 0x74, 0x2f, 0x70, 0x62, 0x2f, 0x72,
	0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x2d, 0x6c, 0x69, 0x6e, 0x6b, 0x61,
	0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x1a, 0x0e, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2f,
	0x67, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x4c, 0x0a, 0x03, 0x53, 0x65, 0x74, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72,
	0x65, 0x73, 0x5f, 0x61, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x65, 0x78, 0x70,
	0x69, 0x72, 0x65, 0x73, 0x41, 0x74, 0x22, 0x1a, 0x0a, 0x06, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x22, 0x5c, 0x0a, 0x05, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x4c, 0x0a, 0x05, 0x63,
	0x68, 0x75, 0x6e, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x34, 0x2e, 0x6c, 0x69, 0x6e,
	0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62,
	0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x43, 0x68, 0x75, 0x6e, 0x6b,
	0x48, 0x00, 0x52, 0x05, 0x63, 0x68, 0x75, 0x6e, 0x6b, 0x42, 0x05, 0x0a, 0x03, 0x63, 0x6d, 0x64,
	0x22, 0xd1, 0x01, 0x0a, 0x02, 0x54, 0x78, 0x12, 0x16, 0x0a, 0x06, 0x72, 0x65, 0x61, 0x64, 0x41,
	0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06, 0x72, 0x65, 0x61, 0x64, 0x41, 0x74, 0x12,
	0x1a, 0x0a, 0x08, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x41, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x08, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x41, 0x74, 0x12, 0x46, 0x0a, 0x04, 0x73,
	0x65, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x32, 0x2e, 0x6c, 0x69, 0x6e, 0x6b,
	0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e,
	0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x53, 0x65, 0x74, 0x52, 0x04, 0x73,
	0x65, 0x74, 0x73, 0x12, 0x4f, 0x0a, 0x07, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x73, 0x18, 0x04,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x35, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f,
	0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x72,
	0x6e, 0x61, 0x6c, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x72, 0x61, 0x66, 0x74, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x07, 0x64, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x73, 0x22, 0x78, 0x0a, 0x05, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x12, 0x13, 0x0a,
	0x05, 0x74, 0x78, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x78,
	0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f,
	0x74, 0x61, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x74, 0x6f, 0x74, 0x61, 0x6c,
	0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04,
	0x64, 0x61, 0x74, 0x61, 0x12, 0x18, 0x0a, 0x07, 0x64, 0x69, 0x73, 0x63, 0x61, 0x72, 0x64, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x64, 0x69, 0x73, 0x63, 0x61, 0x72, 0x64, 0x42, 0x3e,
	0xca, 0xb5, 0x03, 0x02, 0x08, 0x01, 0x5a, 0x36, 0x67, 0x6f, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61,
	0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2f, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2f, 0x72, 0x61, 0x66, 0x74, 0x2f, 0x70, 0x62, 0x3b, 0x70, 0x62, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_replication_raft_pb_raft_proto_rawDescOnce sync.Once
	file_internal_replication_raft_pb_raft_proto_rawDescData = file_internal_replication_raft_pb_raft_proto_rawDesc
)

func file_internal_replication_raft_pb_raft_proto_rawDescGZIP() []byte {
	file_internal_replication_raft_pb_raft_proto_rawDescOnce.Do(func() {
		file_internal_replication_raft_pb_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_replication_raft_pb_raft_proto_rawDescData)
	})
	return file_internal_replication_raft_pb_raft_proto_rawDescData
}

var file_internal_replication_raft_pb_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_internal_replication_raft_pb_raft_proto_goTypes = []interface{}{
	(*Set)(nil),    // 0: linka.cloud.protodb.internal.replication.raft.Set
	(*Delete)(nil), // 1: linka.cloud.protodb.internal.replication.raft.Delete
	(*Entry)(nil),  // 2: linka.cloud.protodb.internal.replication.raft.Entry
	(*Tx)(nil),     // 3: linka.cloud.protodb.internal.replication.raft.Tx
	(*Chunk)(nil),  // 4: linka.cloud.protodb.internal.replication.raft.Chunk
}
var file_internal_replication_raft_pb_raft_proto_depIdxs = []int32{
	4, // 0: linka.cloud.protodb.internal.replication.raft.Entry.chunk:type_name -> linka.cloud.protodb.internal.replication.raft.Chunk
	0, // 1: linka.cloud.protodb.internal.replication.raft.Tx.sets:type_name -> linka.cloud.protodb.internal.replication.raft.Set
	1, // 2: linka.cloud.protodb.internal.replication.raft.Tx.deletes:type_name -> linka.cloud.protodb.internal.replication.raft.Delete
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_internal_replication_raft_pb_raft_proto_init() }
func file_internal_replication_raft_pb_raft_proto_init() {
	if File_internal_replication_raft_pb_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_replication_raft_pb_raft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
		file_internal_replication_raft_pb_raft_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
		file_internal_replication_raft_pb_raft_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entry); i {
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
		file_internal_replication_raft_pb_raft_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Tx); i {
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
		file_internal_replication_raft_pb_raft_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Chunk); i {
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
	file_internal_replication_raft_pb_raft_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*Entry_Chunk)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_replication_raft_pb_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_replication_raft_pb_raft_proto_goTypes,
		DependencyIndexes: file_internal_replication_raft_pb_raft_proto_depIdxs,
		MessageInfos:      file_internal_replication_raft_pb_raft_proto_msgTypes,
	}.Build()
	File_internal_replication_raft_pb_raft_proto = out.File
	file_internal_replication_raft_pb_raft_proto_rawDesc = nil
	file_internal_replication_raft_pb_raft_proto_goTypes = nil
	file_internal_replication_raft_pb_raft_proto_depIdxs = nil
}
