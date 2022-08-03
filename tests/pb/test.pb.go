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
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.4
// source: tests/pb/test.proto

package testpb

import (
	reflect "reflect"
	sync "sync"

	_ "github.com/alta/protopatch/patch/gopb"
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	_ "go.linka.cloud/protoc-gen-defaults/defaults"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"

	_ "go.linka.cloud/protodb/protodb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type InterfaceStatus int32

const (
	StatusUnknown InterfaceStatus = 0
	StatusUp      InterfaceStatus = 1
	StatusDown    InterfaceStatus = 2
)

// Enum value maps for Interface_Status.
var (
	InterfaceStatus_name = map[int32]string{
		0: "UNKNOWN",
		1: "UP",
		2: "DOWN",
	}
	InterfaceStatus_value = map[string]int32{
		"UNKNOWN": 0,
		"UP":      1,
		"DOWN":    2,
	}
)

func (x InterfaceStatus) Enum() *InterfaceStatus {
	p := new(InterfaceStatus)
	*p = x
	return p
}

func (x InterfaceStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (InterfaceStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_tests_pb_test_proto_enumTypes[0].Descriptor()
}

func (InterfaceStatus) Type() protoreflect.EnumType {
	return &file_tests_pb_test_proto_enumTypes[0]
}

func (x InterfaceStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Interface_Status.Descriptor instead.
func (InterfaceStatus) EnumDescriptor() ([]byte, []int) {
	return file_tests_pb_test_proto_rawDescGZIP(), []int{2, 0}
}

type MessageWithKeyOption struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	KeyField uint32 `protobuf:"varint,1,opt,name=key_field,json=keyField,proto3" json:"key_field,omitempty"`
}

func (x *MessageWithKeyOption) Reset() {
	*x = MessageWithKeyOption{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tests_pb_test_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MessageWithKeyOption) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MessageWithKeyOption) ProtoMessage() {}

func (x *MessageWithKeyOption) ProtoReflect() protoreflect.Message {
	mi := &file_tests_pb_test_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MessageWithKeyOption.ProtoReflect.Descriptor instead.
func (*MessageWithKeyOption) Descriptor() ([]byte, []int) {
	return file_tests_pb_test_proto_rawDescGZIP(), []int{0}
}

func (x *MessageWithKeyOption) GetKeyField() uint32 {
	if x != nil {
		return x.KeyField
	}
	return 0
}

type MessageWithStaticKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *MessageWithStaticKey) Reset() {
	*x = MessageWithStaticKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tests_pb_test_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MessageWithStaticKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MessageWithStaticKey) ProtoMessage() {}

func (x *MessageWithStaticKey) ProtoReflect() protoreflect.Message {
	mi := &file_tests_pb_test_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MessageWithStaticKey.ProtoReflect.Descriptor instead.
func (*MessageWithStaticKey) Descriptor() ([]byte, []int) {
	return file_tests_pb_test_proto_rawDescGZIP(), []int{1}
}

func (x *MessageWithStaticKey) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

type Interface struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name      string                  `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Mac       *wrapperspb.StringValue `protobuf:"bytes,2,opt,name=mac,proto3" json:"mac,omitempty"`
	Status    InterfaceStatus         `protobuf:"varint,3,opt,name=status,proto3,enum=linka.cloud.protodbtestpb.Interface_Status" json:"status,omitempty"`
	Addresses []*IPAddress            `protobuf:"bytes,4,rep,name=addresses,proto3" json:"addresses,omitempty"`
	Mtu       uint32                  `protobuf:"varint,5,opt,name=mtu,proto3" json:"mtu,omitempty"`
}

func (x *Interface) Reset() {
	*x = Interface{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tests_pb_test_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Interface) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Interface) ProtoMessage() {}

func (x *Interface) ProtoReflect() protoreflect.Message {
	mi := &file_tests_pb_test_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Interface.ProtoReflect.Descriptor instead.
func (*Interface) Descriptor() ([]byte, []int) {
	return file_tests_pb_test_proto_rawDescGZIP(), []int{2}
}

func (x *Interface) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Interface) GetMac() *wrapperspb.StringValue {
	if x != nil {
		return x.Mac
	}
	return nil
}

func (x *Interface) GetStatus() InterfaceStatus {
	if x != nil {
		return x.Status
	}
	return StatusUnknown
}

func (x *Interface) GetAddresses() []*IPAddress {
	if x != nil {
		return x.Addresses
	}
	return nil
}

func (x *Interface) GetMtu() uint32 {
	if x != nil {
		return x.Mtu
	}
	return 0
}

type IPAddress struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Address:
	//	*IPAddress_Ipv4
	//	*IPAddress_Ipv6
	Address isIPAddress_Address `protobuf_oneof:"Address"`
}

func (x *IPAddress) Reset() {
	*x = IPAddress{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tests_pb_test_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IPAddress) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IPAddress) ProtoMessage() {}

func (x *IPAddress) ProtoReflect() protoreflect.Message {
	mi := &file_tests_pb_test_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IPAddress.ProtoReflect.Descriptor instead.
func (*IPAddress) Descriptor() ([]byte, []int) {
	return file_tests_pb_test_proto_rawDescGZIP(), []int{3}
}

func (m *IPAddress) GetAddress() isIPAddress_Address {
	if m != nil {
		return m.Address
	}
	return nil
}

func (x *IPAddress) GetIPV4() string {
	if x, ok := x.GetAddress().(*IPAddress_IPV4); ok {
		return x.IPV4
	}
	return ""
}

func (x *IPAddress) GetIPV6() string {
	if x, ok := x.GetAddress().(*IPAddress_IPV6); ok {
		return x.IPV6
	}
	return ""
}

type isIPAddress_Address interface {
	isIPAddress_Address()
}

type IPAddress_IPV4 struct {
	IPV4 string `protobuf:"bytes,3,opt,name=ipv4,proto3,oneof"`
}

type IPAddress_IPV6 struct {
	IPV6 string `protobuf:"bytes,4,opt,name=ipv6,proto3,oneof"`
}

func (*IPAddress_IPV4) isIPAddress_Address() {}

func (*IPAddress_IPV6) isIPAddress_Address() {}

type KV struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *KV) Reset() {
	*x = KV{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tests_pb_test_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KV) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KV) ProtoMessage() {}

func (x *KV) ProtoReflect() protoreflect.Message {
	mi := &file_tests_pb_test_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KV.ProtoReflect.Descriptor instead.
func (*KV) Descriptor() ([]byte, []int) {
	return file_tests_pb_test_proto_rawDescGZIP(), []int{4}
}

func (x *KV) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KV) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

var File_tests_pb_test_proto protoreflect.FileDescriptor

var file_tests_pb_test_proto_rawDesc = []byte{
	0x0a, 0x13, 0x74, 0x65, 0x73, 0x74, 0x73, 0x2f, 0x70, 0x62, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x19, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f,
	0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x74, 0x65, 0x73, 0x74, 0x70, 0x62,
	0x1a, 0x17, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x73, 0x2f, 0x64, 0x65, 0x66, 0x61, 0x75,
	0x6c, 0x74, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0e, 0x70, 0x61, 0x74, 0x63, 0x68,
	0x2f, 0x67, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x17, 0x76, 0x61, 0x6c, 0x69, 0x64,
	0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x72, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x15, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x64, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x3d, 0x0a, 0x14, 0x4d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x57, 0x69, 0x74, 0x68, 0x4b, 0x65, 0x79, 0x4f, 0x70, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x20, 0x0a, 0x09, 0x6b, 0x65, 0x79, 0x5f, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0d, 0x42, 0x03, 0xb8, 0x4a, 0x01, 0x52, 0x08, 0x6b, 0x65, 0x79, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x3a, 0x03, 0xe8, 0x49, 0x01, 0x22, 0x39, 0x0a, 0x14, 0x4d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x57, 0x69, 0x74, 0x68, 0x53, 0x74, 0x61, 0x74, 0x69, 0x63, 0x4b, 0x65, 0x79,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x3a, 0x0d, 0xf2, 0x49, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x5f,
	0x6b, 0x65, 0x79, 0x22, 0xaa, 0x03, 0x0a, 0x09, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x66, 0x61, 0x63,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x5a, 0x0a, 0x03, 0x6d, 0x61, 0x63, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x42, 0x2a, 0xfa, 0x42, 0x27, 0x72, 0x25, 0x32, 0x23, 0x5e, 0x28, 0x5b, 0x30, 0x2d, 0x39, 0x41,
	0x2d, 0x46, 0x5d, 0x7b, 0x32, 0x7d, 0x5b, 0x3a, 0x2d, 0x5d, 0x29, 0x7b, 0x35, 0x7d, 0x28, 0x5b,
	0x30, 0x2d, 0x39, 0x41, 0x2d, 0x46, 0x5d, 0x7b, 0x32, 0x7d, 0x29, 0x24, 0x52, 0x03, 0x6d, 0x61,
	0x63, 0x12, 0x53, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x2b, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x74, 0x65, 0x73, 0x74, 0x70, 0x62, 0x2e, 0x49, 0x6e,
	0x74, 0x65, 0x72, 0x66, 0x61, 0x63, 0x65, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x42, 0x0e,
	0xfa, 0x42, 0x05, 0x82, 0x01, 0x02, 0x10, 0x01, 0x9a, 0x49, 0x03, 0x80, 0x01, 0x01, 0x52, 0x06,
	0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x42, 0x0a, 0x09, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73,
	0x73, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x24, 0x2e, 0x6c, 0x69, 0x6e, 0x6b,
	0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x74,
	0x65, 0x73, 0x74, 0x70, 0x62, 0x2e, 0x49, 0x50, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x52,
	0x09, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x65, 0x73, 0x12, 0x18, 0x0a, 0x03, 0x6d, 0x74,
	0x75, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0d, 0x42, 0x06, 0x9a, 0x49, 0x03, 0x28, 0xdc, 0x0b, 0x52,
	0x03, 0x6d, 0x74, 0x75, 0x22, 0x75, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x20,
	0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x1a, 0x13, 0xca, 0xb5, 0x03,
	0x0f, 0x0a, 0x0d, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e,
	0x12, 0x16, 0x0a, 0x02, 0x55, 0x50, 0x10, 0x01, 0x1a, 0x0e, 0xca, 0xb5, 0x03, 0x0a, 0x0a, 0x08,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x55, 0x70, 0x12, 0x1a, 0x0a, 0x04, 0x44, 0x4f, 0x57, 0x4e,
	0x10, 0x02, 0x1a, 0x10, 0xca, 0xb5, 0x03, 0x0c, 0x0a, 0x0a, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x44, 0x6f, 0x77, 0x6e, 0x1a, 0x15, 0xca, 0xb5, 0x03, 0x11, 0x0a, 0x0f, 0x49, 0x6e, 0x74, 0x65,
	0x72, 0x66, 0x61, 0x63, 0x65, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x3a, 0x03, 0xe8, 0x49, 0x01,
	0x22, 0x69, 0x0a, 0x09, 0x49, 0x50, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x27, 0x0a,
	0x04, 0x69, 0x70, 0x76, 0x34, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x42, 0x11, 0xca, 0xb5, 0x03,
	0x06, 0x0a, 0x04, 0x49, 0x50, 0x56, 0x34, 0xfa, 0x42, 0x04, 0x72, 0x02, 0x78, 0x01, 0x48, 0x00,
	0x52, 0x04, 0x69, 0x70, 0x76, 0x34, 0x12, 0x28, 0x0a, 0x04, 0x69, 0x70, 0x76, 0x36, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x09, 0x42, 0x12, 0xca, 0xb5, 0x03, 0x06, 0x0a, 0x04, 0x49, 0x50, 0x56, 0x36,
	0xfa, 0x42, 0x05, 0x72, 0x03, 0x80, 0x01, 0x01, 0x48, 0x00, 0x52, 0x04, 0x69, 0x70, 0x76, 0x36,
	0x42, 0x09, 0x0a, 0x07, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0x2c, 0x0a, 0x02, 0x4b,
	0x56, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x2e, 0x5a, 0x26, 0x67, 0x6f, 0x2e,
	0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x64, 0x62, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x73, 0x2f, 0x70, 0x62, 0x3b, 0x74, 0x65, 0x73,
	0x74, 0x70, 0x62, 0xca, 0xb5, 0x03, 0x02, 0x08, 0x01, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_tests_pb_test_proto_rawDescOnce sync.Once
	file_tests_pb_test_proto_rawDescData = file_tests_pb_test_proto_rawDesc
)

func file_tests_pb_test_proto_rawDescGZIP() []byte {
	file_tests_pb_test_proto_rawDescOnce.Do(func() {
		file_tests_pb_test_proto_rawDescData = protoimpl.X.CompressGZIP(file_tests_pb_test_proto_rawDescData)
	})
	return file_tests_pb_test_proto_rawDescData
}

var file_tests_pb_test_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_tests_pb_test_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_tests_pb_test_proto_goTypes = []interface{}{
	(InterfaceStatus)(0),           // 0: linka.cloud.protodbtestpb.Interface.Status
	(*MessageWithKeyOption)(nil),   // 1: linka.cloud.protodbtestpb.MessageWithKeyOption
	(*MessageWithStaticKey)(nil),   // 2: linka.cloud.protodbtestpb.MessageWithStaticKey
	(*Interface)(nil),              // 3: linka.cloud.protodbtestpb.Interface
	(*IPAddress)(nil),              // 4: linka.cloud.protodbtestpb.IPAddress
	(*KV)(nil),                     // 5: linka.cloud.protodbtestpb.KV
	(*wrapperspb.StringValue)(nil), // 6: google.protobuf.StringValue
}
var file_tests_pb_test_proto_depIdxs = []int32{
	6, // 0: linka.cloud.protodbtestpb.Interface.mac:type_name -> google.protobuf.StringValue
	0, // 1: linka.cloud.protodbtestpb.Interface.status:type_name -> linka.cloud.protodbtestpb.Interface.Status
	4, // 2: linka.cloud.protodbtestpb.Interface.addresses:type_name -> linka.cloud.protodbtestpb.IPAddress
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_tests_pb_test_proto_init() }
func file_tests_pb_test_proto_init() {
	if File_tests_pb_test_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_tests_pb_test_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MessageWithKeyOption); i {
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
		file_tests_pb_test_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MessageWithStaticKey); i {
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
		file_tests_pb_test_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Interface); i {
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
		file_tests_pb_test_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IPAddress); i {
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
		file_tests_pb_test_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KV); i {
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
	file_tests_pb_test_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*IPAddress_IPV4)(nil),
		(*IPAddress_IPV6)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_tests_pb_test_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_tests_pb_test_proto_goTypes,
		DependencyIndexes: file_tests_pb_test_proto_depIdxs,
		EnumInfos:         file_tests_pb_test_proto_enumTypes,
		MessageInfos:      file_tests_pb_test_proto_msgTypes,
	}.Build()
	File_tests_pb_test_proto = out.File
	file_tests_pb_test_proto_rawDesc = nil
	file_tests_pb_test_proto_goTypes = nil
	file_tests_pb_test_proto_depIdxs = nil
}
