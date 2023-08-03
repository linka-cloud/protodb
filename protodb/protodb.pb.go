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
// source: protodb/protodb.proto

package protodb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var file_protodb_protodb_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.MessageOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         1181,
		Name:          "protodb.enabled",
		Tag:           "varint,1181,opt,name=enabled",
		Filename:      "protodb/protodb.proto",
	},
	{
		ExtendedType:  (*descriptorpb.MessageOptions)(nil),
		ExtensionType: (*string)(nil),
		Field:         1182,
		Name:          "protodb.static_key",
		Tag:           "bytes,1182,opt,name=static_key",
		Filename:      "protodb/protodb.proto",
	},
	{
		ExtendedType:  (*descriptorpb.FieldOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         1191,
		Name:          "protodb.key",
		Tag:           "varint,1191,opt,name=key",
		Filename:      "protodb/protodb.proto",
	},
}

// Extension fields to descriptorpb.MessageOptions.
var (
	// enabled enable store generation
	//
	// optional bool enabled = 1181;
	E_Enabled = &file_protodb_protodb_proto_extTypes[0]
	// static_key is a static key for the message type
	//
	// optional string static_key = 1182;
	E_StaticKey = &file_protodb_protodb_proto_extTypes[1]
)

// Extension fields to descriptorpb.FieldOptions.
var (
	// key mark the field as message key
	//
	// optional bool key = 1191;
	E_Key = &file_protodb_protodb_proto_extTypes[2]
)

var File_protodb_protodb_proto protoreflect.FileDescriptor

var file_protodb_protodb_proto_rawDesc = []byte{
	0x0a, 0x15, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64,
	0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62,
	0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x3a, 0x3a, 0x0a, 0x07, 0x65, 0x6e, 0x61, 0x62, 0x6c, 0x65, 0x64, 0x12, 0x1f, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x9d,
	0x09, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x65, 0x6e, 0x61, 0x62, 0x6c, 0x65, 0x64, 0x3a, 0x3f,
	0x0a, 0x0a, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x5f, 0x6b, 0x65, 0x79, 0x12, 0x1f, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x9e, 0x09,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x73, 0x74, 0x61, 0x74, 0x69, 0x63, 0x4b, 0x65, 0x79, 0x3a,
	0x30, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1d, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xa7, 0x09, 0x20, 0x01, 0x28, 0x08, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x42, 0x28, 0x5a, 0x26, 0x67, 0x6f, 0x2e, 0x6c, 0x69, 0x6e, 0x6b, 0x61, 0x2e, 0x63, 0x6c,
	0x6f, 0x75, 0x64, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x64, 0x62, 0x3b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x64, 0x62,
}

var file_protodb_protodb_proto_goTypes = []interface{}{
	(*descriptorpb.MessageOptions)(nil), // 0: google.protobuf.MessageOptions
	(*descriptorpb.FieldOptions)(nil),   // 1: google.protobuf.FieldOptions
}
var file_protodb_protodb_proto_depIdxs = []int32{
	0, // 0: protodb.enabled:extendee -> google.protobuf.MessageOptions
	0, // 1: protodb.static_key:extendee -> google.protobuf.MessageOptions
	1, // 2: protodb.key:extendee -> google.protobuf.FieldOptions
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	0, // [0:3] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_protodb_protodb_proto_init() }
func file_protodb_protodb_proto_init() {
	if File_protodb_protodb_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_protodb_protodb_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 3,
			NumServices:   0,
		},
		GoTypes:           file_protodb_protodb_proto_goTypes,
		DependencyIndexes: file_protodb_protodb_proto_depIdxs,
		ExtensionInfos:    file_protodb_protodb_proto_extTypes,
	}.Build()
	File_protodb_protodb_proto = out.File
	file_protodb_protodb_proto_rawDesc = nil
	file_protodb_protodb_proto_goTypes = nil
	file_protodb_protodb_proto_depIdxs = nil
}
