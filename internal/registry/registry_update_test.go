// Copyright 2026 Linka Cloud  All rights reserved.
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

package registry

import (
	"testing"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestTypesUpdateMessage(t *testing.T) {
	registry := &Types{}
	initial := buildMessageType(t, "acme.v1", "Widget", "name")
	updated := buildMessageType(t, "acme.v1", "Widget", "id")

	if err := registry.RegisterMessage(initial); err != nil {
		t.Fatalf("RegisterMessage(initial) unexpected error: %v", err)
	}
	if err := registry.RegisterMessage(updated); err != nil {
		t.Fatalf("RegisterMessage(updated) unexpected error: %v", err)
	}

	got, err := registry.FindMessageByName("acme.v1.Widget")
	if err != nil {
		t.Fatalf("FindMessageByName unexpected error: %v", err)
	}
	if got != updated {
		t.Fatalf("FindMessageByName returned stale type")
	}
	if got.Descriptor().Fields().ByName("id") == nil {
		t.Errorf("updated message missing field id")
	}
	if got.Descriptor().Fields().ByName("name") != nil {
		t.Errorf("updated message still has field name")
	}
}

func TestTypesUpdateEnum(t *testing.T) {
	registry := &Types{}
	initial := buildEnumType(t, "acme.v1", "Status", "STATUS_UNSPECIFIED", 0)
	updated := buildEnumType(t, "acme.v1", "Status", "STATUS_READY", 1)

	if err := registry.RegisterEnum(initial); err != nil {
		t.Fatalf("RegisterEnum(initial) unexpected error: %v", err)
	}
	if err := registry.RegisterEnum(updated); err != nil {
		t.Fatalf("RegisterEnum(updated) unexpected error: %v", err)
	}

	got, err := registry.FindEnumByName("acme.v1.Status")
	if err != nil {
		t.Fatalf("FindEnumByName unexpected error: %v", err)
	}
	if got != updated {
		t.Fatalf("FindEnumByName returned stale type")
	}
	if got.Descriptor().Values().ByName("STATUS_READY") == nil {
		t.Errorf("updated enum missing STATUS_READY")
	}
	if got.Descriptor().Values().ByName("STATUS_UNSPECIFIED") != nil {
		t.Errorf("updated enum still has STATUS_UNSPECIFIED")
	}
}

func TestTypesReplaceKindAdjustsCounts(t *testing.T) {
	registry := &Types{}
	message := buildMessageType(t, "acme.v1", "Widget", "name")
	if err := registry.RegisterMessage(message); err != nil {
		t.Fatalf("RegisterMessage unexpected error: %v", err)
	}
	if registry.NumMessages() != 1 || registry.NumEnums() != 0 || registry.NumExtensions() != 0 {
		t.Fatalf("unexpected counts after message register: messages=%d enums=%d extensions=%d",
			registry.NumMessages(), registry.NumEnums(), registry.NumExtensions())
	}

	enum := buildEnumType(t, "acme.v1", "Widget", "WIDGET_UNSPECIFIED", 0)
	if err := registry.RegisterEnum(enum); err != nil {
		t.Fatalf("RegisterEnum unexpected error: %v", err)
	}
	if registry.NumMessages() != 0 || registry.NumEnums() != 1 || registry.NumExtensions() != 0 {
		t.Fatalf("unexpected counts after enum replace: messages=%d enums=%d extensions=%d",
			registry.NumMessages(), registry.NumEnums(), registry.NumExtensions())
	}
	if _, err := registry.FindMessageByName("acme.v1.Widget"); err == nil || err == NotFound {
		t.Fatalf("FindMessageByName should fail with wrong type error")
	}

	extension := buildExtensionType(t, "acme.v1", "Target", "Widget", 101)
	if err := registry.RegisterExtension(extension); err != nil {
		t.Fatalf("RegisterExtension unexpected error: %v", err)
	}
	if registry.NumMessages() != 0 || registry.NumEnums() != 0 || registry.NumExtensions() != 1 {
		t.Fatalf("unexpected counts after extension replace: messages=%d enums=%d extensions=%d",
			registry.NumMessages(), registry.NumEnums(), registry.NumExtensions())
	}
	if _, err := registry.FindEnumByName("acme.v1.Widget"); err == nil || err == NotFound {
		t.Fatalf("FindEnumByName should fail with wrong type error")
	}
	got, err := registry.FindExtensionByName("acme.v1.Widget")
	if err != nil {
		t.Fatalf("FindExtensionByName unexpected error: %v", err)
	}
	if got != extension {
		t.Fatalf("FindExtensionByName returned stale type")
	}
}

func TestTypesReplaceExtensionByMessage(t *testing.T) {
	registry := &Types{}
	ext1 := buildExtensionType(t, "acme.v1", "Target", "flag", 101)
	ext2 := buildExtensionType(t, "acme.v1", "Other", "flag", 202)

	if err := registry.RegisterExtension(ext1); err != nil {
		t.Fatalf("RegisterExtension(ext1) unexpected error: %v", err)
	}
	if err := registry.RegisterExtension(ext2); err != nil {
		t.Fatalf("RegisterExtension(ext2) unexpected error: %v", err)
	}
	if registry.NumExtensions() != 1 {
		t.Fatalf("expected 1 extension, got %d", registry.NumExtensions())
	}
	if got := registry.NumExtensionsByMessage("acme.v1.Target"); got != 0 {
		t.Fatalf("expected 0 extensions for Target, got %d", got)
	}
	if got := registry.NumExtensionsByMessage("acme.v1.Other"); got != 1 {
		t.Fatalf("expected 1 extension for Other, got %d", got)
	}
	if _, err := registry.FindExtensionByNumber("acme.v1.Target", 101); err != NotFound {
		t.Fatalf("expected NotFound for old extension number, got %v", err)
	}
	gotByNumber, err := registry.FindExtensionByNumber("acme.v1.Other", 202)
	if err != nil {
		t.Fatalf("FindExtensionByNumber unexpected error: %v", err)
	}
	if gotByNumber != ext2 {
		t.Fatalf("FindExtensionByNumber returned stale type")
	}
	gotByName, err := registry.FindExtensionByName("acme.v1.flag")
	if err != nil {
		t.Fatalf("FindExtensionByName unexpected error: %v", err)
	}
	if gotByName != ext2 {
		t.Fatalf("FindExtensionByName returned stale type")
	}
}

func TestFilesReplaceByPath(t *testing.T) {
	registry := &Files{}
	path := "acme/v1/widget.proto"
	file1 := buildFileWithMessage(t, path, "acme.v1", "Widget", "name")
	file2 := buildFileWithMessage(t, path, "acme.v1", "Widget", "id")

	if err := registry.RegisterFile(file1); err != nil {
		t.Fatalf("RegisterFile(file1) unexpected error: %v", err)
	}
	if err := registry.RegisterFile(file2); err != nil {
		t.Fatalf("RegisterFile(file2) unexpected error: %v", err)
	}
	if registry.NumFiles() != 1 {
		t.Fatalf("expected 1 file, got %d", registry.NumFiles())
	}

	fd, err := registry.FindFileByPath(path)
	if err != nil {
		t.Fatalf("FindFileByPath unexpected error: %v", err)
	}
	if fd != file2 {
		t.Fatalf("FindFileByPath returned stale file")
	}
	msg, err := registry.FindDescriptorByName("acme.v1.Widget")
	if err != nil {
		t.Fatalf("FindDescriptorByName unexpected error: %v", err)
	}
	md, ok := msg.(protoreflect.MessageDescriptor)
	if !ok {
		t.Fatalf("FindDescriptorByName returned wrong descriptor type")
	}
	if md.Fields().ByName("id") == nil {
		t.Errorf("updated message missing field id")
	}
	if md.Fields().ByName("name") != nil {
		t.Errorf("updated message still has field name")
	}

	var count int
	registry.RangeFiles(func(_ protoreflect.FileDescriptor) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("expected 1 ranged file, got %d", count)
	}
}

func TestFilesReplaceBySymbolConflict(t *testing.T) {
	registry := &Files{}
	file1 := buildFileWithMessage(t, "acme/v1/first.proto", "acme.v1", "Widget", "name")
	file2 := buildFileWithMessage(t, "acme/v1/second.proto", "acme.v1", "Widget", "id")

	if err := registry.RegisterFile(file1); err != nil {
		t.Fatalf("RegisterFile(file1) unexpected error: %v", err)
	}
	if err := registry.RegisterFile(file2); err != nil {
		t.Fatalf("RegisterFile(file2) unexpected error: %v", err)
	}
	if registry.NumFiles() != 1 {
		t.Fatalf("expected 1 file, got %d", registry.NumFiles())
	}
	if _, err := registry.FindFileByPath("acme/v1/first.proto"); err != NotFound {
		t.Fatalf("expected NotFound for replaced file, got %v", err)
	}
	fd, err := registry.FindFileByPath("acme/v1/second.proto")
	if err != nil {
		t.Fatalf("FindFileByPath unexpected error: %v", err)
	}
	if fd != file2 {
		t.Fatalf("FindFileByPath returned stale file")
	}
}

func buildMessageType(t *testing.T, pkg, name, fieldName string) protoreflect.MessageType {
	t.Helper()

	field := protobuilder.NewField(protoreflect.Name(fieldName), protobuilder.FieldTypeString()).SetNumber(1)
	message := protobuilder.NewMessage(protoreflect.Name(name)).AddField(field)
	file := protobuilder.NewFile("").
		SetPackageName(protoreflect.FullName(pkg)).
		AddMessage(message)

	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build message file: %v", err)
	}
	md := fd.Messages().ByName(protoreflect.Name(name))
	if md == nil {
		t.Fatalf("message %s not found in descriptor", name)
	}
	return dynamicpb.NewMessageType(md)
}

func buildEnumType(t *testing.T, pkg, name, valueName string, valueNumber protoreflect.EnumNumber) protoreflect.EnumType {
	t.Helper()

	value := protobuilder.NewEnumValue(protoreflect.Name(valueName)).SetNumber(valueNumber)
	enum := protobuilder.NewEnum(protoreflect.Name(name)).AddValue(value)
	file := protobuilder.NewFile("").
		SetPackageName(protoreflect.FullName(pkg)).
		AddEnum(enum)

	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build enum file: %v", err)
	}
	ed := fd.Enums().ByName(protoreflect.Name(name))
	if ed == nil {
		t.Fatalf("enum %s not found in descriptor", name)
	}
	return dynamicpb.NewEnumType(ed)
}

func buildExtensionType(t *testing.T, pkg, extendeeName, extensionName string, number protoreflect.FieldNumber) protoreflect.ExtensionType {
	t.Helper()

	extendee := protobuilder.NewMessage(protoreflect.Name(extendeeName)).AddExtensionRange(100, 1000)
	extension := protobuilder.NewExtension(protoreflect.Name(extensionName), number, protobuilder.FieldTypeString(), extendee)
	file := protobuilder.NewFile("").
		SetPackageName(protoreflect.FullName(pkg)).
		AddMessage(extendee).
		AddExtension(extension)

	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build extension file: %v", err)
	}
	xd := fd.Extensions().ByName(protoreflect.Name(extensionName))
	if xd == nil {
		t.Fatalf("extension %s not found in descriptor", extensionName)
	}
	return dynamicpb.NewExtensionType(xd)
}

func buildFileWithMessage(t *testing.T, path, pkg, name, fieldName string) protoreflect.FileDescriptor {
	t.Helper()

	field := protobuilder.NewField(protoreflect.Name(fieldName), protobuilder.FieldTypeString()).SetNumber(1)
	message := protobuilder.NewMessage(protoreflect.Name(name)).AddField(field)
	file := protobuilder.NewFile(path).
		SetPackageName(protoreflect.FullName(pkg)).
		AddMessage(message)

	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build file: %v", err)
	}
	return fd
}
