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

package breaking

import (
	"testing"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestCheckWireBreakingFieldRules(t *testing.T) {
	prevFile := buildFieldRulesFile(t, false)
	currFile := buildFieldRulesFile(t, true)
	violations, err := CheckWireBreaking(fileSet(prevFile), fileSet(currFile))
	if err != nil {
		t.Fatalf("CheckWireBreaking error: %v", err)
	}

	requireRule(t, violations, ruleFieldNoDeleteUnlessNumberReserved)
	requireRule(t, violations, ruleFieldSameDefault)
	requireRule(t, violations, ruleFieldSameOneof)
	requireRule(t, violations, ruleFieldWireCompatibleCardinality)
	requireRule(t, violations, ruleFieldWireCompatibleType)
	requireRule(t, violations, ruleMessageSameRequiredFields)
}

func TestCheckWireBreakingEnumReservedRules(t *testing.T) {
	prevFile := buildEnumRulesFile(t, false)
	currFile := buildEnumRulesFile(t, true)
	violations, err := CheckWireBreaking(fileSet(prevFile), fileSet(currFile))
	if err != nil {
		t.Fatalf("CheckWireBreaking error: %v", err)
	}

	requireRule(t, violations, ruleEnumValueNoDeleteUnlessNumberReserved)
	requireRule(t, violations, ruleReservedEnumNoDelete)
}

func TestCheckWireBreakingRpcRules(t *testing.T) {
	prevFile := buildRpcRulesFile(t, false)
	currFile := buildRpcRulesFile(t, true)
	violations, err := CheckWireBreaking(fileSet(prevFile), fileSet(currFile))
	if err != nil {
		t.Fatalf("CheckWireBreaking error: %v", err)
	}

	requireRule(t, violations, ruleRpcSameClientStreaming)
	requireRule(t, violations, ruleRpcSameServerStreaming)
	requireRule(t, violations, ruleRpcSameRequestType)
	requireRule(t, violations, ruleRpcSameResponseType)
	requireRule(t, violations, ruleRpcSameIdempotencyLevel)
}

func buildFieldRulesFile(t *testing.T, updated bool) protoreflect.FileDescriptor {
	t.Helper()

	msg := protobuilder.NewMessage("Message")
	req := protobuilder.NewField("req", protobuilder.FieldTypeString()).SetNumber(1)
	if !updated {
		req.SetRequired()
	}
	opt := protobuilder.NewField("opt", protobuilder.FieldTypeString()).SetNumber(2)
	if !updated {
		opt.SetDefaultValue("a")
	} else {
		opt.SetDefaultValue("b")
	}
	var typeField *protobuilder.FieldBuilder
	if updated {
		typeField = protobuilder.NewField("i32", protobuilder.FieldTypeBytes()).SetNumber(3)
	} else {
		typeField = protobuilder.NewField("i32", protobuilder.FieldTypeInt32()).SetNumber(3)
	}
	if updated {
		msg.AddField(req).AddField(opt).AddField(typeField)
		msg.AddField(protobuilder.NewField("sel", protobuilder.FieldTypeString()).SetNumber(4))
	} else {
		oneof := protobuilder.NewOneof("choice").
			AddChoice(protobuilder.NewField("sel", protobuilder.FieldTypeString()).SetNumber(4))
		msg.AddField(req).AddField(opt).AddField(typeField)
		msg.AddOneOf(oneof)
		msg.AddField(protobuilder.NewField("removed", protobuilder.FieldTypeString()).SetNumber(5))
	}

	file := protobuilder.NewFile("field.proto").
		SetPackageName("acme.v1").
		SetSyntax(protoreflect.Proto2).
		AddMessage(msg)
	return mustBuildFile(t, file)
}

func buildEnumRulesFile(t *testing.T, updated bool) protoreflect.FileDescriptor {
	t.Helper()

	enum := protobuilder.NewEnum("Status")
	if updated {
		enum.AddValue(protobuilder.NewEnumValue("STATUS_OK").SetNumber(1))
	} else {
		enum.AddValue(protobuilder.NewEnumValue("STATUS_OK").SetNumber(1))
		enum.AddValue(protobuilder.NewEnumValue("STATUS_GONE").SetNumber(2))
		enum.AddReservedRange(10, 12)
	}
	file := protobuilder.NewFile("enum.proto").
		SetPackageName("acme.v1").
		SetSyntax(protoreflect.Proto2).
		AddEnum(enum)
	return mustBuildFile(t, file)
}

func buildRpcRulesFile(t *testing.T, updated bool) protoreflect.FileDescriptor {
	t.Helper()

	req := protobuilder.NewMessage("Request")
	resp := protobuilder.NewMessage("Response")
	req2 := protobuilder.NewMessage("Request2")
	resp2 := protobuilder.NewMessage("Response2")

	reqType := protobuilder.RpcTypeMessage(req, false)
	respType := protobuilder.RpcTypeMessage(resp, false)
	if updated {
		reqType = protobuilder.RpcTypeMessage(req2, true)
		respType = protobuilder.RpcTypeMessage(resp2, true)
	}
	method := protobuilder.NewMethod("Call", reqType, respType)
	if updated {
		method.Options = &descriptorpb.MethodOptions{
			IdempotencyLevel: descriptorpb.MethodOptions_NO_SIDE_EFFECTS.Enum(),
		}
	} else {
		method.Options = &descriptorpb.MethodOptions{
			IdempotencyLevel: descriptorpb.MethodOptions_IDEMPOTENT.Enum(),
		}
	}
	service := protobuilder.NewService("Service").AddMethod(method)
	file := protobuilder.NewFile("rpc.proto").
		SetPackageName("acme.v1").
		SetSyntax(protoreflect.Proto2).
		AddMessage(req).
		AddMessage(resp).
		AddMessage(req2).
		AddMessage(resp2).
		AddService(service)
	return mustBuildFile(t, file)
}

func fileSet(files ...protoreflect.FileDescriptor) *descriptorpb.FileDescriptorSet {
	set := &descriptorpb.FileDescriptorSet{}
	for _, fd := range files {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
	}
	return set
}

func mustBuildFile(t *testing.T, file *protobuilder.FileBuilder) protoreflect.FileDescriptor {
	t.Helper()
	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build file: %v", err)
	}
	return fd
}

func requireRule(t *testing.T, violations []Violation, rule string) {
	t.Helper()
	for _, v := range violations {
		if v.Rule == rule {
			return
		}
	}
	t.Fatalf("expected rule %s violation, got %+v", rule, violations)
}
