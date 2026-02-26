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
	"fmt"
	"reflect"
	"slices"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	ruleEnumValueNoDeleteUnlessNumberReserved = "ENUM_VALUE_NO_DELETE_UNLESS_NUMBER_RESERVED"
	ruleFieldNoDeleteUnlessNumberReserved     = "FIELD_NO_DELETE_UNLESS_NUMBER_RESERVED"
	ruleFieldSameDefault                      = "FIELD_SAME_DEFAULT"
	ruleFieldSameOneof                        = "FIELD_SAME_ONEOF"
	ruleFieldWireCompatibleCardinality        = "FIELD_WIRE_COMPATIBLE_CARDINALITY"
	ruleFieldWireCompatibleType               = "FIELD_WIRE_COMPATIBLE_TYPE"
	ruleFileSamePackage                       = "FILE_SAME_PACKAGE"
	ruleMessageSameRequiredFields             = "MESSAGE_SAME_REQUIRED_FIELDS"
	ruleReservedEnumNoDelete                  = "RESERVED_ENUM_NO_DELETE"
	ruleReservedMessageNoDelete               = "RESERVED_MESSAGE_NO_DELETE"
	ruleRpcSameClientStreaming                = "RPC_SAME_CLIENT_STREAMING"
	ruleRpcSameIdempotencyLevel               = "RPC_SAME_IDEMPOTENCY_LEVEL"
	ruleRpcSameRequestType                    = "RPC_SAME_REQUEST_TYPE"
	ruleRpcSameResponseType                   = "RPC_SAME_RESPONSE_TYPE"
	ruleRpcSameServerStreaming                = "RPC_SAME_SERVER_STREAMING"
)

type Violation struct {
	Rule    string
	Subject string
	Message string
}

// CheckWireBreaking compares two FileDescriptorSets and returns violations for
// the WIRE category rules from Buf's breaking change detection.
func CheckWireBreaking(previous, current *descriptorpb.FileDescriptorSet) ([]Violation, error) {
	prevSchema, err := newSchema(previous)
	if err != nil {
		return nil, err
	}
	currSchema, err := newSchema(current)
	if err != nil {
		return nil, err
	}

	var violations []Violation
	checkFileSamePackage(prevSchema, currSchema, &violations)
	checkEnumValueNoDeleteUnlessNumberReserved(prevSchema, currSchema, &violations)
	checkFieldNoDeleteUnlessNumberReserved(prevSchema, currSchema, &violations)
	checkFieldSameDefault(prevSchema, currSchema, &violations)
	checkFieldSameOneof(prevSchema, currSchema, &violations)
	checkFieldWireCompatibleCardinality(prevSchema, currSchema, &violations)
	checkFieldWireCompatibleType(prevSchema, currSchema, &violations)
	checkMessageSameRequiredFields(prevSchema, currSchema, &violations)
	checkReservedEnumNoDelete(prevSchema, currSchema, &violations)
	checkReservedMessageNoDelete(prevSchema, currSchema, &violations)
	checkRpcSameClientStreaming(prevSchema, currSchema, &violations)
	checkRpcSameServerStreaming(prevSchema, currSchema, &violations)
	checkRpcSameRequestType(prevSchema, currSchema, &violations)
	checkRpcSameResponseType(prevSchema, currSchema, &violations)
	checkRpcSameIdempotencyLevel(prevSchema, currSchema, &violations)

	return violations, nil
}

type schema struct {
	filesByPath map[string]protoreflect.FileDescriptor
	messages    map[protoreflect.FullName]protoreflect.MessageDescriptor
	enums       map[protoreflect.FullName]protoreflect.EnumDescriptor
	services    map[protoreflect.FullName]protoreflect.ServiceDescriptor
}

func newSchema(set *descriptorpb.FileDescriptorSet) (*schema, error) {
	if set == nil {
		set = &descriptorpb.FileDescriptorSet{}
	}
	files, err := protodesc.NewFiles(set)
	if err != nil {
		return nil, err
	}
	s := &schema{
		filesByPath: make(map[string]protoreflect.FileDescriptor),
		messages:    make(map[protoreflect.FullName]protoreflect.MessageDescriptor),
		enums:       make(map[protoreflect.FullName]protoreflect.EnumDescriptor),
		services:    make(map[protoreflect.FullName]protoreflect.ServiceDescriptor),
	}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		s.filesByPath[fd.Path()] = fd
		for i := 0; i < fd.Messages().Len(); i++ {
			s.addMessage(fd.Messages().Get(i))
		}
		for i := 0; i < fd.Enums().Len(); i++ {
			ed := fd.Enums().Get(i)
			s.enums[ed.FullName()] = ed
		}
		for i := 0; i < fd.Services().Len(); i++ {
			sd := fd.Services().Get(i)
			s.services[sd.FullName()] = sd
		}
		return true
	})
	return s, nil
}

func (s *schema) addMessage(md protoreflect.MessageDescriptor) {
	s.messages[md.FullName()] = md
	for i := 0; i < md.Messages().Len(); i++ {
		s.addMessage(md.Messages().Get(i))
	}
	for i := 0; i < md.Enums().Len(); i++ {
		ed := md.Enums().Get(i)
		s.enums[ed.FullName()] = ed
	}
}

func checkFileSamePackage(prev, curr *schema, out *[]Violation) {
	for path, prevFile := range prev.filesByPath {
		currFile, ok := curr.filesByPath[path]
		if !ok {
			continue
		}
		if prevFile.Package() != currFile.Package() {
			addViolation(out, ruleFileSamePackage, "file:"+path,
				"package changed from %q to %q", prevFile.Package(), currFile.Package())
		}
	}
}

func checkEnumValueNoDeleteUnlessNumberReserved(prev, curr *schema, out *[]Violation) {
	for name, prevEnum := range prev.enums {
		currEnum, ok := curr.enums[name]
		if !ok {
			continue
		}
		values := prevEnum.Values()
		for i := 0; i < values.Len(); i++ {
			v := values.Get(i)
			if currEnum.Values().ByNumber(v.Number()) != nil {
				continue
			}
			if currEnum.ReservedRanges().Has(v.Number()) {
				continue
			}
			addViolation(out, ruleEnumValueNoDeleteUnlessNumberReserved, "enum:"+string(name),
				"value number %d deleted without reserving number", v.Number())
		}
	}
}

func checkFieldNoDeleteUnlessNumberReserved(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevFields := fieldsByNumber(prevMsg)
		currFields := fieldsByNumber(currMsg)
		for number := range prevFields {
			if _, ok := currFields[number]; ok {
				continue
			}
			if currMsg.ReservedRanges().Has(number) {
				continue
			}
			addViolation(out, ruleFieldNoDeleteUnlessNumberReserved,
				fmt.Sprintf("message:%s field:%d", name, number),
				"field deleted without reserving number")
		}
	}
}

func checkFieldSameDefault(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevFields := fieldsByNumber(prevMsg)
		currFields := fieldsByNumber(currMsg)
		for number, prevField := range prevFields {
			currField, ok := currFields[number]
			if !ok {
				continue
			}
			if !isDefaultComparable(prevField) || !isDefaultComparable(currField) {
				continue
			}
			if defaultEqual(prevField, currField) {
				continue
			}
			addViolation(out, ruleFieldSameDefault,
				fmt.Sprintf("message:%s field:%d", name, number),
				"default value changed")
		}
	}
}

func checkFieldSameOneof(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevFields := fieldsByNumber(prevMsg)
		currFields := fieldsByNumber(currMsg)
		for number, prevField := range prevFields {
			currField, ok := currFields[number]
			if !ok {
				continue
			}
			prevOneof := oneofFullName(prevField)
			currOneof := oneofFullName(currField)
			if prevOneof == currOneof {
				continue
			}
			addViolation(out, ruleFieldSameOneof,
				fmt.Sprintf("message:%s field:%d", name, number),
				"oneof changed from %q to %q", prevOneof, currOneof)
		}
	}
}

func checkFieldWireCompatibleCardinality(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevFields := fieldsByNumber(prevMsg)
		currFields := fieldsByNumber(currMsg)
		for number, prevField := range prevFields {
			currField, ok := currFields[number]
			if !ok {
				continue
			}
			if isWireCompatibleCardinality(prevField, currField) {
				continue
			}
			addViolation(out, ruleFieldWireCompatibleCardinality,
				fmt.Sprintf("message:%s field:%d", name, number),
				"cardinality changed from %s to %s",
				cardinalityLabel(prevField), cardinalityLabel(currField))
		}
	}
}

func checkFieldWireCompatibleType(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevFields := fieldsByNumber(prevMsg)
		currFields := fieldsByNumber(currMsg)
		for number, prevField := range prevFields {
			currField, ok := currFields[number]
			if !ok {
				continue
			}
			if isWireCompatibleType(prevField, currField) {
				continue
			}
			addViolation(out, ruleFieldWireCompatibleType,
				fmt.Sprintf("message:%s field:%d", name, number),
				"type changed from %s to %s", fieldTypeLabel(prevField), fieldTypeLabel(currField))
		}
	}
}

func checkMessageSameRequiredFields(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevRequired := requiredFieldNumbers(prevMsg)
		currRequired := requiredFieldNumbers(currMsg)
		for number := range prevRequired {
			if _, ok := currRequired[number]; ok {
				continue
			}
			addViolation(out, ruleMessageSameRequiredFields, "message:"+string(name),
				"required field %d removed", number)
		}
		for number := range currRequired {
			if _, ok := prevRequired[number]; ok {
				continue
			}
			addViolation(out, ruleMessageSameRequiredFields, "message:"+string(name),
				"required field %d added", number)
		}
	}
}

func checkReservedEnumNoDelete(prev, curr *schema, out *[]Violation) {
	for name, prevEnum := range prev.enums {
		currEnum, ok := curr.enums[name]
		if !ok {
			continue
		}
		prevNames := prevEnum.ReservedNames()
		currNames := currEnum.ReservedNames()
		for i := 0; i < prevNames.Len(); i++ {
			reserved := prevNames.Get(i)
			if currNames.Has(reserved) {
				continue
			}
			addViolation(out, ruleReservedEnumNoDelete, "enum:"+string(name),
				"reserved name %q removed", reserved)
		}
		prevRanges := enumIntervals(prevEnum.ReservedRanges())
		currRanges := enumIntervals(currEnum.ReservedRanges())
		for _, prevRange := range prevRanges {
			if rangeCovered(prevRange, currRanges) {
				continue
			}
			addViolation(out, ruleReservedEnumNoDelete, "enum:"+string(name),
				"reserved range %s removed", enumRangeLabel(prevRange))
		}
	}
}

func checkReservedMessageNoDelete(prev, curr *schema, out *[]Violation) {
	for name, prevMsg := range prev.messages {
		currMsg, ok := curr.messages[name]
		if !ok {
			continue
		}
		prevNames := prevMsg.ReservedNames()
		currNames := currMsg.ReservedNames()
		for i := 0; i < prevNames.Len(); i++ {
			reserved := prevNames.Get(i)
			if currNames.Has(reserved) {
				continue
			}
			addViolation(out, ruleReservedMessageNoDelete, "message:"+string(name),
				"reserved name %q removed", reserved)
		}
		prevRanges := fieldIntervals(prevMsg.ReservedRanges())
		currRanges := fieldIntervals(currMsg.ReservedRanges())
		for _, prevRange := range prevRanges {
			if rangeCovered(prevRange, currRanges) {
				continue
			}
			addViolation(out, ruleReservedMessageNoDelete, "message:"+string(name),
				"reserved range %s removed", fieldRangeLabel(prevRange))
		}
	}
}

func checkRpcSameClientStreaming(prev, curr *schema, out *[]Violation) {
	compareMethods(prev, curr, func(prevMethod, currMethod protoreflect.MethodDescriptor) {
		if prevMethod.IsStreamingClient() == currMethod.IsStreamingClient() {
			return
		}
		addViolation(out, ruleRpcSameClientStreaming, rpcSubject(prevMethod),
			"client streaming changed from %t to %t",
			prevMethod.IsStreamingClient(), currMethod.IsStreamingClient())
	})
}

func checkRpcSameServerStreaming(prev, curr *schema, out *[]Violation) {
	compareMethods(prev, curr, func(prevMethod, currMethod protoreflect.MethodDescriptor) {
		if prevMethod.IsStreamingServer() == currMethod.IsStreamingServer() {
			return
		}
		addViolation(out, ruleRpcSameServerStreaming, rpcSubject(prevMethod),
			"server streaming changed from %t to %t",
			prevMethod.IsStreamingServer(), currMethod.IsStreamingServer())
	})
}

func checkRpcSameRequestType(prev, curr *schema, out *[]Violation) {
	compareMethods(prev, curr, func(prevMethod, currMethod protoreflect.MethodDescriptor) {
		if prevMethod.Input().FullName() == currMethod.Input().FullName() {
			return
		}
		addViolation(out, ruleRpcSameRequestType, rpcSubject(prevMethod),
			"request type changed from %s to %s",
			prevMethod.Input().FullName(), currMethod.Input().FullName())
	})
}

func checkRpcSameResponseType(prev, curr *schema, out *[]Violation) {
	compareMethods(prev, curr, func(prevMethod, currMethod protoreflect.MethodDescriptor) {
		if prevMethod.Output().FullName() == currMethod.Output().FullName() {
			return
		}
		addViolation(out, ruleRpcSameResponseType, rpcSubject(prevMethod),
			"response type changed from %s to %s",
			prevMethod.Output().FullName(), currMethod.Output().FullName())
	})
}

func checkRpcSameIdempotencyLevel(prev, curr *schema, out *[]Violation) {
	compareMethods(prev, curr, func(prevMethod, currMethod protoreflect.MethodDescriptor) {
		prevLevel := methodIdempotency(prevMethod)
		currLevel := methodIdempotency(currMethod)
		if prevLevel == currLevel {
			return
		}
		addViolation(out, ruleRpcSameIdempotencyLevel, rpcSubject(prevMethod),
			"idempotency level changed from %s to %s",
			prevLevel.String(), currLevel.String())
	})
}

func compareMethods(prev, curr *schema, f func(prevMethod, currMethod protoreflect.MethodDescriptor)) {
	for _, prevService := range prev.services {
		currService, ok := curr.services[prevService.FullName()]
		if !ok {
			continue
		}
		methods := prevService.Methods()
		for i := 0; i < methods.Len(); i++ {
			prevMethod := methods.Get(i)
			currMethod := currService.Methods().ByName(prevMethod.Name())
			if currMethod == nil {
				continue
			}
			f(prevMethod, currMethod)
		}
	}
}

func methodIdempotency(method protoreflect.MethodDescriptor) descriptorpb.MethodOptions_IdempotencyLevel {
	if opts, ok := method.Options().(*descriptorpb.MethodOptions); ok && opts != nil {
		return opts.GetIdempotencyLevel()
	}
	if method.Options() == nil {
		return descriptorpb.MethodOptions_IDEMPOTENCY_UNKNOWN
	}
	clone := proto.Clone(method.Options())
	if opts, ok := clone.(*descriptorpb.MethodOptions); ok && opts != nil {
		return opts.GetIdempotencyLevel()
	}
	return descriptorpb.MethodOptions_IDEMPOTENCY_UNKNOWN
}

func fieldsByNumber(md protoreflect.MessageDescriptor) map[protoreflect.FieldNumber]protoreflect.FieldDescriptor {
	fields := md.Fields()
	out := make(map[protoreflect.FieldNumber]protoreflect.FieldDescriptor, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		out[fd.Number()] = fd
	}
	return out
}

func requiredFieldNumbers(md protoreflect.MessageDescriptor) map[protoreflect.FieldNumber]struct{} {
	fields := md.Fields()
	out := make(map[protoreflect.FieldNumber]struct{})
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Cardinality() == protoreflect.Required {
			out[fd.Number()] = struct{}{}
		}
	}
	return out
}

func oneofFullName(fd protoreflect.FieldDescriptor) string {
	if fd.ContainingOneof() == nil {
		return ""
	}
	return string(fd.ContainingOneof().FullName())
}

type cardinalityKind int

const (
	cardinalityImplicitOptional cardinalityKind = iota
	cardinalityExplicitOptional
	cardinalityRequired
	cardinalityRepeated
	cardinalityMap
)

func isWireCompatibleCardinality(prev, curr protoreflect.FieldDescriptor) bool {
	prevKind := cardinalityOf(prev)
	currKind := cardinalityOf(curr)
	if prevKind == currKind {
		return true
	}
	if isOptionalPresencePair(prevKind, currKind) {
		return true
	}
	return isRepeatedMapPair(prevKind, currKind)
}

func cardinalityOf(fd protoreflect.FieldDescriptor) cardinalityKind {
	if fd.IsMap() {
		return cardinalityMap
	}
	switch fd.Cardinality() {
	case protoreflect.Repeated:
		return cardinalityRepeated
	case protoreflect.Required:
		return cardinalityRequired
	default:
		if fd.HasPresence() {
			return cardinalityExplicitOptional
		}
		return cardinalityImplicitOptional
	}
}

func isOptionalPresencePair(a, b cardinalityKind) bool {
	return (a == cardinalityImplicitOptional && b == cardinalityExplicitOptional) ||
		(a == cardinalityExplicitOptional && b == cardinalityImplicitOptional)
}

func isRepeatedMapPair(a, b cardinalityKind) bool {
	return (a == cardinalityRepeated && b == cardinalityMap) ||
		(a == cardinalityMap && b == cardinalityRepeated)
}

func cardinalityLabel(fd protoreflect.FieldDescriptor) string {
	switch cardinalityOf(fd) {
	case cardinalityImplicitOptional:
		return "optional-implicit"
	case cardinalityExplicitOptional:
		return "optional-explicit"
	case cardinalityRequired:
		return "required"
	case cardinalityRepeated:
		return "repeated"
	case cardinalityMap:
		return "map"
	default:
		return "unknown"
	}
}

func isWireCompatibleType(prev, curr protoreflect.FieldDescriptor) bool {
	if prev.Kind() == protoreflect.EnumKind && curr.Kind() == protoreflect.EnumKind {
		return enumSubset(prev.Enum(), curr.Enum())
	}
	if prev.Kind() == protoreflect.MessageKind || prev.Kind() == protoreflect.GroupKind ||
		curr.Kind() == protoreflect.MessageKind || curr.Kind() == protoreflect.GroupKind {
		return prev.Kind() == curr.Kind() && prev.Message().FullName() == curr.Message().FullName()
	}
	if prev.Kind() == curr.Kind() {
		return true
	}
	if isIntegralSwap(prev.Kind(), curr.Kind()) {
		return true
	}
	if isSignedSwap(prev.Kind(), curr.Kind()) {
		return true
	}
	if isFixed32Swap(prev.Kind(), curr.Kind()) {
		return true
	}
	if isFixed64Swap(prev.Kind(), curr.Kind()) {
		return true
	}
	if prev.Kind() == protoreflect.StringKind && curr.Kind() == protoreflect.BytesKind {
		return true
	}
	return false
}

func enumSubset(prev, curr protoreflect.EnumDescriptor) bool {
	if prev == nil || curr == nil {
		return false
	}
	if prev.Name() != curr.Name() {
		return false
	}
	currValues := curr.Values()
	currByName := make(map[protoreflect.Name]protoreflect.EnumNumber, currValues.Len())
	for i := 0; i < currValues.Len(); i++ {
		v := currValues.Get(i)
		currByName[v.Name()] = v.Number()
	}
	prevValues := prev.Values()
	for i := 0; i < prevValues.Len(); i++ {
		v := prevValues.Get(i)
		if number, ok := currByName[v.Name()]; !ok || number != v.Number() {
			return false
		}
	}
	return true
}

func isIntegralSwap(a, b protoreflect.Kind) bool {
	return isKindInSet(a, []protoreflect.Kind{
		protoreflect.Int32Kind,
		protoreflect.Uint32Kind,
		protoreflect.Int64Kind,
		protoreflect.Uint64Kind,
		protoreflect.BoolKind,
	}) && isKindInSet(b, []protoreflect.Kind{
		protoreflect.Int32Kind,
		protoreflect.Uint32Kind,
		protoreflect.Int64Kind,
		protoreflect.Uint64Kind,
		protoreflect.BoolKind,
	})
}

func isSignedSwap(a, b protoreflect.Kind) bool {
	return isKindInSet(a, []protoreflect.Kind{protoreflect.Sint32Kind, protoreflect.Sint64Kind}) &&
		isKindInSet(b, []protoreflect.Kind{protoreflect.Sint32Kind, protoreflect.Sint64Kind})
}

func isFixed32Swap(a, b protoreflect.Kind) bool {
	return isKindInSet(a, []protoreflect.Kind{protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind}) &&
		isKindInSet(b, []protoreflect.Kind{protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind})
}

func isFixed64Swap(a, b protoreflect.Kind) bool {
	return isKindInSet(a, []protoreflect.Kind{protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind}) &&
		isKindInSet(b, []protoreflect.Kind{protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind})
}

func isKindInSet(kind protoreflect.Kind, set []protoreflect.Kind) bool {
	return slices.Contains(set, kind)
}

func fieldTypeLabel(fd protoreflect.FieldDescriptor) string {
	if fd.Kind() == protoreflect.EnumKind {
		return fmt.Sprintf("enum(%s)", fd.Enum().FullName())
	}
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		return fmt.Sprintf("%s(%s)", fd.Kind(), fd.Message().FullName())
	}
	return fd.Kind().String()
}

func isDefaultComparable(fd protoreflect.FieldDescriptor) bool {
	if fd.Cardinality() == protoreflect.Repeated {
		return false
	}
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		return false
	}
	return true
}

func defaultEqual(prev, curr protoreflect.FieldDescriptor) bool {
	if prev.Kind() == protoreflect.EnumKind && curr.Kind() == protoreflect.EnumKind {
		prevDefault := prev.DefaultEnumValue()
		currDefault := curr.DefaultEnumValue()
		if prevDefault == nil || currDefault == nil {
			return prevDefault == currDefault
		}
		return prevDefault.Number() == currDefault.Number()
	}
	return reflect.DeepEqual(prev.Default().Interface(), curr.Default().Interface())
}

type interval struct {
	start int64
	end   int64
}

func fieldIntervals(ranges protoreflect.FieldRanges) []interval {
	intervals := make([]interval, 0, ranges.Len())
	for i := 0; i < ranges.Len(); i++ {
		r := ranges.Get(i)
		intervals = append(intervals, interval{start: int64(r[0]), end: int64(r[1])})
	}
	return normalizeIntervals(intervals)
}

func enumIntervals(ranges protoreflect.EnumRanges) []interval {
	intervals := make([]interval, 0, ranges.Len())
	for i := 0; i < ranges.Len(); i++ {
		r := ranges.Get(i)
		intervals = append(intervals, interval{start: int64(r[0]), end: int64(r[1]) + 1})
	}
	return normalizeIntervals(intervals)
}

func normalizeIntervals(ranges []interval) []interval {
	if len(ranges) == 0 {
		return ranges
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start == ranges[j].start {
			return ranges[i].end < ranges[j].end
		}
		return ranges[i].start < ranges[j].start
	})
	merged := []interval{ranges[0]}
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if r.start > last.end {
			merged = append(merged, r)
			continue
		}
		if r.end > last.end {
			last.end = r.end
		}
	}
	return merged
}

func rangeCovered(prev interval, curr []interval) bool {
	cursor := prev.start
	for _, r := range curr {
		if r.end <= cursor {
			continue
		}
		if r.start > cursor {
			return false
		}
		if r.end > cursor {
			cursor = r.end
		}
		if cursor >= prev.end {
			return true
		}
	}
	return cursor >= prev.end
}

func enumRangeLabel(r interval) string {
	return fmt.Sprintf("%d-%d", r.start, r.end-1)
}

func fieldRangeLabel(r interval) string {
	return fmt.Sprintf("%d-%d", r.start, r.end-1)
}

func rpcSubject(method protoreflect.MethodDescriptor) string {
	if method == nil {
		return "rpc:<unknown>"
	}
	return fmt.Sprintf("rpc:%s/%s", method.Parent().FullName(), method.Name())
}

func addViolation(out *[]Violation, rule, subject, format string, args ...any) {
	*out = append(*out, Violation{
		Rule:    rule,
		Subject: subject,
		Message: fmt.Sprintf(format, args...),
	})
}
