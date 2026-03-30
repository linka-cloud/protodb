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

package protodb

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/protodb"
)

const (
	fieldID   = "id"
	fieldKey  = "key"
	fieldName = "name"
)

func KeyFromOpts(m proto.Message) (key string, field string, ok bool) {
	sk := proto.GetExtension(m.ProtoReflect().Descriptor().Options(), protodb.E_StaticKey)
	if sk != nil && sk.(string) != "" {
		return sk.(string), "", true
	}
	path, ok := keyFieldPathFromOpts(m.ProtoReflect().Type().Descriptor())
	if !ok {
		return "", "", false
	}
	field = keyPathFromNames(path)
	if !isKeyLeaf(path[len(path)-1]) {
		return "", field, true
	}
	v, ok := keyPathValue(m.ProtoReflect(), path)
	if !ok || !v.IsValid() {
		return "", field, true
	}
	if k := v.String(); k != "" && k != "0" {
		return k, field, true
	}
	return "", field, true
}

func KeyFor(m proto.Message) (key string, field string, err error) {
	if k, field, ok := KeyFromOpts(m); ok {
		if k != "" {
			return k, field, nil
		}
		return "", field, fmt.Errorf("key / id not found in %s", m.ProtoReflect().Type().Descriptor().FullName())

	}
	switch i := any(m).(type) {
	case *dynamicpb.Message:
		var fd protoreflect.FieldDescriptor
		i.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch strings.ToLower(string(descriptor.FullName().Name())) {
			case "id", "key", "name":
				fd = descriptor
				return false
			}
			return true
		})
		if fd == nil {
			break
		}
		switch fd.Kind() {
		case protoreflect.MessageKind:
			return "", "", fmt.Errorf("key %s is a message", fd.FullName())
		case protoreflect.GroupKind:
			return "", "", fmt.Errorf("key %s is a group", fd.FullName())
		}
		if fd.Cardinality() == protoreflect.Repeated {
			return "", "", fmt.Errorf("key %s is repeated", fd.FullName())
		}
		if !i.Has(fd) {
			break
		}
		return i.Get(fd).String(), string(fd.Name()), nil
	case interface{ Key() string }:
		field = fieldKey
		if k := i.Key(); k != "" {
			return k, field, nil
		}
	case interface{ GetKey() string }:
		field = fieldKey
		if k := i.GetKey(); k != "" {
			return k, field, nil
		}
	case interface{ ID() string }:
		field = fieldID
		if k := i.ID(); k != "" {
			return k, field, nil
		}
	case interface{ GetID() string }:
		field = fieldID
		if k := i.GetID(); k != "" {
			return k, field, nil
		}
	case interface{ Id() string }:
		field = fieldID
		if k := i.Id(); k != "" {
			return k, field, nil
		}
	case interface{ GetId() string }:
		field = fieldID
		if k := i.GetId(); k != "" {
			return k, field, nil
		}
	case interface{ ID() int64 }:
		field = fieldID
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, field, nil
		}
	case interface{ GetID() int64 }:
		field = fieldID
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, field, nil
		}
	case interface{ Id() int64 }:
		field = fieldID
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, field, nil
		}
	case interface{ GetId() int64 }:
		field = fieldID
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, field, nil
		}
	case interface{ ID() int32 }:
		field = fieldID
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, field, nil
		}
	case interface{ GetID() int32 }:
		field = fieldID
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, field, nil
		}
	case interface{ Id() int32 }:
		field = fieldID
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, field, nil
		}
	case interface{ GetId() int32 }:
		field = fieldID
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, field, nil
		}
	case interface{ ID() uint32 }:
		field = fieldID
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, field, nil
		}
	case interface{ GetID() uint32 }:
		field = fieldID
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, field, nil
		}
	case interface{ Id() uint32 }:
		field = fieldID
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, field, nil
		}
	case interface{ GetId() uint32 }:
		field = fieldID
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, field, nil
		}
	case interface{ ID() uint64 }:
		field = fieldID
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, field, nil
		}
	case interface{ GetID() uint64 }:
		field = fieldID
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, field, nil
		}
	case interface{ Id() uint64 }:
		field = fieldID
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, field, nil
		}
	case interface{ GetId() uint64 }:
		field = fieldID
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, field, nil
		}
	case interface{ Name() string }:
		field = fieldName
		if k := i.Name(); k != "" {
			return k, field, nil
		}
	case interface{ GetName() string }:
		field = fieldName
		if k := i.GetName(); k != "" {
			return k, field, nil
		}
	}
	return "", field, fmt.Errorf("key / id not found in %s", m.ProtoReflect().Type().Descriptor().FullName())
}

func KeyFieldName(md protoreflect.MessageDescriptor) (string, bool) {
	if md == nil {
		return "", false
	}
	if path, ok := keyFieldPathFromOpts(md); ok {
		return keyPathFromNames(path), true
	}
	m := dynamicpb.NewMessage(md)
	_, field, _ := KeyFor(m)
	if field != "" {
		return field, true
	}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		v, ok := keyProbeValue(fd)
		if !ok {
			continue
		}
		m.Set(fd, v)
		_, field, err := KeyFor(m)
		m.Clear(fd)
		if err != nil {
			continue
		}
		if field != string(fd.Name()) {
			continue
		}
		return field, true
	}
	return "", false
}

func keyFieldPathFromOpts(md protoreflect.MessageDescriptor) ([]protoreflect.FieldDescriptor, bool) {
	if md == nil {
		return nil, false
	}
	stack := map[protoreflect.FullName]bool{md.FullName(): true}
	return keyFieldPathFromOptsIn(md, nil, stack)
}

func keyFieldPathFromOptsIn(md protoreflect.MessageDescriptor, path []protoreflect.FieldDescriptor, stack map[protoreflect.FullName]bool) ([]protoreflect.FieldDescriptor, bool) {
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		next := appendFieldPath(path, fd)
		if isExplicitKey(fd) {
			return next, true
		}
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			continue
		}
		child := fd.Message()
		if child == nil || stack[child.FullName()] {
			continue
		}
		stack[child.FullName()] = true
		if nested, ok := keyFieldPathFromOptsIn(child, next, stack); ok {
			return nested, true
		}
		delete(stack, child.FullName())
	}
	return nil, false
}

func appendFieldPath(path []protoreflect.FieldDescriptor, fd protoreflect.FieldDescriptor) []protoreflect.FieldDescriptor {
	next := make([]protoreflect.FieldDescriptor, 0, len(path)+1)
	next = append(next, path...)
	return append(next, fd)
}

func isExplicitKey(fd protoreflect.FieldDescriptor) bool {
	if fd == nil {
		return false
	}
	o, ok := fd.Options().(*descriptorpb.FieldOptions)
	if !ok {
		return false
	}
	v := proto.GetExtension(o, protodb.E_Key)
	if v == nil {
		return false
	}
	b, ok := v.(bool)
	if !ok {
		return false
	}
	return b
}

func keyPathFromNames(path []protoreflect.FieldDescriptor) string {
	parts := make([]string, 0, len(path))
	for _, fd := range path {
		parts = append(parts, string(fd.Name()))
	}
	return strings.Join(parts, ".")
}

func keyPathValue(m protoreflect.Message, path []protoreflect.FieldDescriptor) (protoreflect.Value, bool) {
	cur := m
	for i, fd := range path {
		v := cur.Get(fd)
		if i == len(path)-1 {
			return v, true
		}
		if fd.Kind() != protoreflect.MessageKind {
			return protoreflect.Value{}, false
		}
		next := v.Message()
		if !next.IsValid() {
			return protoreflect.Value{}, false
		}
		cur = next
	}
	return protoreflect.Value{}, false
}

func isKeyLeaf(fd protoreflect.FieldDescriptor) bool {
	if fd == nil {
		return false
	}
	if fd.IsMap() || fd.IsList() {
		return false
	}
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return false
	default:
		return true
	}
}

func keyProbeValue(fd protoreflect.FieldDescriptor) (protoreflect.Value, bool) {
	if fd == nil {
		return protoreflect.Value{}, false
	}
	if fd.IsList() || fd.IsMap() {
		return protoreflect.Value{}, false
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true), true
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(1), true
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1), true
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1), true
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1), true
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1), true
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1), true
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1), true
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("x"), true
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("x")), true
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protoreflect.Value{}, false
	}
	return protoreflect.Value{}, false
}

func SeqKey(name string) string {
	return fmt.Sprintf("%s/%s", Seq, name)
}
