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
	var kf protoreflect.FieldDescriptor
	fields := m.ProtoReflect().Type().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		o, ok := f.Options().(*descriptorpb.FieldOptions)
		if !ok {
			continue
		}
		v := proto.GetExtension(o, protodb.E_Key)
		if v == nil {
			continue
		}
		b := v.(bool)
		if b {
			kf = f
			break
		}
	}
	if kf == nil {
		return "", "", false
	}
	field = string(kf.Name())
	v := m.ProtoReflect().Get(kf)
	if !v.IsValid() {
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
	switch i := interface{}(m).(type) {
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
		field = fieldID
		if k := i.Key(); k != "" {
			return k, field, nil
		}
	case interface{ GetKey() string }:
		field = fieldID
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
