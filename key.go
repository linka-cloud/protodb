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

func keyFromOpts(m proto.Message) (string, bool) {
	sk := proto.GetExtension(m.ProtoReflect().Descriptor().Options(), protodb.E_StaticKey)
	if sk != nil && sk.(string) != "" {
		return sk.(string), true
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
		return "", false
	}
	v := m.ProtoReflect().Get(kf)
	if !v.IsValid() {
		return "", true
	}
	if k := v.String(); k != "" && k != "0" {
		return k, true
	}
	return "", true
}

func keyFor(m proto.Message) (string, error) {
	if k, ok := keyFromOpts(m); ok {
		if k != "" {
			return k, nil
		}
		return "", fmt.Errorf("key / id not found in %s", m.ProtoReflect().Type().Descriptor().FullName())

	}
	switch i := interface{}(m).(type) {
	case *dynamicpb.Message:
		var k string
		i.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch strings.ToLower(string(descriptor.FullName().Name())) {
			case "id":
				if id := value.String(); id != "" && id != "0" {
					k = id
					return false
				}
			case "key":
				if key := value.String(); key != "" && key != "0" {
					k = key
					return false
				}
			case "name":
				if n := value.String(); n != "" && n != "0" {
					k = n
					return false
				}
			}
			return true
		})
		if k != "" {
			return k, nil
		}
	case interface{ Key() string }:
		if k := i.Key(); k != "" {
			return k, nil
		}
	case interface{ GetKey() string }:
		if k := i.GetKey(); k != "" {
			return k, nil
		}
	case interface{ ID() string }:
		if k := i.ID(); k != "" {
			return k, nil
		}
	case interface{ GetID() string }:
		if k := i.GetID(); k != "" {
			return k, nil
		}
	case interface{ Id() string }:
		if k := i.Id(); k != "" {
			return k, nil
		}
	case interface{ GetId() string }:
		if k := i.GetId(); k != "" {
			return k, nil
		}
	case interface{ ID() int64 }:
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, nil
		}
	case interface{ GetID() int64 }:
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, nil
		}
	case interface{ Id() int64 }:
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, nil
		}
	case interface{ GetId() int64 }:
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, nil
		}
	case interface{ ID() int32 }:
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, nil
		}
	case interface{ GetID() int32 }:
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, nil
		}
	case interface{ Id() int32 }:
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, nil
		}
	case interface{ GetId() int32 }:
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, nil
		}
	case interface{ ID() uint32 }:
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, nil
		}
	case interface{ GetID() uint32 }:
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, nil
		}
	case interface{ Id() uint32 }:
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, nil
		}
	case interface{ GetId() uint32 }:
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, nil
		}
	case interface{ ID() uint64 }:
		if i.ID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, nil
		}
	case interface{ GetID() uint64 }:
		if i.GetID() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetID()); k != "" {
			return k, nil
		}
	case interface{ Id() uint64 }:
		if i.Id() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, nil
		}
	case interface{ GetId() uint64 }:
		if i.GetId() == 0 {
			break
		}
		if k := fmt.Sprintf("%d", i.GetId()); k != "" {
			return k, nil
		}
	case interface{ Name() string }:
		if k := i.Name(); k != "" {
			return k, nil
		}
	case interface{ GetName() string }:
		if k := i.GetName(); k != "" {
			return k, nil
		}
	}
	return "", fmt.Errorf("key / id not found in %s", m.ProtoReflect().Type().Descriptor().FullName())
}
