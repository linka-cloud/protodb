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

package registry

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.linka.cloud/protodb/protodb"
)

type Registry interface {
	Register(m protoreflect.MessageDescriptor) error
	Indexes(typ protoreflect.FullName) (indexes []int)
	IndexesNames(typ protoreflect.FullName) (names []protoreflect.FullName)
	Key(typ protoreflect.FullName) (index int, ok bool)
}

func New() Registry {
	return newRegistry()
}

func newRegistry() *reg {
	return &reg{
		types:   NewSyncMap[protoreflect.FullName, protoreflect.MessageDescriptor](),
		indexes: NewSyncMap[protoreflect.FullName, int](),
		keys:    NewSyncMap[protoreflect.FullName, int](),
	}
}

type reg struct {
	mu      sync.RWMutex
	types   *SyncMap[protoreflect.FullName, protoreflect.MessageDescriptor]
	indexes *SyncMap[protoreflect.FullName, int]
	keys    *SyncMap[protoreflect.FullName, int]
}

func (r *reg) merge(o *reg) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	o.types.Range(func(k protoreflect.FullName, v protoreflect.MessageDescriptor) bool {
		r.types.Store(k, v)
		return true
	})
	o.indexes.Range(func(k protoreflect.FullName, v int) bool {
		r.indexes.Store(k, v)
		return true
	})
	o.keys.Range(func(k protoreflect.FullName, v int) bool {
		r.keys.Store(k, v)
		return true
	})
}

func (r *reg) Register(m protoreflect.MessageDescriptor) error {
	tmp := newRegistry()
	typ := m.FullName()
	tmp.types.Store(typ, m)
	fields := m.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		o, ok := f.Options().(*descriptorpb.FieldOptions)
		if !ok {
			continue
		}
		if v := proto.GetExtension(o, protodb.E_Key); v != nil && v.(bool) {
			if _, ok := r.keys.Load(typ); ok {
				return fmt.Errorf("multiple keys for %s", typ)
			}
			tmp.keys.Store(typ, i)
		}
		if v := proto.GetExtension(o, protodb.E_Index); v != nil && v.(bool) {
			switch f.Kind() {
			case protoreflect.BoolKind:
			case protoreflect.EnumKind:
			case protoreflect.Int32Kind:
			case protoreflect.Int64Kind:
			case protoreflect.Uint32Kind:
			case protoreflect.Uint64Kind:
			case protoreflect.Sint32Kind:
			case protoreflect.Sint64Kind:
			case protoreflect.Sfixed32Kind:
			case protoreflect.Sfixed64Kind:
			case protoreflect.FloatKind:
			case protoreflect.DoubleKind:
			case protoreflect.StringKind:
			case protoreflect.BytesKind:
			case protoreflect.MessageKind:
				switch f.Message().FullName() {
				case "google.protobuf.Timestamp":
				case "google.protobuf.Duration":
				case "google.protobuf.DoubleValue":
				case "google.protobuf.FloatValue":
				case "google.protobuf.Int64Value":
				case "google.protobuf.UInt64Value":
				case "google.protobuf.Int32Value":
				case "google.protobuf.UInt32Value":
				case "google.protobuf.BoolValue":
				case "google.protobuf.StringValue":
				case "google.protobuf.BytesValue":
				default:
					return fmt.Errorf("unsupported index type %s", f.Kind())
				}
			case protoreflect.GroupKind:
				return fmt.Errorf("unsupported index type %s", f.Kind())
			}
			tmp.indexes.Store(typ.Append(f.Name()), i)
		}
	}
	if tmp.keys.Len() == 0 {
		return fmt.Errorf("no key for %s", typ)
	}
	r.merge(tmp)
	return nil
}

func (r *reg) Indexes(typ protoreflect.FullName) (indexes []int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.indexes.Range(func(key protoreflect.FullName, value int) bool {
		if key.Parent() == typ {
			indexes = append(indexes, value)
		}
		return true
	})
	return
}

func (r *reg) IndexesNames(typ protoreflect.FullName) (names []protoreflect.FullName) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.indexes.Range(func(key protoreflect.FullName, value int) bool {
		if key.Parent() == typ {
			names = append(names, key)
		}
		return true
	})
	return
}

func (r *reg) Key(typ protoreflect.FullName) (index int, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.keys.Load(typ)
}
