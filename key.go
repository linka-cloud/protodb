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
	"google.golang.org/protobuf/types/dynamicpb"
)

func keyFor(m proto.Message) (string, error) {
	switch i := interface{}(m).(type) {
	case *dynamicpb.Message:
		var k string
		i.Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch strings.ToLower(string(descriptor.FullName().Name())) {
			case "id":
				if id := value.String(); id != "" {
					k = id
					return false
				}
				if id := value.Int(); id != 0 {
					k = fmt.Sprintf("%d", id)
					return false
				}
			case "key":
				if key := value.String(); key != "" {
					k = key
					return false
				}
			case "name":
				if n := value.String(); n != "" {
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
	case interface{ ID() string }:
		if k := i.ID(); k != "" {
			return k, nil
		}
	case interface{ Id() string }:
		if k := i.Id(); k != "" {
			return k, nil
		}
	case interface{ ID() int64 }:
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, nil
		}
	case interface{ Id() int64 }:
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
			return k, nil
		}
	case interface{ ID() int32 }:
		if k := fmt.Sprintf("%d", i.ID()); k != "" {
			return k, nil
		}
	case interface{ Id() int32 }:
		if k := fmt.Sprintf("%d", i.Id()); k != "" {
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
