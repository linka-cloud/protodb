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
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	"go.linka.cloud/protodb/pb"
)

func cmpField(f protoreflect.FieldDescriptor, v1, v2 protoreflect.Value, prefix string, diffs map[string]*pb.FieldDiff) {
	diff := &pb.FieldDiff{}
	equals := false
	switch f.Kind() {
	case protoreflect.BoolKind:
		if v1.IsValid() {
			diff.From = structpb.NewBoolValue(v1.Bool())
		}
		if v2.IsValid() {
			diff.To = structpb.NewBoolValue(v2.Bool())
		}
		if v1.IsValid() && v2.IsValid() {
			equals = v1.Bool() == v2.Bool()
		}
	case protoreflect.EnumKind:
		if v1.IsValid() {
			diff.From = structpb.NewStringValue(string(f.Enum().Values().ByNumber(v1.Enum()).Name()))
		}
		if v2.IsValid() {
			diff.To = structpb.NewStringValue(string(f.Enum().Values().ByNumber(v2.Enum()).Name()))
		}
		if v1.IsValid() && v2.IsValid() {
			equals = v1.Enum() == v2.Enum()
		}
	case protoreflect.Int32Kind,
		protoreflect.Sint32Kind,
		protoreflect.Int64Kind,
		protoreflect.Sint64Kind,
		protoreflect.Fixed32Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.Fixed64Kind,
		protoreflect.Sfixed32Kind:
		if v1.IsValid() {
			diff.From = structpb.NewNumberValue(float64(v1.Int()))
		}
		if v2.IsValid() {
			diff.To = structpb.NewNumberValue(float64(v2.Int()))
		}
		if v1.IsValid() && v2.IsValid() {
			equals = v1.Int() == v2.Int()
		}
	case protoreflect.Uint32Kind,
		protoreflect.Uint64Kind:
		if v1.IsValid() {
			diff.From = structpb.NewNumberValue(float64(v1.Uint()))
		}
		if v2.IsValid() {
			diff.To = structpb.NewNumberValue(float64(v2.Uint()))
		}
		if v1.IsValid() && v2.IsValid() {
			equals = v1.Uint() == v2.Uint()
		}
	case protoreflect.FloatKind,
		protoreflect.DoubleKind:
		if v1.IsValid() {
			diff.From = structpb.NewNumberValue(v1.Float())
		}
		if v2.IsValid() {
			diff.To = structpb.NewNumberValue(v2.Float())
		}
		if v1.IsValid() && v2.IsValid() {
			equals = v1.Float() == v2.Float()
		}
	case protoreflect.StringKind:
		if v1.IsValid() {
			diff.From = structpb.NewStringValue(v1.String())
		}
		if v2.IsValid() {
			diff.To = structpb.NewStringValue(v2.String())
		}
		if v1.IsValid() && v2.IsValid() {
			equals = v1.String() == v2.String()
		}
	case protoreflect.BytesKind:
		if v1.IsValid() {
			diff.From = structpb.NewStringValue(string(v1.Bytes()))
		}
		if v2.IsValid() {
			diff.To = structpb.NewStringValue(string(v2.Bytes()))
		}
		if v1.IsValid() && v2.IsValid() {
			equals = bytes.Compare(v1.Bytes(), v2.Bytes()) == 0
		}
	case protoreflect.MessageKind:
		p := prefix
		if !f.IsList() {
			p += string(f.Name())
		}
		var mdiff map[string]*pb.FieldDiff
		switch {
		case v1.IsValid() && v2.IsValid():
			mdiff = cmp(v1.Message().Interface(), v2.Message().Interface(), p)
		case v1.IsValid():
			mdiff = cmp(v1.Message().Interface(), nil, p)
		case v2.IsValid():
			mdiff = cmp(nil, v2.Message().Interface(), p)
		}
		if len(mdiff) == 0 {
			return
		}
		for k, v := range mdiff {
			diffs[k] = v
		}
		return
	}
	if !equals {
		diffs[prefix+string(f.Name())] = diff
	}
}

func cmp(m1, m2 proto.Message, prefix string) map[string]*pb.FieldDiff {
	if prefix != "" {
		prefix += "."
	}
	diffs := make(map[string]*pb.FieldDiff)
	var m proto.Message
	switch {
	case m1 != nil:
		m = m1
	case m2 != nil:
		m = m2
	default:
		return diffs
	}
	fds := m.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fds.Len(); i++ {
		f := fds.Get(i)
		v1, v2 := protoreflect.Value{}, protoreflect.Value{}
		if m1 != nil {
			v1 = m1.ProtoReflect().Get(f)
		}
		if m2 != nil {
			v2 = m2.ProtoReflect().Get(f)
		}
		if f.IsList() {
			var l1, l2 protoreflect.List
			var l1Length, l2Length int
			if v1.IsValid() {
				l1 = v1.List()
				l1Length = l1.Len()
			}
			if v2.IsValid() {
				l2 = v2.List()
				l2Length = l2.Len()
			}
			for i := 0; i < l1Length; i++ {
				p := fmt.Sprintf("%s%s[%d]", prefix, f.Name(), i)
				if i < l2Length {
					cmpField(f, l1.Get(i), l2.Get(i), p, diffs)
				} else {
					cmpField(f, l1.Get(i), protoreflect.Value{}, p, diffs)
				}
			}
			if l2Length > l1Length {
				start := l1Length - 1
				if start < 0 {
					start = 0
				}
				for i := start; i < l2Length; i++ {
					p := fmt.Sprintf("%s%s[%d]", prefix, f.Name(), i)
					cmpField(f, protoreflect.Value{}, l2.Get(i), p, diffs)
				}
			}
			continue
		}
		if f.IsMap() {
			var m1, m2 protoreflect.Map
			if v1.IsValid() {
				m1 = v1.Map()
			}
			if v2.IsValid() {
				m2 = v2.Map()
			}
			m1.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
				if m2.Has(key) {
					cmpField(f, value, m2.Get(key), fmt.Sprintf("%s%s[%v]", prefix, f.Name(), key.Value()), diffs)
				} else {
					cmpField(f, value, protoreflect.Value{}, fmt.Sprintf("%s%s[%v]", prefix, f.Name(), key.Value()), diffs)
				}
				return true
			})
			m2.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
				if m1.Has(key) {
					cmpField(f, m1.Get(key), value, fmt.Sprintf("%s%s[%v]", prefix, f.Name(), key.Value()), diffs)
				} else {
					cmpField(f, protoreflect.Value{}, value, fmt.Sprintf("%s%s[%v]", prefix, f.Name(), key.Value()), diffs)
				}
				return true
			})
			continue
		}
		cmpField(f, v1, v2, prefix, diffs)
	}
	return diffs
}

func Cmp(m1, m2 proto.Message) (*pb.MessageDiff, error) {
	if m1 != nil && m2 != nil {
		if n1, n2 := m1.ProtoReflect().Descriptor().FullName(), m2.ProtoReflect().Descriptor().FullName(); n1 != n2 {
			return nil, fmt.Errorf("cannot compare two different types: %s and %s", n1, n2)
		}
	}
	diffs := cmp(m1, m2, "")
	return &pb.MessageDiff{Fields: diffs}, nil
}
