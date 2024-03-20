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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	Data        = "_data"
	Index       = "_index"
	Descriptors = "_schema"
	Internal    = "_internal"
)

func DataPrefix(m proto.Message) (key []byte, field string, value string, err error) {
	k, field, err := KeyFor(m)
	if err != nil {
		return []byte(fmt.Sprintf("%s/%s/", Data, m.ProtoReflect().Descriptor().FullName())), field, k, fmt.Errorf("key: %w", err)
	}
	return []byte(fmt.Sprintf("%s/%s/%s", Data, m.ProtoReflect().Descriptor().FullName(), k)), field, k, nil
}

func DescriptorPrefix(d *descriptorpb.FileDescriptorProto) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", Descriptors, d.GetPackage(), d.GetName()))
}
