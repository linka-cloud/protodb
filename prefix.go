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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	data        = "_data"
	index       = "_index"
	descriptors = "_schema"
)

func dataPrefix(m proto.Message) ([]byte, error) {
	k, err := keyFor(m)
	if err != nil {
		return []byte(fmt.Sprintf("%s/%s/", data, m.ProtoReflect().Descriptor().FullName())), fmt.Errorf("key: %w", err)
	}
	return []byte(fmt.Sprintf("%s/%s/%s", data, m.ProtoReflect().Descriptor().FullName(), k)), nil
}

func descriptorPrefix(d *descriptorpb.FileDescriptorProto) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", descriptors, d.GetPackage(), d.GetName()))
}
