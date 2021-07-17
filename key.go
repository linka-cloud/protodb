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
)

func keyFor(m proto.Message) (string, error) {
	switch i := interface{}(m).(type) {
	case interface{ Key() string }:
		return i.Key(), nil
	case interface{ ID() string }:
		return i.ID(), nil
	case interface{ Id() string }:
		return i.Id(), nil
	case interface{ ID() int64 }:
		return fmt.Sprintf("%d", i.ID()), nil
	case interface{ Id() int64 }:
		return fmt.Sprintf("%d", i.Id()), nil
	case interface{ ID() int32 }:
		return fmt.Sprintf("%d", i.ID()), nil
	case interface{ Id() int32 }:
		return fmt.Sprintf("%d", i.Id()), nil
	case interface{ Name() string }:
		return i.Name(), nil
	case interface{ GetName() string }:
		return i.GetName(), nil
	}
	return "", fmt.Errorf("key / id not found in %s", m.ProtoReflect().Type().Descriptor().FullName())
}
