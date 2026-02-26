// Copyright 2025 Linka Cloud  All rights reserved.
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
	"google.golang.org/protobuf/reflect/protoreflect"
	preg "google.golang.org/protobuf/reflect/protoregistry"
)

type Registry struct {
	*Files
	*Types
}

func New() (*Registry, error) {
	var err error
	r := &Registry{
		Files: &Files{},
		Types: &Types{},
	}
	preg.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		err = r.RegisterFile(fd)
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	preg.GlobalTypes.RangeMessages(func(mt protoreflect.MessageType) bool {
		err = r.RegisterMessage(mt)
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	preg.GlobalTypes.RangeEnums(func(et protoreflect.EnumType) bool {
		err = r.RegisterEnum(et)
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	preg.GlobalTypes.RangeExtensions(func(et protoreflect.ExtensionType) bool {
		err = r.RegisterExtension(et)
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}
