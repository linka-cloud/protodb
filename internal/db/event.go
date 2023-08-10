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

package db

import (
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb/internal/protodb"
)

type event struct {
	typ protodb.EventType
	old proto.Message
	new proto.Message
	err error
}

func (e event) Type() protodb.EventType {
	return e.typ
}

func (e event) Old() proto.Message {
	return e.old
}

func (e event) New() proto.Message {
	return e.new
}

func (e event) Err() error {
	return e.err
}
