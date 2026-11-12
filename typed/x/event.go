// Copyright 2026 Linka Cloud  All rights reserved.
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

package typed

import "go.linka.cloud/protodb"

type Event[T any, PT Message[T]] interface {
	Type() protodb.EventType
	Old() PT
	New() PT
	Err() error
}

type event[T any, PT Message[T]] struct {
	typ protodb.EventType
	old PT
	new PT
	err error
}

func (e *event[T, PT]) Type() protodb.EventType {
	return e.typ
}

func (e *event[T, PT]) Old() PT {
	return e.old
}

func (e *event[T, PT]) New() PT {
	return e.new
}

func (e *event[T, PT]) Err() error {
	return e.err
}
