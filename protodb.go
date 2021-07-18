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
	"context"
	"io"

	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.linka.cloud/protodb/pb"
)

type Paging = pb.Paging
type PagingInfo = pb.PagingInfo

type DB interface {
	Registerer
	Reader
	Writer
	Watcher
	TxProvider
	io.Closer
}

type Tx interface {
	Reader
	Writer
	Committer
}

type Reader interface {
	Get(ctx context.Context, m proto.Message, paging *Paging, filters ...*filters.FieldFilter) ([]proto.Message, *PagingInfo, error)
}

type Watcher interface {
	Watch(ctx context.Context, m proto.Message, filters ...*filters.FieldFilter) (<-chan Event, error)
}

type Writer interface {
	Put(ctx context.Context, m proto.Message) (proto.Message, error)
	Delete(ctx context.Context, m proto.Message) error
}

type TxProvider interface {
	Tx(ctx context.Context) (Tx, error)
}

type Committer interface {
	Commit(ctx context.Context) error
	Close()
}

type Registerer interface {
	RegisterProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) error
	Register(ctx context.Context, file protoreflect.FileDescriptor) error
	Resolver() protodesc.Resolver
}

type EventType = pb.WatchEventType

const (
	EventTypeEnter  = pb.WatchEventEnter
	EventTypeLeave  = pb.WatchEventLeave
	EventTypeUpdate = pb.WatchEventUpdate
)

type Event interface {
	Type() EventType
	Old() proto.Message
	New() proto.Message
	Err() error
}
