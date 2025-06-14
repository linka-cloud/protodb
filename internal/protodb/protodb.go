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
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/pb"
)

var (
	ErrNotLeader    = badgerd.ErrNotLeader
	ErrNoLeaderConn = errors.New("no leader connection")
)

type DB interface {
	Registerer
	Resolverer
	Reader
	Writer
	Watcher
	TxProvider
	SeqProvider
	Locker
	Leader
	io.Closer
}

type Tx interface {
	Reader
	Writer
	Committer
}

type Reader interface {
	Get(ctx context.Context, m proto.Message, opts ...GetOption) ([]proto.Message, *PagingInfo, error)
	GetOne(ctx context.Context, m proto.Message, opts ...GetOption) (proto.Message, bool, error)
}

type Watcher interface {
	Watch(ctx context.Context, m proto.Message, opts ...GetOption) (<-chan Event, error)
}

type Writer interface {
	Set(ctx context.Context, m proto.Message, opts ...SetOption) (proto.Message, error)
	Delete(ctx context.Context, m proto.Message) error
}

type TxProvider interface {
	Tx(ctx context.Context, opts ...TxOption) (Tx, error)
}

type Committer interface {
	Commit(ctx context.Context) error
	Close()
}

type SeqProvider interface {
	NextSeq(ctx context.Context, name string) (uint64, error)
}

type Registerer interface {
	RegisterProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) error
	Register(ctx context.Context, file protoreflect.FileDescriptor) error
	Descriptors(ctx context.Context) ([]*descriptorpb.DescriptorProto, error)
	FileDescriptors(ctx context.Context) ([]*descriptorpb.FileDescriptorProto, error)
}

type Leader interface {
	IsLeader() bool
	Leader() string
	LeaderChanges() <-chan string
}

type Resolverer interface {
	Resolver() protodesc.Resolver
}

type Locker interface {
	Lock(ctx context.Context, key string) error
	Unlock(ctx context.Context, key string) error
}

type EventType = pb.WatchEventType

const (
	EventTypeUnknown = pb.WatchEventUnknown
	EventTypeEnter   = pb.WatchEventEnter
	EventTypeLeave   = pb.WatchEventLeave
	EventTypeUpdate  = pb.WatchEventUpdate
)

type Event interface {
	Type() EventType
	Old() proto.Message
	New() proto.Message
	Err() error
}

func GetOne(ctx context.Context, r Reader, m proto.Message, opts ...GetOption) (proto.Message, bool, error) {
	res, _, err := r.Get(ctx, m, append(opts, WithOne())...)
	if err != nil {
		return nil, false, err
	}
	if len(res) == 0 {
		return nil, false, nil
	}
	return res[0], true, nil
}
