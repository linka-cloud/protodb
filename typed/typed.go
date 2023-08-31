// Copyright 2022 Linka Cloud  All rights reserved.
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

import (
	"context"
	"fmt"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/internal/client"
)

type message[T any] interface {
	proto.Message
	*T
}

type Store[T any, PT message[T]] interface {
	Reader[T, PT]
	Writer[T, PT]
	Watcher[T, PT]
	TxProvider[T, PT]

	Register(ctx context.Context) error

	Raw() client.Client
}

type Tx[T any, PT message[T]] interface {
	protodb.Committer
	protodb.Sizer
	Reader[T, PT]
	Writer[T, PT]
	Raw() protodb.Tx
}

type Reader[T any, PT message[T]] interface {
	Get(ctx context.Context, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error)
}

type Watcher[T any, PT message[T]] interface {
	Watch(ctx context.Context, m PT, opts ...protodb.GetOption) (<-chan Event[T, PT], error)
}

type Writer[T any, PT message[T]] interface {
	Set(ctx context.Context, m PT, opts ...protodb.SetOption) (PT, error)
	Delete(ctx context.Context, m PT) error
}

type TxProvider[T any, PT message[T]] interface {
	Tx(ctx context.Context) (Tx[T, PT], error)
}

type Event[T any, PT message[T]] interface {
	Type() protodb.EventType
	Old() PT
	New() PT
	Err() error
}

func NewStore[T any, PT message[T]](c client.Client) Store[T, PT] {
	return &store[T, PT]{db: c}
}

type store[T any, PT message[T]] struct {
	db client.Client
}

func (s *store[T, PT]) Raw() client.Client {
	return s.db
}

func (s *store[T, PT]) Register(ctx context.Context) error {
	var a T
	var m PT = &a
	return s.db.Register(ctx, m.ProtoReflect().Descriptor().ParentFile())
}

func (s *store[T, PT]) Get(ctx context.Context, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error) {
	return getTyped[T, PT](ctx, s.db, m, opts...)
}

func (s *store[T, PT]) Set(ctx context.Context, m PT, opts ...protodb.SetOption) (PT, error) {
	return setTyped[T, PT](ctx, s.db, m, opts...)
}

func (s *store[T, PT]) Delete(ctx context.Context, m PT) error {
	return deleteTyped[T, PT](ctx, s.db, m)
}

func (s *store[T, PT]) Watch(ctx context.Context, m PT, opts ...protodb.GetOption) (<-chan Event[T, PT], error) {
	out := make(chan Event[T, PT])
	ch, err := s.db.Watch(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for e := range ch {
			if e.Type() == 0 {
				continue
			}
			ev := &event[T, PT]{typ: e.Type(), err: e.Err()}
			if n := e.New(); n != nil {
				v, ok := n.(PT)
				if !ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for new Typed: %T", n))
				} else {
					ev.new = v
				}
			}
			if o := e.Old(); o != nil {
				v, ok := o.(PT)
				if !ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for old Typed: %T", o))
				} else {
					ev.old = v
				}
			}
			out <- ev
		}
	}()
	return out, nil
}

func (s *store[T, PT]) Tx(ctx context.Context) (Tx[T, PT], error) {
	txn, err := s.db.Tx(ctx)
	return &tx[T, PT]{txn: txn}, err
}

func getTyped[T any, PT message[T]](ctx context.Context, r protodb.Reader, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error) {
	ms, i, err := r.Get(ctx, m, opts...)
	if err != nil {
		return nil, nil, err
	}
	var out []PT
	for _, v := range ms {
		vv, ok := v.(PT)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected type for Typed: %T", v)
		}
		out = append(out, vv)
	}
	return out, i, nil
}

func setTyped[T any, PT message[T]](ctx context.Context, w protodb.Writer, m PT, opts ...protodb.SetOption) (PT, error) {
	v, err := w.Set(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	vv, ok := v.(PT)
	if !ok {
		return nil, fmt.Errorf("unexpected type for Typed: %T", v)
	}
	return vv, nil
}

func deleteTyped[T any, PT message[T]](ctx context.Context, w protodb.Writer, m PT) error {
	return w.Delete(ctx, m)
}

func NewTx[T any, PT message[T]](txn protodb.Tx) Tx[T, PT] {
	return &tx[T, PT]{txn: txn}
}

type tx[T any, PT message[T]] struct {
	txn protodb.Tx
}

func (t *tx[T, PT]) Raw() protodb.Tx {
	return t.txn
}

func (t *tx[T, PT]) Get(ctx context.Context, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error) {
	return getTyped[T, PT](ctx, t.txn, m, opts...)
}

func (t *tx[T, PT]) Set(ctx context.Context, m PT, opts ...protodb.SetOption) (PT, error) {
	return setTyped[T, PT](ctx, t.txn, m, opts...)
}

func (t *tx[T, PT]) Delete(ctx context.Context, m PT) error {
	return deleteTyped[T, PT](ctx, t.txn, m)
}

func (t *tx[T, PT]) Commit(ctx context.Context) error {
	return t.txn.Commit(ctx)
}

func (t *tx[T, PT]) Close() {
	t.txn.Close()
}

func (t *tx[T, PT]) Count() (int64, error) {
	return t.txn.Count()
}

func (t *tx[T, PT]) Size() (int64, error) {
	return t.txn.Size()
}

type event[T any, PT message[T]] struct {
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
