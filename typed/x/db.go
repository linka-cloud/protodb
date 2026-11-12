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

import (
	"context"
	"fmt"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.linka.cloud/protodb"
)

type Message[T any] interface {
	proto.Message
	*T
}

type DB struct {
	c protodb.Client
}

func New(c protodb.Client) *DB {
	return &DB{c: c}
}

func (db *DB) Raw() protodb.Client {
	return db.c
}

func (db *DB) Register[T any, PT Message[T]](ctx context.Context) error {
	var a T
	var m PT = &a
	return db.c.Register(ctx, m.ProtoReflect().Descriptor().ParentFile())
}

func (db *DB) Descriptors(ctx context.Context) ([]*descriptorpb.DescriptorProto, error) {
	return db.c.Descriptors(ctx)
}

func (db *DB) FileDescriptors(ctx context.Context) ([]*descriptorpb.FileDescriptorProto, error) {
	return db.c.FileDescriptors(ctx)
}

func (db *DB) NextSeq(ctx context.Context, name string) (uint64, error) {
	return db.c.NextSeq(ctx, name)
}

func (db *DB) Lock(ctx context.Context, key string) error {
	return db.c.Lock(ctx, key)
}

func (db *DB) Unlock(ctx context.Context, key string) error {
	return db.c.Unlock(ctx, key)
}

func (db *DB) Get[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error) {
	return getTyped[T, PT](ctx, db.c, m, opts...)
}

func (db *DB) GetOne[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.GetOption) (PT, bool, error) {
	return getOneTyped[T, PT](ctx, db.c, m, opts...)
}

func (db *DB) Set[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.SetOption) (PT, error) {
	return setTyped[T, PT](ctx, db.c, m, opts...)
}

func (db *DB) Delete[T any, PT Message[T]](ctx context.Context, m PT) error {
	return deleteTyped[T, PT](ctx, db.c, m)
}

func (db *DB) Watch[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.GetOption) (<-chan Event[T, PT], error) {
	out := make(chan Event[T, PT])
	ch, err := db.c.Watch(ctx, m, opts...)
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

func (db *DB) Tx(ctx context.Context, opts ...protodb.TxOption) (*Tx, error) {
	txn, err := db.c.Tx(ctx, opts...)
	return &Tx{txn: txn}, err
}

func (db *DB) Maybe() *Maybe {
	return &Maybe{db: db}
}

func (db *DB) WithTx(ctx context.Context, fn func(ctx context.Context, tx *Tx) error, opts ...protodb.TxOption) error {
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (db *DB) WithTx2[R any](ctx context.Context, fn func(ctx context.Context, tx *Tx) (R, error), opts ...protodb.TxOption) (R, error) {
	var o R
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return o, err
	}
	defer tx.Close()
	o, err = fn(ctx, tx)
	if err != nil {
		return o, err
	}
	if err := tx.Commit(ctx); err != nil {
		return o, err
	}
	return o, nil
}

func (db *DB) WithTx3[R1, R2 any](ctx context.Context, fn func(ctx context.Context, tx *Tx) (R1, R2, error), opts ...protodb.TxOption) (R1, R2, error) {
	var (
		o1 R1
		o2 R2
	)
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return o1, o2, err
	}
	defer tx.Close()
	o1, o2, err = fn(ctx, tx)
	if err != nil {
		return o1, o2, err
	}
	if err := tx.Commit(ctx); err != nil {
		return o1, o2, err
	}
	return o1, o2, nil
}

func (db *DB) WithTx4[R1, R2, R3 any](ctx context.Context, fn func(ctx context.Context, tx *Tx) (R1, R2, R3, error), opts ...protodb.TxOption) (R1, R2, R3, error) {
	var (
		o1 R1
		o2 R2
		o3 R3
	)
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return o1, o2, o3, err
	}
	defer tx.Close()
	o1, o2, o3, err = fn(ctx, tx)
	if err != nil {
		return o1, o2, o3, err
	}
	if err := tx.Commit(ctx); err != nil {
		return o1, o2, o3, err
	}
	return o1, o2, o3, nil
}

func getTyped[T any, PT Message[T]](ctx context.Context, r protodb.Reader, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error) {
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

func getOneTyped[T any, PT Message[T]](ctx context.Context, r protodb.Reader, m PT, opts ...protodb.GetOption) (PT, bool, error) {
	v, ok, err := r.GetOne(ctx, m, opts...)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	vv, ok := v.(PT)
	if !ok {
		return nil, false, fmt.Errorf("unexpected type for Typed: %T", v)
	}
	return vv, true, nil
}

func setTyped[T any, PT Message[T]](ctx context.Context, w protodb.Writer, m PT, opts ...protodb.SetOption) (PT, error) {
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

func deleteTyped[T any, PT Message[T]](ctx context.Context, w protodb.Writer, m PT) error {
	return w.Delete(ctx, m)
}
