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

	"go.linka.cloud/protodb"
)

type txKey struct{}

func C(ctx context.Context, tx *Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func From(ctx context.Context) (*Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(*Tx)
	return tx, ok
}

type Maybe struct {
	db *DB
}

func (m *Maybe) Tx(ctx context.Context, opts ...protodb.TxOption) (*Tx, error) {
	if t, ok := From(ctx); ok {
		return &Tx{txn: t.txn, nested: true}, nil
	}
	return m.db.Tx(ctx, opts...)
}

func (m *Maybe) WithTx(ctx context.Context, fn func(ctx context.Context, tx *Tx) error, opts ...protodb.TxOption) error {
	tx, err := m.Tx(ctx, opts...)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (m *Maybe) WithTx2[R any](ctx context.Context, fn func(ctx context.Context, tx *Tx) (R, error), opts ...protodb.TxOption) (R, error) {
	var o R
	tx, err := m.Tx(ctx, opts...)
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

func (m *Maybe) WithTx3[R1, R2 any](ctx context.Context, fn func(ctx context.Context, tx *Tx) (R1, R2, error), opts ...protodb.TxOption) (R1, R2, error) {
	var (
		o1 R1
		o2 R2
	)
	tx, err := m.Tx(ctx, opts...)
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

func (m *Maybe) WithTx4[R1, R2, R3 any](ctx context.Context, fn func(ctx context.Context, tx *Tx) (R1, R2, R3, error), opts ...protodb.TxOption) (R1, R2, R3, error) {
	var (
		o1 R1
		o2 R2
		o3 R3
	)
	tx, err := m.Tx(ctx, opts...)
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
