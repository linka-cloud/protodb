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

package maybe

import (
	"context"

	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/typed"
)

type message[T any] interface {
	proto.Message
	*T
}

func TypedTxFrom[T any, PT message[T]](ctx context.Context, db protodb.Client) (typed.Tx[T, PT], error) {
	tx, err := Tx(ctx, db)
	if err != nil {
		return nil, err
	}
	return typed.NewTx[T, PT](tx), nil
}

func WithTypedTx[T any, PT message[T]](ctx context.Context, db protodb.Client, fn func(ctx context.Context, tx typed.Tx[T, PT]) error) error {
	tx, err := TypedTxFrom[T, PT](ctx, db)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func WithTypedTx2[T any, R any, PT message[T]](ctx context.Context, db protodb.Client, fn func(ctx context.Context, tx typed.Tx[T, PT]) (R, error)) (R, error) {
	var v R
	tx, err := TypedTxFrom[T, PT](ctx, db)
	if err != nil {
		return v, err
	}
	defer tx.Close()
	if v, err = fn(ctx, tx); err != nil {
		return v, err
	}
	return v, tx.Commit(ctx)
}

func WithTypedTx3[T, R1, R2 any, PT message[T]](ctx context.Context, db protodb.Client, fn func(ctx context.Context, tx typed.Tx[T, PT]) (R1, R2, error)) (R1, R2, error) {
	var (
		v1 R1
		v2 R2
	)
	tx, err := TypedTxFrom[T, PT](ctx, db)
	if err != nil {
		return v1, v2, err
	}
	defer tx.Close()
	if v1, v2, err = fn(ctx, tx); err != nil {
		return v1, v2, err
	}
	return v1, v2, tx.Commit(ctx)
}

func WithTypedTx4[T, R1, R2, R3 any, PT message[T]](ctx context.Context, db protodb.Client, fn func(ctx context.Context, tx typed.Tx[T, PT]) (R1, R2, R3, error)) (R1, R2, R3, error) {
	var (
		v1 R1
		v2 R2
		v3 R3
	)
	tx, err := TypedTxFrom[T, PT](ctx, db)
	if err != nil {
		return v1, v2, v3, err
	}
	defer tx.Close()
	if v1, v2, v3, err = fn(ctx, tx); err != nil {
		return v1, v2, v3, err
	}
	return v1, v2, v3, tx.Commit(ctx)
}
