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

type Tx struct {
	txn    protodb.Tx
	nested bool
}

func (t *Tx) Raw() protodb.Tx {
	return t.txn
}

func (t *Tx) Get[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.GetOption) ([]PT, *protodb.PagingInfo, error) {
	return getTyped[T, PT](ctx, t.txn, m, opts...)
}

func (t *Tx) GetOne[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.GetOption) (PT, bool, error) {
	return getOneTyped[T, PT](ctx, t.txn, m, opts...)
}

func (t *Tx) Set[T any, PT Message[T]](ctx context.Context, m PT, opts ...protodb.SetOption) (PT, error) {
	return setTyped[T, PT](ctx, t.txn, m, opts...)
}

func (t *Tx) Delete[T any, PT Message[T]](ctx context.Context, m PT) error {
	return deleteTyped[T, PT](ctx, t.txn, m)
}

func (t *Tx) Commit(ctx context.Context) error {
	if t.nested {
		return nil
	}
	return t.txn.Commit(ctx)
}

func (t *Tx) Close() {
	if t.nested {
		return
	}
	t.txn.Close()
}
