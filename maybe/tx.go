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

	"go.linka.cloud/protodb"
)

var _ protodb.Tx = (*tx)(nil)

func Tx(ctx context.Context, db protodb.Client) (protodb.Tx, error) {
	if t, ok := ctx.Value(txKey{}).(protodb.Tx); ok {
		return &tx{Tx: t}, nil
	}
	return db.Tx(ctx)
}

type tx struct {
	protodb.Tx
}

func (t *tx) Commit(_ context.Context) error {
	return nil
}

func (t *tx) Close() {}
