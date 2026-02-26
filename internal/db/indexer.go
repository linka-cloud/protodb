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

package db

import (
	"context"

	"google.golang.org/protobuf/reflect/protoreflect"

	"go.linka.cloud/protodb/internal/protodb"
)

func (db *db) rebuildIndexes(ctx context.Context, files ...protoreflect.FileDescriptor) error {
	_, ok, err := db.maybeProxy(false)
	if err != nil {
		return err
	}
	if ok {
		return protodb.ErrNotLeader
	}
	tx, err := newTx(ctx, db)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := db.idx.RebuildIfNeeded(ctx, tx, files...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
