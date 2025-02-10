// Copyright 2024 Linka Cloud  All rights reserved.
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

package badgerd

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"google.golang.org/grpc"

	"go.linka.cloud/protodb/internal/badgerd/pending"
)

type Iterator = pending.Iterator
type Item = pending.Item

type DB interface {
	View(fn func(tx *badger.Txn) error) error
	Subscribe(ctx context.Context, cb func(kv *badger.KVList) error, matches []pb.Match) error
	NewTransaction(ctx context.Context, update bool) (Tx, error)
	Replicated() bool
	IsLeader() bool
	Leader() string
	LeaderChanges() <-chan string
	LeaderConn() (grpc.ClientConnInterface, bool)
	LinearizableReads() bool
	Close() error
}

type Tx interface {
	ReadTs() uint64
	Iterator(opt badger.IteratorOptions) Iterator
	Set(ctx context.Context, key, value []byte, expiresAt uint64) error
	Get(ctx context.Context, key []byte) (Item, error)
	AddReadKey(key []byte)
	Delete(ctx context.Context, key []byte) error
	Commit(ctx context.Context) error
	Close(ctx context.Context) error
}
