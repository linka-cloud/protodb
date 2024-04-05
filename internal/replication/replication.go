// Copyright 2023 Linka Cloud  All rights reserved.
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

package replication

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/grpc"

	"go.linka.cloud/protodb/internal/db/pending"
)

// MaxMsgSize is the maximum message size accepted by the gRPC server.
// It should be 4MiB, but we set it to 4MB to avoid exceeding the allowed size
const MaxMsgSize = 4 * 1000 * 1000

const (
	ModeNone Mode = iota
	ModeAsync
	ModeSync
)

type Mode uint8

func (m Mode) String() string {
	switch m {
	case ModeAsync:
		return "async"
	case ModeSync:
		return "sync"
	default:
		return "unknown"
	}
}

type Replication interface {
	IsLeader() bool
	LeaderConn() (grpc.ClientConnInterface, bool)
	CurrentLeader() string
	Subscribe() <-chan string
	LinearizableReads() bool
	NewTx(ctx context.Context) (Tx, error)
	Close() error
}
type Tx interface {
	Iterator(tx *badger.Txn, readTs uint64, opt badger.IteratorOptions) pending.Iterator
	New(ctx context.Context, readTs uint64) error
	Set(ctx context.Context, key, value []byte, expiresAt uint64) error
	Delete(ctx context.Context, key []byte) error
	Commit(ctx context.Context, at uint64) error
	Close(ctx context.Context) error
}
