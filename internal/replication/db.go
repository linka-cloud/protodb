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
	"io"

	"github.com/dgraph-io/badger/v3"

	"go.linka.cloud/protodb/internal/protodb"
)

type DB interface {
	protodb.DB
	Path() string
	InMemory() bool
	MaxVersion() uint64
	SetVersion(v uint64)
	Load(ctx context.Context, r io.Reader) error
	Stream(ctx context.Context, at, since uint64, w io.Writer) error
	LoadDescriptors(ctx context.Context) error
	NewWriteBatchAt(readTs uint64) *badger.WriteBatch

	ValueThreshold() int64
	MaxBatchCount() int64
	MaxBatchSize() int64
	Close() error
}
