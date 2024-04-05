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

	"go.linka.cloud/protodb/internal/db/pending"
)

var _ Tx = (*Maybe)(nil)

type Maybe struct {
	Tx
	DB
	readTs uint64
	w      pending.IterableMergedWrites
}

func (s *Maybe) Iterator(tx *badger.Txn, readTs uint64, opt badger.IteratorOptions) pending.Iterator {
	if s.Tx != nil {
		return s.Tx.Iterator(tx, readTs, opt)
	}
	return s.w.MergedIterator(tx, readTs, opt)
}

func (s *Maybe) New(ctx context.Context, readTs uint64) error {
	if s.Tx != nil {
		return s.Tx.New(ctx, readTs)
	}
	s.readTs = readTs
	s.w = pending.New(s.DB.Path(), s.DB.MaxBatchCount(), s.DB.MaxBatchSize(), int(s.DB.ValueThreshold()), func(key []byte) {})
	return nil
}

func (s *Maybe) Set(ctx context.Context, key, value []byte, expiresAt uint64) error {
	if s.Tx != nil {
		return s.Tx.Set(ctx, key, value, expiresAt)
	}
	s.w.Set(&badger.Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	})
	return nil
}

func (s *Maybe) Delete(ctx context.Context, key []byte) error {
	if s.Tx != nil {
		return s.Tx.Delete(ctx, key)
	}
	s.w.Delete(key)
	return nil
}

func (s *Maybe) Commit(ctx context.Context, at uint64) error {
	if s.Tx != nil {
		return s.Tx.Commit(ctx, at)
	}
	b := s.DB.NewWriteBatchAt(s.readTs)
	defer b.Cancel()
	if err := s.w.Replay(func(e *badger.Entry) error {
		if e.UserMeta != 0 {
			return b.DeleteAt(e.Key, at)
		}
		return b.SetEntryAt(e, at)
	}); err != nil {
		return err
	}
	return b.Flush()
}

func (s *Maybe) Close(ctx context.Context) error {
	if s.Tx != nil {
		return s.Tx.Close(ctx)
	}
	if s.w != nil {
		return s.w.Close()
	}
	return nil
}
