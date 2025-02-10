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

package pending

import (
	"github.com/dgraph-io/badger/v3"
)

type Writes interface {
	Iterator(prefix []byte, reversed bool) Iterator
	Get(key []byte) (Item, error)
	Set(e *badger.Entry)
	Delete(key []byte)
	Replay(fn func(e *badger.Entry) error) error
	Close() error
}

func NewWithDB(db *badger.DB, tx *badger.Txn) Writes {
	return newWrites(db.Opts().Dir, tx, db.MaxBatchCount(), db.MaxBatchSize(), int(db.Opts().ValueThreshold))
}

func New(path string, tx *badger.Txn, maxCount, maxSize int64, threshold int) Writes {
	return newWrites(path, tx, maxCount, maxSize, threshold)
}

func newWrites(path string, tx *badger.Txn, maxCount, maxSize int64, threshold int) *writes {
	w := newMem(tx)
	return &writes{
		path:           path,
		tx:             tx,
		m:              w,
		c:              w,
		maxCount:       maxCount,
		maxSize:        maxSize,
		valueThreshold: threshold,
	}
}

type writes struct {
	path string

	tx *badger.Txn

	m *mem
	w *wal
	c Writes

	count int64
	size  int64

	maxCount       int64
	maxSize        int64
	valueThreshold int
}

func (w *writes) Iterator(prefix []byte, reversed bool) Iterator {
	return w.c.Iterator(prefix, reversed)
}

func (w *writes) Get(key []byte) (Item, error) {
	return w.c.Get(key)
}

func (w *writes) Set(e *badger.Entry) {
	if w.w == nil && !w.checkSize(e) {
		w.w = newWal(w.path, w.tx, w.m, w.maxSize)
		w.c = w.w
	}
	w.c.Set(e)
}

func (w *writes) Delete(key []byte) {
	e := &badger.Entry{
		Key: key,
	}
	if w.w == nil && !w.checkSize(e) {
		w.w = newWal(w.path, w.tx, w.m, w.maxSize)
		w.c = w.w
	}
	w.c.Delete(key)
}

func (w *writes) Replay(fn func(e *badger.Entry) error) error {
	return w.c.Replay(fn)
}

func (w *writes) Close() error {
	return w.c.Close()
}

func (w *writes) checkSize(e *badger.Entry) bool {
	count := w.count + 1
	size := w.size + int64(estimateSize(e, w.valueThreshold))
	if count >= w.maxCount || size >= w.maxSize {
		return false
	}
	w.count, w.size = count, size
	return true
}

func estimateSize(e *badger.Entry, threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}
	return len(e.Key) + 12 + 2 // 12 for ValuePointer, 2 for metas.
}
