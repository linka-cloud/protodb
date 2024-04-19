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
	"bytes"

	"github.com/dgraph-io/badger/v3"
)

type Iterator interface {
	Rewind()
	Next()
	Valid() bool
	Seek(key []byte)
	SeekLast()
	Key() []byte
	Item() Item
	Close()
}

type Item interface {
	Key() []byte
	KeyCopy(dst []byte) []byte
	Value(fn func(val []byte) error) error
	ValueCopy(dst []byte) ([]byte, error)
	IsDeletedOrExpired() bool
	ExpiresAt() uint64
	Version() uint64
}

// func TxIterator(i *badger.Iterator, addReadKey ReadTracker) Iterator {
// 	return &txIterator{i: i, addReadKey: addReadKey}
// }

type iterator interface {
	Iterator
	skip() bool
	seek(key []byte)
}

type txIterator struct {
	prefix     []byte
	i          *badger.Iterator
	addReadKey ReadTracker
}

func (t *txIterator) Next() {
	t.i.Next()
}

func (t *txIterator) Seek(key []byte) {
	t.addReadKey(key)
	t.i.Seek(key)
}

func (t *txIterator) seek(key []byte) {
	t.i.Seek(key)
}

func (t *txIterator) SeekLast() {
	seekLast(t, t.prefix)
}

func (t *txIterator) Rewind() {
	t.i.Rewind()
}

func (t *txIterator) Valid() bool {
	return t.i.Valid()
}

func (t *txIterator) Key() []byte {
	return t.i.Item().Key()
}

func (t *txIterator) Item() Item {
	item := t.i.Item()
	t.addReadKey(item.Key())
	return item
}

func (t *txIterator) skip() bool {
	return t.i.Item().IsDeletedOrExpired()
}

func (t *txIterator) Close() {
	t.i.Close()
}

func incrementPrefix(prefix []byte) []byte {
	result := make([]byte, len(prefix))
	copy(result, prefix)
	var len = len(prefix)
	for len > 0 {
		if result[len-1] != 0xFF {
			result[len-1] += 1
			break
		}
		len -= 1
	}
	return result[0:len]
}

func seekLast(it iterator, prefix []byte) {
	i := incrementPrefix(prefix)
	it.Seek(i)
	if it.Valid() && bytes.Equal(i, it.Item().Key()) {
		it.Next()
	}
}
