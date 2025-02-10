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
	"math"
	"sort"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
)

const BitDelete byte = 1 << 0 // Set if the key has been deleted.

func newMem(tx *badger.Txn) *mem {
	return &mem{
		tx: tx,
		m:  make(map[string]*badger.Entry),
	}
}

type mem struct {
	tx *badger.Txn
	m  map[string]*badger.Entry
}

func (m *mem) Iterator(prefix []byte, reversed bool) Iterator {
	if m.tx == nil {
		return m.newIterator(prefix, math.MaxUint64, reversed)
	}
	return newMergeIterator(
		&txIterator{
			prefix: prefix,
			i:      m.tx.NewIterator(badger.IteratorOptions{Prefix: prefix, Reverse: reversed}),
		},
		m.newIterator(prefix, m.tx.ReadTs(), reversed),
		reversed,
	)
}
func (m *mem) newIterator(prefix []byte, readTs uint64, reversed bool) iterator {
	if m.empty() {
		return &memIterator{
			prefix:   prefix,
			readTs:   readTs,
			reversed: reversed,
		}
	}
	entries := m.slice()
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &memIterator{
		prefix:   prefix,
		readTs:   readTs,
		entries:  entries,
		reversed: reversed,
	}
}

func (m *mem) Get(key []byte) (Item, error) {
	e, ok := m.m[string(key)]
	if !ok {
		if m.tx == nil {
			return nil, badger.ErrKeyNotFound
		}
		return m.tx.Get(key)
	}
	if e.UserMeta&BitDelete != 0 {
		return nil, badger.ErrKeyNotFound
	}
	return &item{
		readTs: 0,
		e: &badger.Entry{
			Key:       e.Key,
			Value:     e.Value,
			ExpiresAt: e.ExpiresAt,
			UserMeta:  e.UserMeta,
		},
	}, nil
}

func (m *mem) Set(e *badger.Entry) {
	m.m[string(e.Key)] = e
}

func (m *mem) Delete(key []byte) {
	m.m[string(key)] = &badger.Entry{
		Key:      key,
		UserMeta: BitDelete,
	}
}

func (m *mem) Replay(fn func(e *badger.Entry) error) error {
	for _, e := range m.m {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (m *mem) Close() error {
	return nil
}

func (m *mem) slice() []*badger.Entry {
	entries := make([]*badger.Entry, 0, len(m.m))
	for _, e := range m.m {
		entries = append(entries, e)
	}
	return entries
}

func (m *mem) empty() bool {
	return len(m.m) == 0
}

type memIterator struct {
	prefix   []byte
	entries  []*badger.Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (i *memIterator) Next() {
	i.nextIdx++
}

func (i *memIterator) skip() bool {
	return i.entries[i.nextIdx].UserMeta&BitDelete != 0 || !bytes.HasPrefix(i.entries[i.nextIdx].Key, i.prefix)
}

func (i *memIterator) Rewind() {
	i.nextIdx = 0
}

func (i *memIterator) Seek(key []byte) {
	i.seek(key)
}

func (i *memIterator) seek(key []byte) {
	i.nextIdx = sort.Search(len(i.entries), func(idx int) bool {
		cmp := bytes.Compare(i.entries[idx].Key, key)
		if !i.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (i *memIterator) SeekLast() {
	seekLast(i, i.prefix)
}

func (i *memIterator) Key() []byte {
	y.AssertTrue(i.Valid())
	return i.entries[i.nextIdx].Key
}

func (i *memIterator) Item() Item {
	y.AssertTrue(i.Valid())
	entry := i.entries[i.nextIdx]
	return &item{
		readTs: i.readTs,
		e: &badger.Entry{
			Key:       entry.Key,
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
			UserMeta:  entry.UserMeta,
		},
	}
}

func (i *memIterator) Valid() bool {
	return i.nextIdx < len(i.entries)
}

func (i *memIterator) Close() {}
