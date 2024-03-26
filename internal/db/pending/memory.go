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

package pending

import (
	"bytes"
	"sort"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
)

const bitDelete byte = 1 << 0 // Set if the key has been deleted.

func newMem(addReadKey func(key []byte)) *mem {
	return &mem{
		m:          make(map[string]*badger.Entry),
		addReadKey: addReadKey,
	}
}

type mem struct {
	m          map[string]*badger.Entry
	addReadKey func(key []byte)
}

func (m *mem) Iterator(prefix []byte, readTs uint64, reversed bool) Iterator {
	if m.empty() {
		return &memIterator{
			prefix:     prefix,
			readTs:     readTs,
			reversed:   reversed,
			addReadKey: m.addReadKey,
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
		prefix:     prefix,
		readTs:     readTs,
		entries:    entries,
		reversed:   reversed,
		addReadKey: m.addReadKey,
	}
}

func (p *mem) Set(e *badger.Entry) {
	p.m[string(e.Key)] = e
}

func (p *mem) Delete(key []byte) {
	p.m[string(key)] = &badger.Entry{
		Key:      key,
		UserMeta: bitDelete,
	}
}

func (p *mem) Replay(fn func(e *badger.Entry) error) error {
	for _, e := range p.m {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (p *mem) Close() error {
	return nil
}

func (p *mem) slice() []*badger.Entry {
	entries := make([]*badger.Entry, 0, len(p.m))
	for _, e := range p.m {
		entries = append(entries, e)
	}
	return entries
}

func (p *mem) empty() bool {
	return len(p.m) == 0
}

type memIterator struct {
	prefix     []byte
	entries    []*badger.Entry
	nextIdx    int
	readTs     uint64
	reversed   bool
	addReadKey func(key []byte)
}

func (i *memIterator) Next() {
	i.nextIdx++
}

func (i *memIterator) skip() bool {
	return i.entries[i.nextIdx].UserMeta&bitDelete != 0 || !bytes.HasPrefix(i.entries[i.nextIdx].Key, i.prefix)
}

func (i *memIterator) Rewind() {
	i.nextIdx = 0
}

func (i *memIterator) Seek(key []byte) {
	i.addReadKey(key)
	i.nextIdx = sort.Search(len(i.entries), func(idx int) bool {
		cmp := bytes.Compare(i.entries[idx].Key, key)
		if !i.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (i *memIterator) Key() []byte {
	y.AssertTrue(i.Valid())
	return i.entries[i.nextIdx].Key
}

func (i *memIterator) Item() Item {
	y.AssertTrue(i.Valid())
	entry := i.entries[i.nextIdx]
	i.addReadKey(entry.Key)
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
