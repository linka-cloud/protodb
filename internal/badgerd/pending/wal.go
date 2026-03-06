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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
)

var counter = struct {
	n atomic.Uint64
}{}

type pointer struct {
	key     []byte
	offset  uint32
	len     uint32
	deleted bool
}

const headerSize = 1 + 4 + 4 + 8

type header struct {
	userMeta  byte
	klen      uint32
	vlen      uint32
	expiresAt uint64
}

type walEntry struct {
	h header
	k []byte
	v []byte
}

func (h *header) encode(out []byte) {
	out[0] = h.userMeta
	binary.BigEndian.PutUint32(out[1:], h.klen)
	binary.BigEndian.PutUint32(out[5:], h.vlen)
	binary.BigEndian.PutUint64(out[9:], h.expiresAt)
	return
}

// decode decodes the given header from the provided byte slice.
// Returns the number of bytes read.
func (h *header) decode(buf []byte) {
	h.userMeta = buf[0]
	h.klen = binary.BigEndian.Uint32(buf[1:])
	h.vlen = binary.BigEndian.Uint32(buf[5:])
	h.expiresAt = binary.BigEndian.Uint64(buf[9:])
}

func newWal(path string, tx *badger.Txn, m *mem, size int64) *wal {
	var fp string
	for {
		n := counter.n.Add(1)
		fp = filepath.Join(path, fmt.Sprintf("tx-%04d.wal", n))
		if err := os.Remove(fp); os.IsNotExist(err) {
			break
		}
	}
	f, err := z.OpenMmapFile(fp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, walInitSize(size))
	if !errors.Is(err, z.NewFile) {
		y.Check(err)
	}
	if m == nil {
		return &wal{f: f, m: make(map[uint64]pointer), dirty: true}
	}
	w := &wal{f: f, tx: tx, m: make(map[uint64]pointer, len(m.m)), dirty: true}
	for _, e := range m.m {
		y.Check(w.append(e))
	}
	m.m = make(map[uint64]*badger.Entry)
	return w
}

func walInitSize(max int64) int {
	const min = 1 << 20
	const cap = 4 << 20
	if max <= 0 {
		return min
	}
	if max < min {
		return int(max)
	}
	if max < cap {
		return int(max)
	}
	return cap
}

type wal struct {
	tx  *badger.Txn
	f   *z.MmapFile
	m   map[uint64]pointer
	pos int64
	o   sync.Once

	asc   []*entry
	desc  []*entry
	dirty bool
}

func (w *wal) Iterator(prefix []byte, reversed bool) Iterator {
	if w.tx == nil {
		return w.newIterator(prefix, math.MaxUint64, reversed)
	}
	return newMergeIterator(
		&txIterator{
			prefix: prefix,
			i:      w.tx.NewIterator(badger.IteratorOptions{Prefix: prefix, Reverse: reversed}),
		},
		w.newIterator(prefix, w.tx.ReadTs(), reversed),
		reversed,
	)
}

func (w *wal) Get(key []byte) (Item, error) {
	p, ok := w.m[z.MemHash(key)]
	if !ok {
		if w.tx == nil {
			return nil, badger.ErrKeyNotFound
		}
		return w.tx.Get(key)
	}
	if p.deleted {
		return nil, badger.ErrKeyNotFound
	}
	e, err := w.readPtr(p, false)
	if err != nil {
		return nil, err
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

func (w *wal) Set(e *badger.Entry) {
	y.Check(w.append(e))
}

func (w *wal) Delete(key []byte) {
	y.Check(w.append(&badger.Entry{
		Key:      key,
		UserMeta: BitDelete,
	}))
}

func (w *wal) Close() error {
	var err error
	w.o.Do(func() {
		err = w.f.Delete()
	})
	return err
}

func (w *wal) append(e *badger.Entry) error {
	l := uint32(headerSize + len(e.Key) + len(e.Value))
	buf, pos, err := w.f.AllocateSlice(int(l), int(w.pos))
	if err != nil {
		return err
	}
	p := pointer{
		key:     e.Key,
		offset:  uint32(w.pos),
		len:     l,
		deleted: e.UserMeta&BitDelete > 0,
	}
	h := header{
		userMeta:  e.UserMeta,
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
	}
	h.encode(buf)
	copy(buf[headerSize:], e.Key)
	copy(buf[headerSize+int(h.klen):], e.Value)
	w.m[z.MemHash(e.Key)] = p
	w.dirty = true
	w.pos = int64(pos)
	return nil
}

func (w *wal) read(key []byte, cpy bool) (*badger.Entry, error) {
	p, ok := w.m[z.MemHash(key)]
	if !ok {
		return nil, badger.ErrKeyNotFound
	}
	return w.readPtr(p, cpy)
}

func (w *wal) readPtrRaw(p pointer) (k, v []byte, userMeta byte, expiresAt uint64) {
	buf := w.f.Slice(int(p.offset))
	var h header
	h.decode(buf)
	k = buf[headerSize : headerSize+h.klen]
	v = buf[headerSize+h.klen : headerSize+h.klen+h.vlen]
	return k, v, h.userMeta, h.expiresAt
}

func (w *wal) readPtr(p pointer, cpy bool) (*badger.Entry, error) {
	k, v, userMeta, expiresAt := w.readPtrRaw(p)
	if cpy {
		k = y.SafeCopy(nil, k)
		v = y.SafeCopy(nil, v)
	}
	return &badger.Entry{
		Key:       k,
		Value:     v,
		UserMeta:  userMeta,
		ExpiresAt: expiresAt,
	}, nil
}

func (w *wal) ReplayRaw(fn func(key, value []byte, userMeta byte, expiresAt uint64) error) error {
	for _, p := range w.m {
		k, v, userMeta, expiresAt := w.readPtrRaw(p)
		if err := fn(k, v, userMeta, expiresAt); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) Replay(fn func(e *badger.Entry) error) error {
	return w.ReplayRaw(func(key, value []byte, userMeta byte, expiresAt uint64) error {
		e := &badger.Entry{Key: y.SafeCopy(nil, key), Value: y.SafeCopy(nil, value), UserMeta: userMeta, ExpiresAt: expiresAt}
		if err := fn(e); err != nil {
			return err
		}
		return nil
	})
}

func (w *wal) newIterator(prefix []byte, readTs uint64, reversed bool) iterator {
	items := w.sortedEntries(reversed)
	if len(items) == 0 {
		return nil
	}
	return &walIterator{w: w, prefix: prefix, items: items, reversed: reversed, readTs: readTs}
}

func (w *wal) sortedEntries(reversed bool) []*entry {
	if w.dirty {
		w.rebuildEntries()
	}
	if reversed {
		return w.desc
	}
	return w.asc
}

func (w *wal) rebuildEntries() {
	w.asc = make([]*entry, 0, len(w.m))
	for h, p := range w.m {
		w.asc = append(w.asc, &entry{h: h, key: p.key, p: p})
	}
	sort.Slice(w.asc, func(i, j int) bool {
		return bytes.Compare(w.asc[i].key, w.asc[j].key) < 0
	})
	w.desc = make([]*entry, len(w.asc))
	copy(w.desc, w.asc)
	for i, j := 0, len(w.desc)-1; i < j; i, j = i+1, j-1 {
		w.desc[i], w.desc[j] = w.desc[j], w.desc[i]
	}
	w.dirty = false
}

type entry struct {
	h   uint64
	key []byte
	p   pointer
}

type walIterator struct {
	w        *wal
	prefix   []byte
	items    []*entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (i *walIterator) Next() {
	i.nextIdx++
}

func (i *walIterator) skip() bool {
	return !i.items[i.nextIdx].p.deleted || !bytes.HasPrefix(i.items[i.nextIdx].key, i.prefix)
}

func (i *walIterator) Rewind() {
	i.nextIdx = 0
}

func (i *walIterator) Seek(key []byte) {
	i.seek(key)
}

func (i *walIterator) seek(key []byte) {
	i.nextIdx = sort.Search(len(i.items), func(idx int) bool {
		cmp := bytes.Compare(i.items[idx].key, key)
		if !i.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (i *walIterator) SeekLast() {
	seekLast(i, i.prefix)
}

func (i *walIterator) Key() []byte {
	y.AssertTrue(i.Valid())
	return i.items[i.nextIdx].key
}

func (i *walIterator) Item() Item {
	y.AssertTrue(i.Valid())
	e := i.items[i.nextIdx]
	p, ok := i.w.m[e.h]
	if !ok {
		p = e.p
	}
	entry, err := i.w.readPtr(p, false)
	y.Check(err)
	return &item{
		readTs: i.readTs,
		e: &badger.Entry{
			Key:       entry.Key,
			Value:     entry.Value,
			UserMeta:  entry.UserMeta,
			ExpiresAt: entry.ExpiresAt,
		},
	}
}

func (i *walIterator) Valid() bool {
	return i.nextIdx < len(i.items)
}

func (i *walIterator) Close() {
}
