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
	"encoding/binary"
	"errors"
	"fmt"
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

func newWal(path string, p *mem, size int64) *wal {
	var fp string
	for {
		n := counter.n.Add(1)
		fp = filepath.Join(path, fmt.Sprintf("tx-%04d.wal", n))
		if err := os.Remove(fp); os.IsNotExist(err) {
			break
		}
	}
	if size == 0 {
		size = 1024 * 1024
	}
	f, err := z.OpenMmapFile(fp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, int(size))
	if !errors.Is(err, z.NewFile) {
		y.Check(err)
	}
	if p == nil {
		return &wal{f: f, m: make(map[uint32]*pointer)}
	}
	w := &wal{f: f, m: make(map[uint32]*pointer, len(p.m)), addReadKey: p.addReadKey}
	for _, e := range p.m {
		y.Check(w.append(e))
	}
	p.m = make(map[string]*badger.Entry)
	return w
}

type wal struct {
	f          *z.MmapFile
	m          map[uint32]*pointer
	pos        int64
	addReadKey func(key []byte)
	o          sync.Once
}

func (w *wal) Iterator(prefix []byte, readTs uint64, reversed bool) Iterator {
	return w.newIterator(prefix, readTs, reversed)
}

func (w *wal) Set(e *badger.Entry) {
	y.Check(w.append(e))
}

func (w *wal) Delete(key []byte) {
	y.Check(w.append(&badger.Entry{
		Key:      key,
		UserMeta: bitDelete,
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
	p := &pointer{
		key:     e.Key,
		offset:  uint32(w.pos),
		len:     l,
		deleted: e.UserMeta&bitDelete > 0,
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
	w.m[y.Hash(e.Key)] = p
	w.pos = int64(pos)
	return nil
}

func (w *wal) read(key []byte) (*badger.Entry, error) {
	p, ok := w.m[y.Hash(key)]
	if !ok {
		return nil, badger.ErrKeyNotFound
	}
	buf := w.f.Slice(int(p.offset))
	var e walEntry
	e.h.decode(buf)
	e.k = buf[headerSize : headerSize+e.h.klen]
	e.v = buf[headerSize+e.h.klen : headerSize+e.h.klen+e.h.vlen]
	return &badger.Entry{
		Key:       e.k,
		Value:     e.v,
		UserMeta:  e.h.userMeta,
		ExpiresAt: e.h.expiresAt,
	}, nil
}

func (w *wal) Replay(fn func(e *badger.Entry) error) error {
	for _, v := range w.m {
		e, err := w.read(v.key)
		if err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) newIterator(prefix []byte, readTs uint64, reversed bool) Iterator {
	if len(w.m) == 0 {
		return nil
	}
	items := make([]*entry, 0, len(w.m))
	for _, v := range w.m {
		items = append(items, &entry{key: v.key, p: v})
	}
	sort.Slice(items, func(i, j int) bool {
		cmp := bytes.Compare(items[i].key, items[j].key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &walIterator{w: w, prefix: prefix, items: items, reversed: reversed, readTs: readTs, addReadKey: w.addReadKey}
}

type entry struct {
	key []byte
	p   *pointer
}

type walIterator struct {
	w          *wal
	prefix     []byte
	items      []*entry
	nextIdx    int
	readTs     uint64
	reversed   bool
	addReadKey func(key []byte)
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
	i.addReadKey(key)
	i.nextIdx = sort.Search(len(i.items), func(idx int) bool {
		cmp := bytes.Compare(i.items[idx].key, key)
		if !i.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (i *walIterator) Key() []byte {
	y.AssertTrue(i.Valid())
	return i.items[i.nextIdx].key
}

func (i *walIterator) Item() Item {
	y.AssertTrue(i.Valid())
	e := i.items[i.nextIdx]
	entry, err := i.w.read(e.key)
	i.addReadKey(e.key)
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
