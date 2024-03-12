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
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEntry(i int, size int64) *badger.Entry {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i+1))
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return &badger.Entry{
		Key:   k,
		Value: b,
	}
}

func genEntries(count int, size int64) []*badger.Entry {
	var entries []*badger.Entry
	for i := 0; i < count; i++ {
		entries = append(entries, newEntry(i, size))
	}
	return entries
}

func TestWal(t *testing.T) {
	w := newWal(os.TempDir(), nil)
	w.addReadKey = func(key []byte) {}
	defer w.Close()

	const count = 1_000

	entries := genEntries(count, 1024*1024)

	e := entries[0]
	s := uint32(headerSize + len(e.Key) + len(e.Value))
	t.Run("Set", func(t *testing.T) {
		w.Set(e)
		assert.Len(t, w.m, 1)
		assert.Equal(t, &pointer{key: e.Key, len: s}, w.m[y.Hash(e.Key)])
		require.NoError(t, w.Replay(func(e *badger.Entry) error {
			assert.Equal(t, entries[0].Key, e.Key)
			assert.Equal(t, entries[0].Value, e.Value)
			return nil
		}))
	})

	t.Run("Delete", func(t *testing.T) {
		w.Delete(e.Key)
		assert.Equal(t, &pointer{key: e.Key, offset: s, len: uint32(headerSize + len(e.Key)), deleted: true}, w.m[y.Hash(e.Key)])
		require.NoError(t, w.Replay(func(e *badger.Entry) error {
			assert.Equal(t, entries[0].Key, e.Key)
			assert.Equal(t, bitDelete, e.UserMeta)
			assert.Empty(t, e.Value)
			return nil
		}))
	})

	t.Run(fmt.Sprintf("Batch insert %d entries and iterate", count), func(t *testing.T) {
		for _, v := range entries {
			w.Set(v)
		}

		var g []*badger.Entry
		require.NoError(t, w.Replay(func(e *badger.Entry) error {
			g = append(g, e)
			return nil
		}))

		sort.Slice(g, func(i, j int) bool {
			return bytes.Compare(g[i].Key, g[j].Key) < 0
		})
		assert.Equal(t, entries, g)

		it := w.Iterator(0, false)
		defer it.Close()

		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Item()
			assert.Equalf(t, entries[i].Key, v.Key(), "key mismatch")
			var b []byte
			require.NoError(t, v.Value(func(val []byte) error {
				b = append(b, val...)
				return nil
			}))
			assert.Equalf(t, entries[i].Value, b, "value mismatch")
			assert.Equalf(t, entries[i].ExpiresAt, v.ExpiresAt(), "expiresAt mismatch")
			i++
		}
		assert.Equal(t, count, i)
	})
	t.Run(fmt.Sprintf("Batch delete %d entries and iterate", count), func(t *testing.T) {
		it := w.Iterator(0, false)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			w.Delete(it.Item().Key())
		}
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Item()
			assert.Equalf(t, entries[i].Key, v.Key(), "key mismatch")
			var b []byte
			require.NoError(t, v.Value(func(val []byte) error {
				b = append(b, val...)
				return nil
			}))
			assert.Emptyf(t, b, "should be empty")
			assert.Equalf(t, entries[i].ExpiresAt, v.ExpiresAt(), "expiresAt mismatch")
			assert.Truef(t, v.IsDeletedOrExpired(), "expected deleted")
			i++
		}
		assert.Equal(t, count, i)
	})

	t.Run("Close", func(t *testing.T) {

		require.NoError(t, w.Close())

		_, err := os.Stat(w.f.Name())
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
}
