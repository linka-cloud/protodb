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
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	const count = 100
	initial := genEntries(count, 1024)
	entries := genEntries(count, 2048)

	tx := db.NewTransaction(true)
	for _, v := range initial {
		require.NoError(t, tx.Set(v.Key, v.Value))
	}
	require.NoError(t, tx.Commit())
	tx.Discard()

	tx = db.NewTransaction(false)
	defer tx.Discard()

	w := NewWithDB(db, tx, nil)
	defer w.Close()

	t.Run("no pending writes", func(t *testing.T) {
		it := w.Iterator(nil, false)
		defer it.Close()

		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var b []byte
			require.NoError(t, item.Value(func(val []byte) error {
				b = append([]byte{}, val...)
				return nil
			}))
			assert.Equal(t, initial[i].Key, item.Key())
			assert.Equal(t, initial[i].Value, b)
			i++
		}
	})

	for i := 0; i < count; i += 2 {
		w.Set(entries[i])
	}

	t.Run("with pending writes", func(t *testing.T) {
		it := w.Iterator(nil, false)
		defer it.Close()
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var b []byte
			require.NoError(t, item.Value(func(val []byte) error {
				b = append([]byte{}, val...)
				return nil
			}))
			var ss []*badger.Entry
			if i%2 == 0 {
				ss = entries
			} else {
				ss = initial
			}
			require.Less(t, i, len(ss))
			assert.Equal(t, ss[i].Key, item.Key())
			assert.Equal(t, ss[i].Value, b)
			i++
		}
		assert.Equal(t, count, i)
	})

	t.Run("start outside ou pending writes", func(t *testing.T) {
		it := w.Iterator([]byte("zzz"), false)
		defer it.Close()
		it.Rewind()
		assert.False(t, it.Valid())
	})

	t.Run("reverse", func(t *testing.T) {
		it := w.Iterator(nil, true)
		defer it.Close()
		i := len(entries) - 1
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var b []byte
			require.NoError(t, item.Value(func(val []byte) error {
				b = append([]byte{}, val...)
				return nil
			}))
			var ss []*badger.Entry
			if i%2 == 0 {
				ss = entries
			} else {
				ss = initial
			}
			require.Less(t, i, len(ss))
			assert.Equal(t, ss[i].Key, item.Key())
			assert.Equal(t, ss[i].Value, b)
			i--
		}
		assert.Equal(t, -1, i)
	})

	for i := 0; i < count; i += 2 {
		w.Delete(entries[i].Key)
	}

	t.Run("with pending deletes", func(t *testing.T) {
		it := w.Iterator(nil, false)
		defer it.Close()
		i := 1
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var b []byte
			require.NoError(t, item.Value(func(val []byte) error {
				b = append([]byte{}, val...)
				return nil
			}))
			require.Less(t, i, len(entries))
			assert.Equal(t, initial[i].Key, item.Key())
			assert.Equal(t, initial[i].Value, b)
			i += 2
		}
		assert.Equal(t, count+1, i)
	})

	t.Run("with pending deletes reverse", func(t *testing.T) {
		it := w.Iterator(nil, true)
		defer it.Close()
		i := len(entries) - 1
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var b []byte
			require.NoError(t, item.Value(func(val []byte) error {
				b = append([]byte{}, val...)
				return nil
			}))
			require.Less(t, i, len(initial))
			assert.Equal(t, initial[i].Key, item.Key())
			assert.Equal(t, initial[i].Value, b)
			i -= 2
		}
		assert.Equal(t, -1, i)
	})

	for i := 0; i < count; i++ {
		w.Delete(initial[i].Key)
	}

	t.Run("pending deletes only", func(t *testing.T) {
		it := w.Iterator(nil, false)
		defer it.Close()
		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			i++
		}
		assert.Equal(t, 0, i)
	})
}
