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

func TestWrites(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	const count = 1_000
	entries := genEntries(count, 1024*1024)

	w := newWrites(db.Opts().Dir, nil, db.MaxBatchCount(), db.MaxBatchSize(), int(db.Opts().ValueThreshold), nil)
	defer w.Close()

	for _, v := range entries {
		ok := w.checkSize(v)
		if ok {
			assert.Nil(t, w.w)
		}
		w.Set(v)
		if !ok {
			assert.Empty(t, w.m.m)
			assert.NotNil(t, w.w)
		}
	}
	it := w.Iterator(nil, false)
	defer it.Close()
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var b []byte
		err := item.Value(func(val []byte) error {
			b = append([]byte{}, val...)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, entries[i].Key, item.Key())
		assert.Equal(t, entries[i].Value, b)
		i++
	}
	assert.Equal(t, count, i)
}
