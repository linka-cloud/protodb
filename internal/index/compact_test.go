// Copyright 2026 Linka Cloud  All rights reserved.
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

package index

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/index/bitmap"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.linka.cloud/protodb/internal/badgerd"
)

func TestCompactDeltasAppliesAndRemoves(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	fieldPath := "1"
	encoded := []byte("ready")
	baseKey := shardKey(name, fieldPath, encoded, 0)
	addKey := deltaKey(name, fieldPath, encoded, 0, 3)
	removeKey := deltaKey(name, fieldPath, encoded, 0, 1)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	bm := bitmap.NewWith(2)
	bm.Set(1)
	bm.Set(2)
	require.NoError(t, tx.Set(ctx, baseKey, bm.Bytes(), 0))
	require.NoError(t, tx.Set(ctx, addKey, []byte{deltaAdd}, 0))
	require.NoError(t, tx.Set(ctx, removeKey, []byte{deltaRemove}, 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	processed, err := CompactDeltas(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 2, processed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	bm2, err := readBitmapTx(view, baseKey)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{2, 3}, bitmapValues(bm2))
	require.False(t, keyExists(t, ctx, view, addKey))
	require.False(t, keyExists(t, ctx, view, removeKey))
}

func TestCompactDeltasRespectsLimit(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	fieldPath := "1"
	encoded := []byte("ready")
	baseKey := shardKey(name, fieldPath, encoded, 0)
	d1 := deltaKey(name, fieldPath, encoded, 0, 1)
	d2 := deltaKey(name, fieldPath, encoded, 0, 2)
	d3 := deltaKey(name, fieldPath, encoded, 0, 3)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	require.NoError(t, tx.Set(ctx, d1, []byte{deltaAdd}, 0))
	require.NoError(t, tx.Set(ctx, d2, []byte{deltaAdd}, 0))
	require.NoError(t, tx.Set(ctx, d3, []byte{deltaAdd}, 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	processed, err := CompactDeltas(ctx, db, 1)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	bm, err := readBitmapTx(view, baseKey)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, bitmapValues(bm))
	require.Equal(t, 2, countPrefix(ctx, t, view, []byte("_index_delta/")))
	require.NoError(t, view.Close(ctx))

	processed, err = CompactDeltas(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 2, processed)

	view, err = db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	bm, err = readBitmapTx(view, baseKey)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2, 3}, bitmapValues(bm))
	require.Equal(t, 0, countPrefix(ctx, t, view, []byte("_index_delta/")))
}

func TestCompactDeltasDeletesEmptyBase(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	fieldPath := "1"
	encoded := []byte("ready")
	baseKey := shardKey(name, fieldPath, encoded, 0)
	removeKey := deltaKey(name, fieldPath, encoded, 0, 7)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	bm := bitmap.NewWith(1)
	bm.Set(7)
	require.NoError(t, tx.Set(ctx, baseKey, bm.Bytes(), 0))
	require.NoError(t, tx.Set(ctx, removeKey, []byte{deltaRemove}, 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	processed, err := CompactDeltas(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	require.False(t, keyExists(t, ctx, view, baseKey))
	require.False(t, keyExists(t, ctx, view, removeKey))
}

func bitmapValues(bm bitmap.Bitmap) []uint64 {
	if bm == nil {
		return nil
	}
	values := make([]uint64, 0)
	for v := range bm.Iter() {
		values = append(values, v)
	}
	return values
}

func keyExists(t *testing.T, ctx context.Context, tx badgerd.Tx, key []byte) bool {
	t.Helper()
	_, err := tx.Get(ctx, key)
	if err == nil {
		return true
	}
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	return false
}

func countPrefix(ctx context.Context, t *testing.T, tx badgerd.Tx, prefix []byte) int {
	t.Helper()
	it := tx.Iterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	return count
}
