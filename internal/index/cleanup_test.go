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

	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/index/bitmap"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
)

func TestCleanupOrphanUIDsRemovesStaleUIDs(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	baseKey := shardKey(name, "1", []byte("ready"), 0)
	dataKey := []byte(protodb.Data + "/tests.index.Doc/k1")
	uid := uint64(7)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	bm := bitmap.NewWith(1)
	bm.Set(uid)
	require.NoError(t, tx.Set(ctx, baseKey, bm.Bytes(), 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDKey(uid), dataKey, 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDRevKey(dataKey), y.U64ToBytes(uid), 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	removed, err := CleanupOrphanUIDs(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	require.False(t, hasKey(t, ctx, view, baseKey))
	require.False(t, hasKey(t, ctx, view, protodb.UIDKey(uid)))
	require.False(t, hasKey(t, ctx, view, protodb.UIDRevKey(dataKey)))
}

func TestCleanupOrphanUIDsKeepsLiveUIDs(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	baseKey := shardKey(name, "1", []byte("ready"), 0)
	dataKey := []byte(protodb.Data + "/tests.index.Doc/k1")
	uid := uint64(7)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	bm := bitmap.NewWith(1)
	bm.Set(uid)
	require.NoError(t, tx.Set(ctx, baseKey, bm.Bytes(), 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDKey(uid), dataKey, 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDRevKey(dataKey), y.U64ToBytes(uid), 0))
	require.NoError(t, tx.Set(ctx, dataKey, []byte("payload"), 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	removed, err := CleanupOrphanUIDs(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 0, removed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	require.True(t, hasKey(t, ctx, view, baseKey))
	require.True(t, hasKey(t, ctx, view, protodb.UIDKey(uid)))
	require.True(t, hasKey(t, ctx, view, protodb.UIDRevKey(dataKey)))
	bm2, err := readBitmapTx(view, baseKey)
	require.NoError(t, err)
	require.Equal(t, []uint64{uid}, uids(bm2))
}

func TestCleanupOrphanUIDsRespectsLimit(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	baseKey := shardKey(name, "1", []byte("ready"), 0)
	dataKey1 := []byte(protodb.Data + "/tests.index.Doc/k1")
	dataKey2 := []byte(protodb.Data + "/tests.index.Doc/k2")
	uid1 := uint64(1)
	uid2 := uint64(2)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	bm := bitmap.NewWith(2)
	bm.Set(uid1)
	bm.Set(uid2)
	require.NoError(t, tx.Set(ctx, baseKey, bm.Bytes(), 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDKey(uid1), dataKey1, 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDKey(uid2), dataKey2, 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDRevKey(dataKey1), y.U64ToBytes(uid1), 0))
	require.NoError(t, tx.Set(ctx, protodb.UIDRevKey(dataKey2), y.U64ToBytes(uid2), 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	removed, err := CleanupOrphanUIDs(ctx, db, 1)
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	bm2, err := readBitmapTx(view, baseKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(uids(bm2)))
	require.NoError(t, view.Close(ctx))

	removed, err = CleanupOrphanUIDs(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	view, err = db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	require.False(t, hasKey(t, ctx, view, baseKey))
	require.False(t, hasKey(t, ctx, view, protodb.UIDKey(uid1)))
	require.False(t, hasKey(t, ctx, view, protodb.UIDKey(uid2)))
	require.False(t, hasKey(t, ctx, view, protodb.UIDRevKey(dataKey1)))
	require.False(t, hasKey(t, ctx, view, protodb.UIDRevKey(dataKey2)))
}

func TestCleanupOrphanUIDsSkipsDeltaKeys(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Doc")
	delta := deltaKey(name, "1", []byte("ready"), 0, 9)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	require.NoError(t, tx.Set(ctx, delta, []byte{deltaAdd}, 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	removed, err := CleanupOrphanUIDs(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 0, removed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	require.True(t, hasKey(t, ctx, view, delta))
}

func TestCleanupOrphanUIDsIgnoresMalformedIndexKey(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	require.NoError(t, tx.Set(ctx, []byte(protodb.Index+"/bad"), []byte{1}, 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	removed, err := CleanupOrphanUIDs(ctx, db, 0)
	require.NoError(t, err)
	require.Equal(t, 0, removed)

	view, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer view.Close(ctx)
	require.True(t, hasKey(t, ctx, view, []byte(protodb.Index+"/bad")))
}
