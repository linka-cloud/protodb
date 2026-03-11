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

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/internal/protodb"
)

func TestStorageMigrationRebuildsUIDMappings(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	d := dbif.(*db)

	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, d.RegisterProto(ctx, fd))

	md, err := lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)

	_, err = d.Set(ctx, newIndexedMessage(md, "k1", "up", nil, "admin", "alpha"))
	require.NoError(t, err)
	_, err = d.Set(ctx, newIndexedMessage(md, "k2", "down", nil, "user", "beta"))
	require.NoError(t, err)

	filter := filters.Where("status").StringEquals("up")
	res, _, err := d.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)

	require.NoError(t, deleteByPrefix(ctx, d, []byte(protodb.UID+"/")))
	require.NoError(t, deleteByPrefix(ctx, d, []byte(protodb.UIDRev+"/")))
	require.NoError(t, deleteKeys(ctx, d, protodb.UIDLastKey(), protodb.StorageVersionKey()))
	require.NoError(t, d.Close())

	dbif, err = Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()
	d = dbif.(*db)

	md, err = lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)
	res, _, err = d.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "k1", keyFromMessage(t, res[0]))

	uidCount, err := countPrefix(ctx, d, []byte(protodb.UID+"/"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, uidCount, 2)

	uidRevCount, err := countPrefix(ctx, d, []byte(protodb.UIDRev+"/"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, uidRevCount, 2)

	version, found, err := getUint64(ctx, d, protodb.StorageVersionKey())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, storageVersion, version)

	last, found, err := getUint64(ctx, d, protodb.UIDLastKey())
	require.NoError(t, err)
	require.True(t, found)
	require.GreaterOrEqual(t, last, uint64(2))
}

func TestStorageVersionTooNewFailsOpen(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	d := dbif.(*db)

	require.NoError(t, setRaw(ctx, d, protodb.StorageVersionKey(), y.U64ToBytes(storageVersion+1)))
	require.NoError(t, d.Close())

	_, err = Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.Error(t, err)
	require.True(t, errors.Is(err, errUnsupportedStorageVersion))
}

func deleteByPrefix(ctx context.Context, db *db, prefix []byte) error {
	var keys [][]byte
	if err := db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return deleteKeys(ctx, db, keys...)
}

func deleteKeys(ctx context.Context, db *db, keys ...[]byte) error {
	tx, err := db.bdb.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Close(ctx)
	for _, key := range keys {
		if err := tx.Delete(ctx, key); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func setRaw(ctx context.Context, db *db, key, value []byte) error {
	tx, err := db.bdb.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Close(ctx)
	if err := tx.Set(ctx, key, value, 0); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func countPrefix(ctx context.Context, db *db, prefix []byte) (int, error) {
	count := 0
	err := db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func getUint64(ctx context.Context, db *db, key []byte) (uint64, bool, error) {
	var out uint64
	found := false
	err := db.bdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		found = true
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return errors.New("invalid uint64 payload")
			}
			out = y.BytesToU64(val)
			return nil
		})
	})
	if err != nil {
		return 0, false, err
	}
	return out, found, nil
}
