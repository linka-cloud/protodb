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
	"errors"

	"github.com/dgraph-io/badger/v3"
	"go.linka.cloud/protofilters/index/bitmap"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
)

// CleanupOrphanUIDs removes stale UIDs from base index bitmaps when they no longer
// resolve to a live data record. It also removes orphan uid/uid-rev mappings.
//
// limit controls how many stale UIDs are removed in one call (0 = no limit).
func CleanupOrphanUIDs(ctx context.Context, db badgerd.DB, limit int) (int, error) {
	txn, err := db.NewTransaction(ctx, true)
	if err != nil {
		return 0, err
	}
	defer txn.Close(ctx)
	count, err := cleanupTx(ctx, txn, limit)
	if err != nil {
		return count, err
	}
	if count == 0 {
		return 0, nil
	}
	return count, txn.Commit(ctx)
}

func cleanupTx(ctx context.Context, txn badgerd.Tx, limit int) (int, error) {
	count := 0
	prefix := []byte(protodb.Index + "/")
	it := txn.Iterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if limit > 0 && count >= limit {
			break
		}
		removed, err := cleanupKey(ctx, txn, it.Item(), limit-count)
		if err != nil {
			return count, err
		}
		count += removed
	}
	return count, nil
}

func cleanupKey(ctx context.Context, txn badgerd.Tx, item badgerd.Item, limit int) (int, error) {
	key := item.KeyCopy(nil)
	shard, ok := parseShard(key)
	if !ok {
		return 0, nil
	}
	b, err := readItem(item)
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, txn.Delete(ctx, key)
	}
	bm, ok := readBitmap(b)
	if !ok {
		return 0, nil
	}
	removed, err := pruneUIDs(ctx, txn, bm, shard, limit)
	if err != nil {
		return removed, err
	}
	if removed == 0 {
		return 0, nil
	}
	if len(bm.Bytes()) == 0 {
		return removed, txn.Delete(ctx, key)
	}
	return removed, txn.Set(ctx, key, bm.Bytes(), 0)
}

func pruneUIDs(ctx context.Context, txn badgerd.Tx, bm bitmap.Bitmap, shard uint32, limit int) (int, error) {
	count := 0
	for low := range bm.Iter() {
		if limit > 0 && count >= limit {
			break
		}
		uid := (uint64(shard) << uidShardShift) | low
		live, key, err := uidLive(ctx, txn, uid)
		if err != nil {
			return count, err
		}
		if live {
			continue
		}
		bm.Remove(low)
		count++
		if err := deleteUIDMaps(ctx, txn, uid, key); err != nil {
			return count, err
		}
	}
	return count, nil
}

func deleteUIDMaps(ctx context.Context, txn badgerd.Tx, uid uint64, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	if err := txn.Delete(ctx, protodb.UIDKey(uid)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	if err := txn.Delete(ctx, protodb.UIDRevKey(key)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	return nil
}

func readItem(item badgerd.Item) ([]byte, error) {
	var b []byte
	err := item.Value(func(val []byte) error {
		b = append([]byte(nil), val...)
		return nil
	})
	return b, err
}

func uidLive(ctx context.Context, txn badgerd.Tx, uid uint64) (bool, []byte, error) {
	uk := protodb.UIDKey(uid)
	item, err := txn.Get(ctx, uk)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil, nil
		}
		return false, nil, err
	}
	var dataKey []byte
	if err := item.Value(func(val []byte) error {
		dataKey = append(dataKey[:0], val...)
		return nil
	}); err != nil {
		return false, nil, err
	}
	if len(dataKey) == 0 {
		return false, nil, nil
	}
	_, err = txn.Get(ctx, dataKey)
	if err == nil {
		return true, dataKey, nil
	}
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, dataKey, nil
	}
	return false, nil, err
}

func readBitmap(data []byte) (bm bitmap.Bitmap, ok bool) {
	defer func() {
		if recover() != nil {
			bm = nil
			ok = false
		}
	}()
	return bitmap.NewFrom(data), true
}
