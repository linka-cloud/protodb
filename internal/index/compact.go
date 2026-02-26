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
	"bytes"
	"context"
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
	"go.linka.cloud/protofilters/index/bitmap"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
)

type deltaOp struct {
	key []byte
	uid uint64
	op  byte
}

// CompactDeltas folds delta entries into base bitmaps and deletes deltas.
// limit controls how many delta entries are processed in one call (0 = no limit).
func CompactDeltas(ctx context.Context, db badgerd.DB, limit int) (int, error) {
	txn, err := db.NewTransaction(ctx, true)
	if err != nil {
		return 0, err
	}
	defer txn.Close(ctx)
	count, err := compactDeltasTx(ctx, txn, limit)
	if err != nil {
		return count, err
	}
	if count == 0 {
		return 0, nil
	}
	return count, txn.Commit(ctx)
}

func compactDeltasTx(ctx context.Context, txn badgerd.Tx, limit int) (int, error) {
	var (
		processed int
		current   []byte
		deltas    []deltaOp
	)
	flush := func() error {
		if len(current) == 0 {
			return nil
		}
		bm, err := readBitmapTx(txn, current)
		if err != nil {
			return err
		}
		for _, d := range deltas {
			switch d.op {
			case deltaAdd:
				bm.Set(d.uid)
			case deltaRemove:
				bm.Remove(d.uid)
			}
		}
		if len(bm.Bytes()) == 0 {
			if err := txn.Delete(ctx, current); err != nil {
				return err
			}
		} else {
			if err := txn.Set(ctx, current, bm.Bytes(), 0); err != nil {
				return err
			}
		}
		for _, d := range deltas {
			if err := txn.Delete(ctx, d.key); err != nil {
				return err
			}
		}
		return nil
	}

	prefix := []byte(protodb.IndexDelta + "/")
	it := txn.Iterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		if limit > 0 && processed >= limit {
			break
		}
		item := it.Item()
		key := item.Key()
		base, ok := baseKeyFromDelta(key)
		if !ok {
			continue
		}
		if len(current) == 0 {
			current = append(current[:0], base...)
		}
		if !bytes.Equal(base, current) {
			if err := flush(); err != nil {
				return processed, err
			}
			current = append(current[:0], base...)
			deltas = deltas[:0]
		}
		op := deltaAdd
		if err := item.Value(func(val []byte) error {
			if len(val) > 0 {
				op = val[0]
			}
			return nil
		}); err != nil {
			return processed, err
		}
		uid, ok := deltaUIDLow(key)
		if !ok {
			continue
		}
		deltas = append(deltas, deltaOp{key: item.KeyCopy(nil), uid: uid, op: op})
		processed++
	}
	if err := flush(); err != nil {
		return processed, err
	}
	return processed, nil
}

func baseKeyFromDelta(key []byte) ([]byte, bool) {
	if !bytes.HasPrefix(key, []byte(protodb.IndexDelta+"/")) {
		return nil, false
	}
	if len(key) <= uidSize {
		return nil, false
	}
	base := append([]byte{}, []byte(protodb.Index)...)
	base = append(base, key[len(protodb.IndexDelta):len(key)-uidSize]...)
	return base, true
}

func deltaUIDLow(key []byte) (uint64, bool) {
	if len(key) < uidSize {
		return 0, false
	}
	uid := binary.BigEndian.Uint64(key[len(key)-uidSize:])
	return uid, true
}

func readBitmapTx(txn badgerd.Tx, key []byte) (bitmap.Bitmap, error) {
	item, err := txn.Get(context.Background(), key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return bitmap.New(), nil
		}
		return nil, err
	}
	var b []byte
	if err := item.Value(func(val []byte) error {
		b = append(b[:0], val...)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return bitmap.New(), nil
	}
	return bitmap.NewFrom(b), nil
}
