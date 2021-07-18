// Copyright 2021 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protodb

import (
	"context"
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v2"
	pf "go.linka.cloud/protofilters"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed = errors.New("transaction closed")
)

func newTx(ctx context.Context, db *db) (Tx, error) {
	return &tx{ctx: ctx, txn: db.bdb.NewTransaction(true), db: db}, nil
}

type tx struct {
	ctx context.Context

	db  *db
	txn *badger.Txn

	applyDefaults bool

	m    sync.RWMutex
	done bool
}

func (t *tx) Get(ctx context.Context, m proto.Message, paging *Paging, filters ...*filters.FieldFilter) (out []proto.Message, info *PagingInfo, err error) {
	if t.closed() {
		return nil, nil, ErrClosed
	}
	it := t.txn.NewIterator(badger.IteratorOptions{PrefetchValues: false, Prefix: dataPrefix(m)})
	defer it.Close()
	count := uint64(0)
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		count++
		if paging != nil && paging.Offset > 0 && count < paging.Offset {
			continue
		}
		if paging != nil && paging.Limit > 0 && count > paging.Limit {
			return out, &PagingInfo{HasNext: true}, nil
		}
		v := m.ProtoReflect().New().Interface()
		if err := it.Item().Value(func(val []byte) error {
			if err := t.db.unmarshal(val, v); err != nil {
				return err
			}
			if len(filters) != 0 {
				ok, err := pf.MatchFilters(m, filters...)
				if err != nil {
					return err
				}
				if !ok {
					count--
					return nil
				}
			}
			out = append(out, v)
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return out, nil, nil
}

func (t *tx) Put(ctx context.Context, m proto.Message) (proto.Message, error) {
	if t.closed() {
		return nil, ErrClosed
	}
	if m == nil {
		return nil, errors.New("empty message")
	}
	if t.db.opts.applyDefaults {
		defaults(m)
	}
	k := dataPrefix(m)
	b, err := t.db.marshal(m)
	if err != nil {
		return nil, err
	}
	if err := t.txn.Set(k, b); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		t.close()
		return nil, err
	}
	return m, nil
}

func (t *tx) Delete(ctx context.Context, m proto.Message) error {
	if t.closed() {
		return ErrClosed
	}
	if m == nil {
		return errors.New("empty message")
	}
	// TODO(adphi): should we check / read for key first ?
	if err := t.txn.Delete(dataPrefix(m)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		t.close()
		return err
	}
	return nil
}

func (t *tx) Commit(ctx context.Context) error {
	if t.closed() {
		return ErrClosed
	}
	defer t.close()
	return t.txn.Commit()
}

func (t *tx) Close() {
	t.txn.Discard()
	t.close()
}

func (t *tx) closed() bool {
	t.m.RLock()
	defer t.m.RUnlock()
	return t.done
}

func (t *tx) close() {
	t.m.Lock()
	t.done = true
	t.m.Unlock()
}
