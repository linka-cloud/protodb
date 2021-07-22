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
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v2"
	pf "go.linka.cloud/protofilters"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb/internal/token"
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

func (tx *tx) Get(ctx context.Context, m proto.Message, paging *Paging, queryFilters ...*filters.FieldFilter) (out []proto.Message, info *PagingInfo, err error) {
	if tx.closed() {
		return nil, nil, ErrClosed
	}
	prefix := dataPrefix(m)
	it := tx.txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
	hasContinuationToken := paging.GetToken() != ""
	inToken := &token.Token{}
	if err := inToken.Decode(paging.GetToken()); err != nil {
		return nil, nil, err
	}
	hash, err := hash(filters.New(queryFilters...))
	if err != nil {
		return nil, nil, fmt.Errorf("hash filters: %w", err)
	}
	outToken := &token.Token{
		Ts:          tx.txn.ReadTs(),
		Type:        string(m.ProtoReflect().Descriptor().FullName()),
		FiltersHash: hash,
	}
	if err := outToken.ValidateFor(inToken); err != nil {
		return nil, nil, err
	}
	var (
		count   = uint64(0)
		match   = true
		hasNext = false
	)
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		item := it.Item()
		if item.Version() <= inToken.Ts &&
			count < paging.GetOffset() &&
			bytes.Compare(item.Key(), inToken.GetLastPrefix()) <= 0 {
			continue
		}
		v := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error {
			if err := tx.db.unmarshal(val, v); err != nil {
				return err
			}
			if len(queryFilters) != 0 {
				match, err = pf.MatchFilters(v, queryFilters...)
				if err != nil {
					return err
				}
				if match {
					count++
				}
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
		if max := paging.GetOffset() + paging.GetLimit(); max != 0 {
			if count == max+1 || (hasContinuationToken && count == paging.GetLimit()+1) {
				hasNext = true
				break
			}
			if !hasContinuationToken && count <= paging.GetOffset() {
				continue
			}
		}
		if !match {
			continue
		}
		outToken.LastPrefix = make([]byte, len(item.Key()))
		copy(outToken.LastPrefix, item.Key())
		out = append(out, v)
	}
	tks, err := outToken.Encode()
	if err != nil {
		return nil, nil, err
	}
	return out, &PagingInfo{HasNext: hasNext, Token: tks}, nil
}

func (tx *tx) Put(ctx context.Context, m proto.Message) (proto.Message, error) {
	if tx.closed() {
		return nil, ErrClosed
	}
	if m == nil {
		return nil, errors.New("empty message")
	}
	if tx.db.opts.applyDefaults {
		defaults(m)
	}
	k := dataPrefix(m)
	b, err := tx.db.marshal(m)
	if err != nil {
		return nil, err
	}
	if err := tx.txn.Set(k, b); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return nil, err
	}
	return m, nil
}

func (tx *tx) Delete(ctx context.Context, m proto.Message) error {
	if tx.closed() {
		return ErrClosed
	}
	if m == nil {
		return errors.New("empty message")
	}
	// TODO(adphi): should we check / read for key first ?
	if err := tx.txn.Delete(dataPrefix(m)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	return nil
}

func (tx *tx) Commit(ctx context.Context) error {
	if tx.closed() {
		return ErrClosed
	}
	defer tx.close()
	return tx.txn.Commit()
}

func (tx *tx) Close() {
	tx.txn.Discard()
	tx.close()
}

func (tx *tx) closed() bool {
	tx.m.RLock()
	defer tx.m.RUnlock()
	return tx.done
}

func (tx *tx) close() {
	tx.m.Lock()
	tx.done = true
	tx.m.Unlock()
}

func hash(m interface{ MarshalVT() ([]byte, error) }) (string, error) {
	b, err := m.MarshalVT()
	if err != nil {
		return "", err
	}
	sha := sha512.New()
	sha.Write(b)
	h := sha.Sum(nil)
	return base64.StdEncoding.EncodeToString(h), nil
}
