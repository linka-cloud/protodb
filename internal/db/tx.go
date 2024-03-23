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

package db

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
	"go.linka.cloud/grpc-toolkit/logger"
	pf "go.linka.cloud/protofilters"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb/internal/db/pending"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/replication"
	"go.linka.cloud/protodb/internal/token"
)

func newTx(ctx context.Context, db *db, opts ...protodb.TxOption) (*tx, error) {
	if db.closed() {
		return nil, badger.ErrDBClosed
	}
	end := metrics.Tx.Start("")
	readTs := db.orc.readTs()
	if db.replicated() {
		logger.C(ctx).Tracef("starting transaction with readTs %d", readTs)
	}
	var o protodb.TxOpts
	for _, opt := range opts {
		opt(&o)
	}
	update := !o.ReadOnly
	var (
		txr *replication.Tx
		err error
	)
	if update && db.replicated() {
		if !db.repl.IsLeader() {
			return nil, protodb.ErrNotLeader
		}
		if txr, err = db.repl.NewTx(ctx); err != nil {
			return nil, err
		}
		if err := txr.New(ctx, readTs); err != nil {
			return nil, err
		}
	}
	tx := &tx{
		ctx:          ctx,
		txn:          db.bdb.NewTransactionAt(readTs, false),
		txr:          txr,
		readTs:       readTs,
		db:           db,
		me:           end,
		update:       update,
		conflictKeys: make(map[uint64]struct{}),
	}
	tx.pendingWrites = pending.NewWithDB(db.bdb, tx.addReadKey)
	return tx, nil
}

type tx struct {
	ctx context.Context

	db       *db
	update   bool
	txn      *badger.Txn
	readTs   uint64
	doneRead bool

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys  map[uint64]struct{}
	readsLock     sync.Mutex // guards the reads slice. See addReadKey.
	pendingWrites pending.IterableMergedWrites

	applyDefaults bool

	count int64
	size  int64
	me    MetricsEnd

	m    sync.RWMutex
	done bool

	txr *replication.Tx
}

func (tx *tx) newIterator(opt badger.IteratorOptions) pending.Iterator {
	if !tx.update {
		return pending.TxIterator(tx.txn.NewIterator(opt), tx.addReadKey)
	}
	return tx.pendingWrites.MergedIterator(tx.txn, tx.readTs, opt)
}

func (tx *tx) Get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (out []proto.Message, info *protodb.PagingInfo, err error) {
	defer metrics.Tx.Get.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	out, info, err = tx.get(ctx, m, opts...)
	if err != nil {
		metrics.Tx.Get.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
	}
	return
}

func (tx *tx) get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (out []proto.Message, info *protodb.PagingInfo, err error) {
	if tx.closed() {
		return nil, nil, badger.ErrDBClosed
	}
	o := makeGetOpts(opts...)
	prefix, field, key, _ := protodb.DataPrefix(m)
	hasContinuationToken := o.Paging.GetToken() != ""
	inToken := &token.Token{}
	if err := inToken.Decode(o.Paging.GetToken()); err != nil {
		return nil, nil, err
	}
	hash, err := hash(o.Filter)
	if err != nil {
		return nil, nil, fmt.Errorf("hash filter: %w", err)
	}
	outToken := &token.Token{
		Ts:          tx.txn.ReadTs(),
		Type:        string(m.ProtoReflect().Descriptor().FullName()),
		FiltersHash: hash,
	}
	if err := outToken.ValidateFor(inToken); err != nil {
		return nil, nil, err
	}
	keyOnly := IsKeyOnlyFilter(o.Filter, field)
	if keyOnly && o.Filter.Expr().GetCondition().GetFilter().GetString_().GetHasPrefix() != "" {
		prefix = []byte(o.Filter.Expr().GetCondition().GetFilter().GetString_().GetHasPrefix())
	}
	it := tx.newIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
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
		key := key
		if key == "" {
			key = string(item.Key()[len(prefix):])
		}
		if item.Version() <= inToken.Ts &&
			count < o.Paging.GetOffset() &&
			bytes.Compare(item.Key(), inToken.GetLastPrefix()) <= 0 {
			continue
		}
		v := m.ProtoReflect().New().Interface()

		if o.Filter == nil || !keyOnly {
			if err := item.Value(func(val []byte) error {
				if err := tx.db.unmarshal(val, v); err != nil {
					return err
				}
				if o.Filter != nil {
					match, err = pf.Match(v, o.Filter)
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return nil, nil, err
			}
			if !match {
				continue
			}
		} else {
			match, err = MatchKey(o.Filter, key)
			if err != nil {
				return nil, nil, err
			}
			if !match {
				continue
			}
			if err := item.Value(func(val []byte) error {
				if err := tx.db.unmarshal(val, v); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return nil, nil, err
			}
		}
		count++
		if max := o.Paging.GetOffset() + o.Paging.GetLimit(); max != 0 {
			if count == max+1 || (hasContinuationToken && count == o.Paging.GetLimit()+1) {
				hasNext = true
				break
			}
			if !hasContinuationToken && count <= o.Paging.GetOffset() {
				continue
			}
		}
		outToken.LastPrefix = item.KeyCopy(outToken.LastPrefix)
		if o.FieldMask != nil {
			if err := FilterFieldMask(v, o.FieldMask); err != nil {
				return nil, nil, err
			}
		}
		out = append(out, v)
	}
	tks, err := outToken.Encode()
	if err != nil {
		return nil, nil, err
	}
	return out, &protodb.PagingInfo{HasNext: hasNext, Token: tks}, nil
}

func (tx *tx) getRaw(key []byte) ([]byte, error) {
	if tx.closed() {
		return nil, badger.ErrDBClosed
	}
	item, err := tx.txn.Get(key)
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (tx *tx) Set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	defer metrics.Tx.Set.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	out, err := tx.set(ctx, m, opts...)
	if err != nil {
		metrics.Tx.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
	}
	return out, err
}
func (tx *tx) set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	if tx.closed() {
		return nil, badger.ErrDBClosed
	}
	if !tx.update {
		return nil, badger.ErrReadOnlyTxn
	}
	if m == nil {
		return nil, errors.New("empty message")
	}
	o := makeSetOpts(opts...)
	k, _, _, err := protodb.DataPrefix(m)
	if err != nil {
		return nil, err
	}
	if o.FieldMask != nil {
		item, err := tx.txn.Get(k)
		if err != nil {
			return nil, err
		}
		old := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error {
			return tx.db.unmarshal(val, old)
		}); err != nil {
			return nil, err
		}
		if err := ApplyFieldMask(m, old, o.FieldMask); err != nil {
			return nil, err
		}
		m = old
	}
	if tx.db.opts.applyDefaults {
		applyDefaults(m)
	}
	b, err := tx.db.marshal(m)
	if err != nil {
		return nil, err
	}
	e := badger.NewEntry(k, b)
	if o.TTL != 0 {
		e = e.WithTTL(o.TTL)
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return nil, err
	}
	tx.addConflictKey(e.Key)
	tx.pendingWrites.Set(e)
	return m, tx.txr.Set(ctx, e.Key, e.Value, e.ExpiresAt)
}

func (tx *tx) setRaw(ctx context.Context, key, val []byte) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	tx.addConflictKey(key)
	tx.pendingWrites.Set(badger.NewEntry(key, val))
	return tx.txr.Set(ctx, key, val, 0)
}

func (tx *tx) Delete(ctx context.Context, m proto.Message) error {
	defer metrics.Tx.Delete.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	if err := tx.delete(ctx, m); err != nil {
		metrics.Tx.Delete.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return err
	}
	return nil
}

func (tx *tx) delete(ctx context.Context, m proto.Message) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if m == nil {
		return errors.New("empty message")
	}
	if !tx.update {
		return badger.ErrReadOnlyTxn
	}
	// TODO(adphi): should we check / read for key first ?
	k, _, _, err := protodb.DataPrefix(m)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	tx.addConflictKey(k)
	tx.pendingWrites.Delete(k)
	return tx.txr.Delete(ctx, k)
}

func (tx *tx) deleteRaw(ctx context.Context, key []byte) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	tx.addConflictKey(key)
	tx.pendingWrites.Delete(key)
	if err := tx.txn.Delete(key); err != nil {
		return err
	}
	return tx.txr.Delete(ctx, key)
}

func (tx *tx) Commit(ctx context.Context) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	defer tx.close()
	if !tx.update {
		return nil
	}
	tx.db.orc.writeChLock.Lock()
	defer tx.db.orc.writeChLock.Unlock()

	ts, conflict := tx.db.orc.newCommitTs(tx)
	if conflict {
		return badger.ErrConflict
	}
	defer tx.db.orc.doneCommit(ts)

	b := tx.db.bdb.NewWriteBatchAt(ts)
	defer b.Cancel()
	if err := tx.pendingWrites.Replay(func(e *badger.Entry) error {
		if e.UserMeta != 0 {
			if err := b.DeleteAt(e.Key, ts); err != nil {
				return err
			}
		}
		return b.SetEntryAt(e, ts)
	}); err != nil {
		metrics.Tx.ErrorsCounter.WithLabelValues("").Inc()
		return err
	}
	if err := b.Flush(); err != nil {
		metrics.Tx.ErrorsCounter.WithLabelValues("").Inc()
		return err
	}
	return tx.txr.Commit(ctx, ts)
}

func (tx *tx) Close() {
	tx.txn.Discard()
	tx.close()
}

func (tx *tx) closed() bool {
	tx.m.RLock()
	defer tx.m.RUnlock()
	return tx.done || tx.db.closed()
}

func (tx *tx) close() {
	if tx.closed() {
		return
	}
	tx.m.Lock()
	tx.me.End()
	metrics.Tx.OpCountHist.Observe(float64(tx.count))
	metrics.Tx.SizeHist.Observe(float64(tx.size))
	tx.db.orc.doneRead(tx)
	tx.pendingWrites.Close()
	tx.done = true
	tx.m.Unlock()
}

func (tx *tx) addReadKey(key []byte) {
	if tx.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		tx.readsLock.Lock()
		tx.reads = append(tx.reads, fp)
		tx.readsLock.Unlock()
	}
}

func (tx *tx) addConflictKey(key []byte) {
	if !tx.update {
		return
	}
	fp := z.MemHash(key)
	tx.conflictKeys[fp] = struct{}{}
}

func hash(f protodb.Filter) (hash string, err error) {
	var b []byte
	if f != nil {
		b, err = f.Expr().MarshalVT()
		if err != nil {
			return "", err
		}
	}
	sha := sha512.New()
	sha.Write(b)
	h := sha.Sum(nil)
	return base64.StdEncoding.EncodeToString(h), nil
}
