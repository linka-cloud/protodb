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

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
	pf "go.linka.cloud/protofilters"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb/internal/token"
)

func newTx(ctx context.Context, db *db, opts ...TxOption) (*tx, error) {
	if db.closed() {
		return nil, ErrDBClosed
	}
	end := metrics.Tx.Start("")
	readTs := db.orc.readTs()
	tx := &tx{
		ctx:          ctx,
		txn:          db.bdb.NewTransactionAt(readTs, true),
		readTs:       readTs,
		db:           db,
		me:           end,
		update:       true,
		conflictKeys: make(map[uint64]struct{}),
	}
	var o TxOpts
	for _, opt := range opts {
		opt(&o)
	}
	tx.update = !o.ReadOnly
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
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	applyDefaults bool

	count int64
	size  int64
	me    MetricsEnd

	m    sync.RWMutex
	done bool
}

func (tx *tx) Get(ctx context.Context, m proto.Message, opts ...GetOption) (out []proto.Message, info *PagingInfo, err error) {
	defer metrics.Tx.Get.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	out, info, err = tx.get(ctx, m, opts...)
	if err != nil {
		metrics.Tx.Get.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
	}
	return
}

func (tx *tx) get(ctx context.Context, m proto.Message, opts ...GetOption) (out []proto.Message, info *PagingInfo, err error) {
	if tx.closed() {
		return nil, nil, ErrDBClosed
	}
	o := makeGetOpts(opts...)
	prefix, _ := dataPrefix(m)
	it := tx.txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
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
		tx.addReadKey(item.Key())
		if item.Version() <= inToken.Ts &&
			count < o.Paging.GetOffset() &&
			bytes.Compare(item.Key(), inToken.GetLastPrefix()) <= 0 {
			continue
		}
		v := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error {
			if err := tx.db.unmarshal(val, v); err != nil {
				return err
			}
			if o.Filter != nil {
				match, err = pf.Match(v, o.Filter)
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
		if max := o.Paging.GetOffset() + o.Paging.GetLimit(); max != 0 {
			if count == max+1 || (hasContinuationToken && count == o.Paging.GetLimit()+1) {
				hasNext = true
				break
			}
			if !hasContinuationToken && count <= o.Paging.GetOffset() {
				continue
			}
		}
		if !match {
			continue
		}
		outToken.LastPrefix = make([]byte, len(item.Key()))
		copy(outToken.LastPrefix, item.Key())
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
	return out, &PagingInfo{HasNext: hasNext, Token: tks}, nil
}

func (tx *tx) getRaw(key []byte) ([]byte, error) {
	if tx.closed() {
		return nil, ErrDBClosed
	}
	item, err := tx.txn.Get(key)
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (tx *tx) Set(ctx context.Context, m proto.Message, opts ...SetOption) (proto.Message, error) {
	defer metrics.Tx.Set.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	m, err := tx.set(ctx, m, opts...)
	if err != nil {
		metrics.Tx.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
	}
	return m, err
}
func (tx *tx) set(ctx context.Context, m proto.Message, opts ...SetOption) (proto.Message, error) {
	if tx.closed() {
		return nil, ErrDBClosed
	}
	if !tx.update {
		return nil, ErrReadOnlyTxn
	}
	if m == nil {
		return nil, errors.New("empty message")
	}
	if tx.db.opts.applyDefaults {
		applyDefaults(m)
	}
	o := makeSetOpts(opts...)
	k, err := dataPrefix(m)
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
	b, err := tx.db.marshal(m)
	if err != nil {
		return nil, err
	}
	e := badger.NewEntry(k, b)
	if o.TTL != 0 {
		e = e.WithTTL(o.TTL)
	}
	if err := tx.checkSize(e); err != nil {
		return nil, err
	}
	// The txn.conflictKeys is used for conflict detection.
	fp := z.MemHash(e.Key) // Avoid dealing with byte arrays.
	tx.conflictKeys[fp] = struct{}{}
	if err := ctx.Err(); err != nil {
		tx.close()
		return nil, err
	}
	if err := tx.txn.SetEntry(e); err != nil {
		return nil, err
	}
	return m, nil
}

func (tx *tx) setRaw(key, val []byte) error {
	if tx.closed() {
		return ErrDBClosed
	}
	return tx.txn.Set(key, val)
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
		return ErrDBClosed
	}
	if m == nil {
		return errors.New("empty message")
	}
	if !tx.update {
		return ErrReadOnlyTxn
	}
	// TODO(adphi): should we check / read for key first ?
	k, err := dataPrefix(m)
	if err != nil {
		return err
	}
	if err := tx.checkSize(badger.NewEntry(k, nil)); err != nil {
		return err
	}
	// The txn.conflictKeys is used for conflict detection.
	fp := z.MemHash(k) // Avoid dealing with byte arrays.
	tx.conflictKeys[fp] = struct{}{}
	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	if err := tx.txn.Delete(k); err != nil {
		return err
	}
	return nil
}

func (tx *tx) deleteRaw(key []byte) error {
	if tx.closed() {
		return ErrDBClosed
	}
	return tx.txn.Delete(key)
}

func (tx *tx) Count() (int64, error) {
	tx.m.RLock()
	defer tx.m.RUnlock()
	return tx.count, nil
}

func (tx *tx) Size() (int64, error) {
	tx.m.RLock()
	defer tx.m.RUnlock()
	return tx.size, nil
}

func (tx *tx) Commit(ctx context.Context) error {
	if tx.closed() {
		return ErrDBClosed
	}
	defer tx.close()
	tx.db.orc.writeChLock.Lock()
	defer tx.db.orc.writeChLock.Unlock()

	ts, conflict := tx.db.orc.newCommitTs(tx)
	if conflict {
		return ErrConflict
	}
	defer tx.db.orc.doneCommit(ts)
	if err := tx.txn.CommitAt(ts, nil); err != nil {
		metrics.Tx.ErrorsCounter.WithLabelValues("").Inc()
		return err
	}
	return nil
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
	tx.done = true
	tx.m.Unlock()
}

func (tx *tx) checkSize(e *badger.Entry) error {
	tx.m.Lock()
	defer tx.m.Unlock()
	count := tx.count + 1
	size := tx.size + int64(estimateSize(e, int(tx.db.bopts.ValueThreshold)))
	if count >= tx.db.bdb.MaxBatchCount() || size >= tx.db.bdb.MaxBatchSize() {
		return ErrTxnTooBig
	}
	tx.count, tx.size = count, size
	return nil
}

func (txn *tx) addReadKey(key []byte) {
	if txn.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}

func hash(f Filter) (hash string, err error) {
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

func estimateSize(e *badger.Entry, threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}
	return len(e.Key) + 12 + 2 // 12 for ValuePointer, 2 for metas.
}
