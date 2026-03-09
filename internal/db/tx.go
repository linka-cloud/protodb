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
	"cmp"
	"context"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	pfreflect "go.linka.cloud/protofilters/reflect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.linka.cloud/protofilters/index/bitmap"

	"go.linka.cloud/protodb/internal/badgerd"
	idxstore "go.linka.cloud/protodb/internal/index"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/token"
	"go.linka.cloud/protodb/pb"
	protopts "go.linka.cloud/protodb/protodb"
)

func newTx(ctx context.Context, db *db, opts ...protodb.TxOption) (*tx, error) {
	if db.closed() {
		return nil, badger.ErrDBClosed
	}
	end := metrics.Tx.Start("")
	var o protodb.TxOpts
	for _, opt := range opts {
		opt(&o)
	}
	update := !o.ReadOnly
	txn, err := db.bdb.NewTransaction(ctx, !o.ReadOnly)
	if err != nil {
		end.End()
		return nil, err
	}
	tx := &tx{
		ctx:    ctx,
		db:     db,
		txn:    txn,
		me:     end,
		update: update,
		span:   trace.SpanFromContext(ctx),
	}
	return tx, nil
}

type tx struct {
	ctx context.Context

	db     *db
	update bool
	txn    badgerd.Tx
	// readTs   uint64
	// doneRead bool
	//
	// reads []uint64 // contains fingerprints of keys read.
	// // contains fingerprints of keys written. This is used for conflict detection.
	// conflictKeys map[uint64]struct{}
	// readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	applyDefaults bool

	count int64
	size  int64
	me    MetricsEnd

	m    sync.RWMutex
	done bool

	span trace.Span
}

func (tx *tx) Txn() badgerd.Tx {
	return tx.txn
}

func (tx *tx) UID(ctx context.Context, key []byte, inc bool) (uint64, bool, error) {
	return tx.uid(ctx, key, inc)
}

func (tx *tx) GetOne(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (proto.Message, bool, error) {
	return protodb.GetOne(ctx, tx, m, opts...)
}

func (tx *tx) Get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (out []proto.Message, info *protodb.PagingInfo, err error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.Get")
		span.SetAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName())))
		defer span.End()
	}
	defer metrics.Tx.Get.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	tx.count++
	out, info, err = tx.get(ctx, m, opts...)
	if err != nil {
		span.RecordError(err)
		metrics.Tx.Get.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
	}
	return
}

func (tx *tx) get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (out []proto.Message, info *protodb.PagingInfo, err error) {
	span := trace.SpanFromContext(ctx)
	if tx.closed() {
		return nil, nil, badger.ErrDBClosed
	}
	o := makeGetOpts(opts...)
	if o.OrderBy != nil {
		if o.Reverse {
			return nil, nil, errors.New("reverse and order_by cannot be combined")
		}
		return tx.getOrdered(ctx, m, o)
	}
	if o.One {
		out = make([]proto.Message, 0, 1)
	} else if lim := o.Paging.GetLimit(); lim > 0 {
		out = make([]proto.Message, 0, lim)
	}
	prefix, field, value, _ := protodb.DataPrefix(m)
	span.SetAttributes(
		attribute.String("prefix", string(prefix)),
		attribute.String("key_field", field),
		attribute.String("key", value),
		attribute.Bool("filtered", o.Filter != nil),
		attribute.Bool("reverse", o.Reverse),
		attribute.Bool("continuation", o.Paging.GetToken() != ""),
	)
	if o.FieldMask != nil {
		span.SetAttributes(attribute.StringSlice("field_mask", o.FieldMask.GetPaths()))
	}
	// short path for simple get
	if value != "" {
		span.SetAttributes(attribute.Bool("direct", true))
		item, err := tx.txn.Get(ctx, prefix)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				span.SetAttributes(attribute.Bool("found", false))
				return nil, &protodb.PagingInfo{}, nil
			}
			return nil, nil, err
		}
		v := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error {
			return tx.db.unmarshal(val, v)
		}); err != nil {
			return nil, nil, err
		}
		if o.Filter != nil {
			match, err := tx.db.matcher.Match(v, o.Filter)
			if err != nil {
				return nil, nil, err
			}
			if !match {
				span.SetAttributes(attribute.Bool("found", false))
				return nil, &protodb.PagingInfo{}, nil
			}
		}
		span.SetAttributes(attribute.Bool("found", true))
		if o.FieldMask != nil {
			if err := FilterFieldMask(v, o.FieldMask); err != nil {
				return nil, nil, err
			}
		}
		tx.txn.AddReadKey(prefix)
		return []proto.Message{v}, &protodb.PagingInfo{}, nil
	}
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
		Reverse:     o.Reverse,
	}
	if err := outToken.ValidateFor(inToken); err != nil {
		return nil, nil, err
	}
	if o.Filter != nil {
		span.SetAttributes(attribute.Stringer("filter", o.Filter.Expr()))
		tx.db.opts.logger.Tracef("starting iteration at %s with filter %v", string(prefix), o.Filter.Expr().String())
	} else {
		tx.db.opts.logger.Tracef("starting unfiltered iteration at %s", string(prefix))
	}

	keyOnly := IsKeyOnlyFilter(o.Filter, field)
	span.SetAttributes(attribute.Bool("key_only", keyOnly))
	useIndex := false
	idxr := tx.db.idx
	if o.Filter != nil && !keyOnly {
		ok, err := idxr.IndexableFilter(m, o.Filter)
		if err != nil {
			return nil, nil, err
		}
		useIndex = ok
	}
	span.SetAttributes(attribute.Bool("use_index", useIndex))
	if useIndex {
		windowLimit := o.Paging.GetLimit()
		if o.One && (windowLimit == 0 || windowLimit > 1) {
			windowLimit = 1
		}
		findOpts := idxstore.FindOpts{Reverse: o.Reverse}
		if !hasContinuationToken {
			findOpts.Offset = o.Paging.GetOffset()
			if windowLimit != 0 {
				findOpts.Limit = windowLimit + 1
			}
		}
		uids, err := idxr.FindUIDs(ctx, tx, m.ProtoReflect().Descriptor().FullName(), o.Filter, findOpts)
		if err != nil {
			return nil, nil, err
		}
		hasNext := false
		if !hasContinuationToken && windowLimit != 0 && uint64(len(uids)) > windowLimit {
			hasNext = true
			uids = uids[:windowLimit]
		}
		var (
			reads        int
			readBytes    int
			resultsBytes int
		)
		for _, uid := range uids {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			uidItem, err := tx.txn.Get(ctx, protodb.UIDKey(uid))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return nil, nil, err
			}
			var fullKey string
			if err := uidItem.Value(func(val []byte) error {
				fullKey = string(val)
				return nil
			}); err != nil {
				return nil, nil, err
			}
			fullKeyBytes := stringToBytesNoCopy(fullKey)
			if !bytes.HasPrefix(fullKeyBytes, prefix) {
				continue
			}
			if hasContinuationToken {
				if !o.Reverse && bytes.Compare(fullKeyBytes, inToken.GetLastPrefix()) <= 0 {
					continue
				}
				if o.Reverse && bytes.Compare(fullKeyBytes, inToken.GetLastPrefix()) >= 0 {
					continue
				}
				if windowLimit != 0 && uint64(len(out)) >= windowLimit {
					hasNext = true
					break
				}
			}
			item, err := tx.txn.Get(ctx, fullKeyBytes)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return nil, nil, err
			}
			reads++
			v := m.ProtoReflect().New().Interface()
			var size int
			if err := item.Value(func(val []byte) error {
				size = len(val)
				readBytes += len(val)
				if err := tx.db.unmarshal(val, v); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return nil, nil, err
			}
			outToken.LastPrefix = append(outToken.LastPrefix[:0], fullKeyBytes...)
			if o.FieldMask != nil {
				if err := FilterFieldMask(v, o.FieldMask); err != nil {
					return nil, nil, err
				}
			}
			tx.txn.AddReadKey(fullKeyBytes)
			out = append(out, v)
			resultsBytes += int(size)
			if o.One {
				break
			}
		}
		span.SetAttributes(
			attribute.Bool("index", true),
			attribute.Int64("reads", int64(reads)),
			attribute.Int64("results", int64(len(out))),
			attribute.Bool("has_next", hasNext),
			attribute.Int("read_bytes", readBytes),
			attribute.Int("results_bytes", resultsBytes),
		)
		tks, err := outToken.Encode()
		if err != nil {
			return nil, nil, err
		}
		return out, &protodb.PagingInfo{HasNext: hasNext, Token: tks}, nil
	}
	iterPrefix := prefix
	if p, ok := MatchPrefixOnly(o.Filter); keyOnly && ok {
		tx.db.opts.logger.Tracef("filtering optimized by key only filter prefix")
		iterPrefix = append(prefix, []byte(p)...)
	}
	it := tx.txn.Iterator(badger.IteratorOptions{Prefix: iterPrefix, PrefetchValues: false, Reverse: o.Reverse})
	defer it.Close()
	var (
		count        = uint64(0)
		match        = true
		hasNext      = false
		reads        int
		readBytes    int
		resultsBytes int
	)
	if o.Reverse {
		it.SeekLast()
	} else {
		it.Rewind()
	}
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		var size int
		item := it.Item()
		itemKey := item.Key()
		if item.Version() <= inToken.Ts &&
			count < o.Paging.GetOffset() {
			// could be inlined, but it is easier to read this way
			if !o.Reverse && bytes.Compare(itemKey, inToken.GetLastPrefix()) <= 0 {
				continue
			}
			if o.Reverse && bytes.Compare(itemKey, inToken.GetLastPrefix()) >= 0 {
				continue
			}
		}

		tx.db.opts.logger.Tracef("checking %q", string(itemKey))
		reads++
		v := m.ProtoReflect().New().Interface()

		if o.Filter == nil || !keyOnly {
			if err := item.Value(func(val []byte) error {
				size = len(val)
				readBytes += len(val)
				if err := tx.db.unmarshal(val, v); err != nil {
					return err
				}
				if o.Filter != nil {
					match, err = tx.db.matcher.Match(v, o.Filter)
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
			key := string(itemKey[len(prefix):])
			match, err = MatchKey(o.Filter, key)
			if err != nil {
				return nil, nil, err
			}
			if !match {
				continue
			}
			if err := item.Value(func(val []byte) error {
				size = len(val)
				if err := tx.db.unmarshal(val, v); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return nil, nil, err
			}
		}
		tx.db.opts.logger.Tracef("key %q match filter", string(itemKey))
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
		outToken.LastPrefix = append(outToken.LastPrefix[:0], itemKey...)
		if o.FieldMask != nil {
			if err := FilterFieldMask(v, o.FieldMask); err != nil {
				return nil, nil, err
			}
		}
		tx.txn.AddReadKey(itemKey)
		out = append(out, v)
		resultsBytes += int(size)
		if o.One {
			break
		}
	}
	span.SetAttributes(
		attribute.Int64("reads", int64(reads)),
		attribute.Int64("results", int64(count)),
		attribute.Bool("has_next", hasNext),
		attribute.Int("read_bytes", readBytes),
		attribute.Int("results_bytes", resultsBytes),
	)
	tks, err := outToken.Encode()
	if err != nil {
		return nil, nil, err
	}
	return out, &protodb.PagingInfo{HasNext: hasNext, Token: tks}, nil
}

func (tx *tx) getRaw(ctx context.Context, key []byte) ([]byte, error) {
	if tx.closed() {
		return nil, badger.ErrDBClosed
	}
	item, err := tx.txn.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	tx.txn.AddReadKey(key)
	return item.ValueCopy([]byte{})
}

func (tx *tx) Set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.Set")
		span.SetAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName())))
		defer span.End()
	}
	defer metrics.Tx.Set.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	tx.count++
	out, err := tx.set(ctx, m, opts...)
	if err != nil {
		span.RecordError(err)
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
	span := trace.SpanFromContext(ctx)
	o := makeSetOpts(opts...)
	k, field, value, err := protodb.DataPrefix(m)
	if err != nil {
		return nil, err
	}
	uid, ok, err := tx.uid(ctx, k, true)
	if err != nil {
		return nil, err
	}
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("prefix", string(k)),
			attribute.String("key", value),
			attribute.String("key_field", field),
			attribute.Int64("uid", int64(uid)),
		)
	}
	if !ok {
		if err := ctx.Err(); err != nil {
			tx.close()
			return nil, err
		}
		uk := protodb.UIDKey(uid)
		tx.db.opts.logger.Tracef("set key %q", string(uk))
		if err := tx.txn.Set(ctx, uk, k, 0); err != nil {
			return nil, err
		}
		urk := protodb.UIDRevKey(k)
		tx.db.opts.logger.Tracef("set key %q", string(urk))
		if err := tx.txn.Set(ctx, urk, y.U64ToBytes(uid), 0); err != nil {
			return nil, err
		}
	}
	var oldMsg proto.Message
	skipIndexUpdate := false
	if o.FieldMask != nil {
		if span.IsRecording() {
			span.SetAttributes(attribute.StringSlice("field_mask", o.FieldMask.GetPaths()))
		}
		item, err := tx.txn.Get(ctx, k)
		if err != nil {
			return nil, err
		}
		old := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error {
			return tx.db.unmarshal(val, old)
		}); err != nil {
			return nil, err
		}
		oldMsg = proto.Clone(old.(proto.Message))
		if err := ApplyFieldMask(m, old, o.FieldMask); err != nil {
			return nil, err
		}
		skipIndexUpdate = !fieldMaskTouchesIndexed(old.(proto.Message).ProtoReflect().Descriptor(), o.FieldMask.GetPaths(), tx.db.idx.CollectEntries(old.(proto.Message).ProtoReflect().Descriptor()))
		m = old
	} else if ok {
		item, err := tx.txn.Get(ctx, k)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return nil, err
			}
		} else {
			old := m.ProtoReflect().New().Interface()
			if err := item.Value(func(val []byte) error {
				return tx.db.unmarshal(val, old)
			}); err != nil {
				return nil, err
			}
			oldMsg = old.(proto.Message)
		}
	}
	if tx.db.opts.applyDefaults {
		applyDefaults(m)
	}
	if err := tx.db.idx.EnforceUnique(ctx, tx, m, uid); err != nil {
		return nil, err
	}
	b, err := tx.db.marshal(m)
	if err != nil {
		return nil, err
	}
	if ok && !skipIndexUpdate {
		if err := tx.db.idx.Update(ctx, tx, k, oldMsg, m); err != nil {
			return nil, err
		}
	}
	if !ok {
		if err := tx.db.idx.Insert(ctx, tx, k, m); err != nil {
			return nil, err
		}
	}
	s := int64(len(b))
	if span.IsRecording() {
		span.SetAttributes(attribute.Int64("size", s))
	}
	tx.size += s
	expiresAt := uint64(0)
	if o.TTL != 0 {
		expiresAt = uint64(time.Now().Add(o.TTL).Unix())
		if span.IsRecording() {
			span.SetAttributes(attribute.String("ttl", o.TTL.String()))
		}
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return nil, err
	}
	tx.db.opts.logger.Tracef("set key %q", string(k))
	return m, tx.txn.Set(ctx, k, b, expiresAt)
}

func (tx *tx) setRaw(ctx context.Context, key, val []byte) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	tx.db.opts.logger.Tracef("raw set key %q", string(key))
	return tx.txn.Set(ctx, key, val, 0)
}

func (tx *tx) Delete(ctx context.Context, m proto.Message) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.Delete")
		span.SetAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName())))
		defer span.End()
	}
	defer metrics.Tx.Delete.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	tx.count++
	if err := tx.delete(ctx, m); err != nil {
		span.RecordError(err)
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
	k, field, value, err := protodb.DataPrefix(m)
	if err != nil {
		return err
	}

	uid, ok, err := tx.uid(ctx, k, false)
	if err != nil {
		return err
	}
	if !ok {
		return badger.ErrKeyNotFound
	}
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("key", value),
		attribute.String("key_field", field),
		attribute.Int64("uid", int64(uid)),
	)

	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	item, err := tx.txn.Get(ctx, k)
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	} else {
		old := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error {
			return tx.db.unmarshal(val, old)
		}); err != nil {
			return err
		}
		if err := tx.db.idx.Update(ctx, tx, k, old.(proto.Message), nil); err != nil {
			return err
		}
	}
	uk := protodb.UIDKey(uid)
	tx.db.opts.logger.Tracef("delete key %q", uk)
	if err := tx.txn.Delete(ctx, uk); err != nil {
		return err
	}
	urk := protodb.UIDRevKey(k)
	tx.db.opts.logger.Tracef("delete key %q", urk)
	if err := tx.txn.Delete(ctx, urk); err != nil {
		return err
	}
	tx.db.opts.logger.Tracef("delete key %q", k)
	return tx.txn.Delete(ctx, k)
}

func (tx *tx) deleteRaw(ctx context.Context, key []byte) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return err
	}
	tx.db.opts.logger.Tracef("delete key %q", string(key))
	return tx.txn.Delete(ctx, key)
}

func (tx *tx) uid(ctx context.Context, key []byte, inc bool) (uint64, bool, error) {
	urk := protodb.UIDRevKey(key)
	item, err := tx.txn.Get(ctx, urk)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, err
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		var uid uint64
		if err := item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid uid value for key %q: %s", string(urk), string(val))
			}
			uid = y.BytesToU64(val)
			return nil
		}); err != nil {
			return 0, false, err
		}
		return uid, true, nil
	}
	if !inc {
		return 0, false, nil
	}
	uid := tx.db.uid.Add(1)
	ulk := protodb.UIDLastKey()
	if err := tx.txn.Set(ctx, ulk, y.U64ToBytes(uid), 0); err != nil {
		return 0, false, err
	}
	return uid, false, nil
}

func (tx *tx) Commit(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.Commit")
		defer span.End()
	}
	if tx.closed() {
		return badger.ErrDBClosed
	}
	defer tx.close()
	if !tx.update {
		return nil
	}

	span.SetAttributes(
		attribute.Int64("operations", tx.count),
		attribute.Int64("size", tx.size),
	)
	tx.db.opts.logger.Debugf("committing")
	if err := tx.txn.Commit(ctx); err != nil {
		span.RecordError(err)
		metrics.Tx.ErrorsCounter.WithLabelValues("").Inc()
		return err
	}
	return nil
}

func (tx *tx) Close() {
	tx.txn.Close(context.Background())
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
	defer tx.span.End()
	tx.m.Lock()
	tx.me.End()
	metrics.Tx.OpCountHist.Observe(float64(tx.count))
	metrics.Tx.SizeHist.Observe(float64(tx.size))
	tx.txn.Close(context.Background())
	tx.done = true
	tx.m.Unlock()
}

func fieldMaskTouchesIndexed(md protoreflect.MessageDescriptor, maskPaths []string, entries []string) bool {
	if len(maskPaths) == 0 || len(entries) == 0 {
		return true
	}
	indexed := make([]string, 0, len(entries))
	for _, e := range entries {
		p := e
		if i := strings.IndexByte(e, '|'); i >= 0 {
			p = e[:i]
		}
		if p != "" {
			indexed = append(indexed, p)
		}
	}
	if len(indexed) == 0 {
		return false
	}
	for _, mp := range maskPaths {
		np, err := fieldMaskPathToNumberPath(md, mp)
		if err != nil {
			return true
		}
		for _, ip := range indexed {
			if np == ip || strings.HasPrefix(np, ip+".") || strings.HasPrefix(ip, np+".") {
				return true
			}
		}
	}
	return false
}

func fieldMaskPathToNumberPath(md protoreflect.MessageDescriptor, p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("empty field mask path")
	}
	parts := strings.Split(p, ".")
	cur := md
	nums := make([]string, 0, len(parts))
	for i, part := range parts {
		fd := cur.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return "", fmt.Errorf("field mask path %q not found", p)
		}
		nums = append(nums, fmt.Sprintf("%d", fd.Number()))
		if i == len(parts)-1 {
			break
		}
		if fd.Kind() != protoreflect.MessageKind {
			return "", fmt.Errorf("field mask path %q is invalid", p)
		}
		cur = fd.Message()
	}
	return strings.Join(nums, "."), nil
}

func stringToBytesNoCopy(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

type orderField struct {
	path      []protoreflect.FieldDescriptor
	fieldPath string
	direction pb.OrderDirection
	key       bool
	seconds   protoreflect.FieldDescriptor
	nanos     protoreflect.FieldDescriptor
}

type orderedResult struct {
	msg proto.Message
	key []byte
}

func (tx *tx) getOrdered(ctx context.Context, m proto.Message, o protodb.GetOpts) ([]proto.Message, *protodb.PagingInfo, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.getOrdered")
		span.SetAttributes(
			attribute.String("message", string(m.ProtoReflect().Descriptor().FullName())),
			attribute.Bool("filtered", o.Filter != nil),
			attribute.Bool("continuation", o.Paging.GetToken() != ""),
		)
		defer span.End()
	}
	plan, err := buildOrderPlan(m.ProtoReflect().New(), o.OrderBy)
	if err != nil {
		return nil, nil, err
	}
	hasContinuationToken := o.Paging.GetToken() != ""
	inToken := &token.Token{}
	if err := inToken.Decode(o.Paging.GetToken()); err != nil {
		return nil, nil, err
	}
	fhash, err := hash(o.Filter)
	if err != nil {
		return nil, nil, fmt.Errorf("hash filter: %w", err)
	}
	ohash := orderHash(plan)
	outToken := &token.Token{
		Ts:          tx.txn.ReadTs(),
		Type:        string(m.ProtoReflect().Descriptor().FullName()),
		FiltersHash: fhash,
		Reverse:     false,
		OrderHash:   ohash,
	}
	if err := outToken.ValidateFor(inToken); err != nil {
		return nil, nil, err
	}
	out, info, ok, err := tx.getOrderedIndexed(ctx, m, o, plan, inToken, outToken, hasContinuationToken)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return out, info, nil
	}
	return tx.getOrderedFallbackScanSort(ctx, m, o, plan, inToken, outToken, hasContinuationToken)
}

func (tx *tx) getOrderedIndexed(ctx context.Context, m proto.Message, o protodb.GetOpts, plan orderField, inToken, outToken *token.Token, hasContinuationToken bool) ([]proto.Message, *protodb.PagingInfo, bool, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.getOrderedIndexed")
		span.SetAttributes(
			attribute.String("message", string(m.ProtoReflect().Descriptor().FullName())),
			attribute.String("order_field", plan.fieldPath),
			attribute.Bool("order_desc", plan.direction == pb.OrderDirectionDesc),
			attribute.Bool("filtered", o.Filter != nil),
			attribute.Bool("continuation", hasContinuationToken),
		)
		defer span.End()
	}
	if plan.key {
		span.SetAttributes(attribute.Bool("fallback", true), attribute.String("fallback_reason", "key_order"))
		return nil, nil, false, nil
	}
	prefix, _, _, _ := protodb.DataPrefix(m)
	if o.Filter != nil {
		ok, err := tx.db.idx.IndexableFilter(m, o.Filter)
		if err != nil {
			return nil, nil, false, err
		}
		if !ok {
			span.SetAttributes(attribute.Bool("fallback", true), attribute.String("fallback_reason", "non_indexable_filter"))
			return nil, nil, false, nil
		}
	}
	var allowed bitmap.Bitmap
	if o.Filter != nil {
		uids, err := tx.db.idx.FindUIDs(ctx, tx, m.ProtoReflect().Descriptor().FullName(), o.Filter, idxstore.FindOpts{})
		if err != nil {
			return nil, nil, false, err
		}
		allowed = bitmap.NewWith(len(uids))
		for _, uid := range uids {
			allowed.Set(uid)
		}
	}
	windowLimit := o.Paging.GetLimit()
	if o.One && (windowLimit == 0 || windowLimit > 1) {
		windowLimit = 1
	}
	off := o.Paging.GetOffset()
	count := uint64(0)
	selectedKeys := make([][]byte, 0)
	hasNext := false
	continuationFound := !hasContinuationToken
	seq, err := tx.db.idx.OrderedUIDGroupsSeq(ctx, tx, m.ProtoReflect().Descriptor().FullName(), plan.path, plan.direction == pb.OrderDirectionDesc)
	if err != nil {
		return nil, nil, false, err
	}
	groupsSeen := 0
	for uids, err := range seq {
		if err != nil {
			return nil, nil, false, err
		}
		groupsSeen++
		keys := make([][]byte, 0, len(uids))
		for _, uid := range uids {
			if err := ctx.Err(); err != nil {
				return nil, nil, false, err
			}
			if allowed != nil && !allowed.Contains(uid) {
				continue
			}
			uidItem, err := tx.txn.Get(ctx, protodb.UIDKey(uid))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return nil, nil, false, err
			}
			var fullKey []byte
			if err := uidItem.Value(func(val []byte) error {
				fullKey = append(fullKey[:0], val...)
				return nil
			}); err != nil {
				return nil, nil, false, err
			}
			if !bytes.HasPrefix(fullKey, prefix) {
				continue
			}
			keys = append(keys, fullKey)
		}
		slices.SortStableFunc(keys, func(a, b []byte) int { return bytes.Compare(a, b) })
		for _, key := range keys {
			if hasContinuationToken && !continuationFound {
				if bytes.Equal(key, inToken.GetLastPrefix()) {
					continuationFound = true
				}
				continue
			}
			count++
			if !hasContinuationToken && off > 0 && count <= off {
				continue
			}
			if windowLimit > 0 && uint64(len(selectedKeys)) >= windowLimit {
				hasNext = true
				break
			}
			selectedKeys = append(selectedKeys, key)
		}
		if hasNext || (o.One && len(selectedKeys) > 0) {
			break
		}
	}
	if hasContinuationToken && !continuationFound {
		return nil, nil, false, fmt.Errorf("%w: continuation key not found", token.ErrInvalid)
	}
	out := make([]proto.Message, 0, len(selectedKeys))
	for _, key := range selectedKeys {
		item, err := tx.txn.Get(ctx, key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return nil, nil, false, err
		}
		v := m.ProtoReflect().New().Interface()
		if err := item.Value(func(val []byte) error { return tx.db.unmarshal(val, v) }); err != nil {
			return nil, nil, false, err
		}
		if o.FieldMask != nil {
			if err := FilterFieldMask(v, o.FieldMask); err != nil {
				return nil, nil, false, err
			}
		}
		tx.txn.AddReadKey(key)
		out = append(out, v)
		outToken.LastPrefix = append(outToken.LastPrefix[:0], key...)
	}
	tks, err := outToken.Encode()
	if err != nil {
		return nil, nil, false, err
	}
	span.SetAttributes(
		attribute.Bool("fallback", false),
		attribute.Int("groups_seen", groupsSeen),
		attribute.Int("selected_keys", len(selectedKeys)),
		attribute.Bool("has_next", hasNext),
	)
	return out, &protodb.PagingInfo{HasNext: hasNext, Token: tks}, true, nil
}

func (tx *tx) getOrderedFallbackScanSort(ctx context.Context, m proto.Message, o protodb.GetOpts, plan orderField, inToken, outToken *token.Token, hasContinuationToken bool) ([]proto.Message, *protodb.PagingInfo, error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		ctx, span = tracer.Start(ctx, "Tx.getOrderedFallbackScanSort")
		span.SetAttributes(
			attribute.String("message", string(m.ProtoReflect().Descriptor().FullName())),
			attribute.String("order_field", plan.fieldPath),
			attribute.Bool("order_desc", plan.direction == pb.OrderDirectionDesc),
			attribute.Bool("filtered", o.Filter != nil),
			attribute.Bool("continuation", hasContinuationToken),
		)
		defer span.End()
	}

	baseOpts := make([]protodb.GetOption, 0, 1)
	if o.Filter != nil {
		baseOpts = append(baseOpts, protodb.WithFilter(o.Filter))
	}
	all, _, err := tx.get(ctx, m, baseOpts...)
	if err != nil {
		return nil, nil, err
	}

	ordered := make([]orderedResult, 0, len(all))
	for _, item := range all {
		key, _, _, err := protodb.DataPrefix(item)
		if err != nil {
			return nil, nil, err
		}
		ordered = append(ordered, orderedResult{msg: item, key: append([]byte(nil), key...)})
	}

	slices.SortStableFunc(ordered, func(a, b orderedResult) int {
		av, aok := fieldValue(a.msg.ProtoReflect(), plan.path)
		bv, bok := fieldValue(b.msg.ProtoReflect(), plan.path)
		c := compareMaybeValue(plan, av, aok, bv, bok)
		if c == 0 {
			return bytes.Compare(a.key, b.key)
		}
		if plan.direction == pb.OrderDirectionDesc {
			return -c
		}
		return c
	})

	start := 0
	if hasContinuationToken {
		if len(inToken.GetLastPrefix()) == 0 {
			return nil, nil, fmt.Errorf("%w: missing continuation key", token.ErrInvalid)
		}
		found := false
		for i, item := range ordered {
			if bytes.Equal(item.key, inToken.GetLastPrefix()) {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("%w: continuation key not found", token.ErrInvalid)
		}
	} else if off := o.Paging.GetOffset(); off > 0 {
		start = int(min(off, uint64(len(ordered))))
	}

	windowLimit := o.Paging.GetLimit()
	if o.One && (windowLimit == 0 || windowLimit > 1) {
		windowLimit = 1
	}

	end := len(ordered)
	if windowLimit > 0 {
		end = min(end, start+int(windowLimit))
	}
	hasNext := end < len(ordered)

	out := make([]proto.Message, 0, end-start)
	for _, item := range ordered[start:end] {
		v := item.msg
		if o.FieldMask != nil {
			if err := FilterFieldMask(v, o.FieldMask); err != nil {
				return nil, nil, err
			}
		}
		tx.txn.AddReadKey(item.key)
		out = append(out, v)
		outToken.LastPrefix = append(outToken.LastPrefix[:0], item.key...)
	}
	tks, err := outToken.Encode()
	if err != nil {
		return nil, nil, err
	}
	span.SetAttributes(
		attribute.Int("ordered_candidates", len(ordered)),
		attribute.Int("selected", len(out)),
		attribute.Bool("has_next", hasNext),
	)
	return out, &protodb.PagingInfo{HasNext: hasNext, Token: tks}, nil
}

func buildOrderPlan(msg protoreflect.Message, orderBy *pb.OrderBy) (orderField, error) {
	if orderBy == nil {
		return orderField{}, errors.New("order_by cannot be empty")
	}
	md := msg.Descriptor()
	keyField, hasKey := protodb.KeyFieldName(md)
	fieldPath := strings.TrimSpace(orderBy.GetField())
	if fieldPath == "" {
		return orderField{}, errors.New("order_by field cannot be empty")
	}
	fds, err := pfreflect.Lookup(msg, fieldPath)
	if err != nil {
		return orderField{}, fmt.Errorf("invalid order_by field %q: %w", fieldPath, err)
	}
	if !isOrderableFieldPath(fds) {
		return orderField{}, fmt.Errorf("order_by field %q is not sortable", fieldPath)
	}
	isKey := hasKey && len(fds) == 1 && string(fds[0].Name()) == keyField
	if !isKey {
		fd := fds[len(fds)-1]
		if !proto.HasExtension(fd.Options(), protopts.E_Index) {
			return orderField{}, fmt.Errorf("order_by field %q must be indexed", fieldPath)
		}
	}
	direction := orderBy.GetDirection()
	if direction == pb.OrderDirectionUnspecified {
		direction = pb.OrderDirectionAsc
	}
	if direction != pb.OrderDirectionAsc && direction != pb.OrderDirectionDesc {
		return orderField{}, fmt.Errorf("order_by field %q has invalid direction %v", fieldPath, orderBy.GetDirection())
	}
	pl := orderField{path: fds, fieldPath: fieldPath, direction: direction, key: isKey}
	if fds[len(fds)-1].Kind() == protoreflect.MessageKind {
		mfd := fds[len(fds)-1]
		if n := mfd.Message().FullName(); n == "google.protobuf.Timestamp" || n == "google.protobuf.Duration" {
			pl.seconds = mfd.Message().Fields().Get(0)
			pl.nanos = mfd.Message().Fields().Get(1)
		}
	}
	return pl, nil
}

func isOrderableFieldPath(fds []protoreflect.FieldDescriptor) bool {
	if len(fds) == 0 {
		return false
	}
	for i, fd := range fds {
		if fd.IsMap() || fd.IsList() {
			return false
		}
		if i == len(fds)-1 {
			break
		}
		if fd.Kind() != protoreflect.MessageKind || pfreflect.IsWKType(fd.Message().FullName()) {
			return false
		}
	}
	return isOrderableLeaf(fds[len(fds)-1])
}

func isOrderableLeaf(fd protoreflect.FieldDescriptor) bool {
	return idxstore.IsIndexableLeaf(fd)
}

func fieldValue(msg protoreflect.Message, path []protoreflect.FieldDescriptor) (protoreflect.Value, bool) {
	cur := msg
	for i, fd := range path {
		v := cur.Get(fd)
		if i == len(path)-1 {
			if fd.Kind() == protoreflect.MessageKind && !v.Message().IsValid() {
				return protoreflect.Value{}, false
			}
			return v, true
		}
		if fd.Kind() != protoreflect.MessageKind || !v.Message().IsValid() {
			return protoreflect.Value{}, false
		}
		cur = v.Message()
	}
	return protoreflect.Value{}, false
}

func compareMaybeValue(of orderField, av protoreflect.Value, aok bool, bv protoreflect.Value, bok bool) int {
	if !aok && !bok {
		return 0
	}
	if !aok {
		return -1
	}
	if !bok {
		return 1
	}
	return compareValue(of, av, bv)
}

func compareValue(of orderField, av, bv protoreflect.Value) int {
	fd := of.path[len(of.path)-1]
	switch fd.Kind() {
	case protoreflect.BoolKind:
		ab, bb := av.Bool(), bv.Bool()
		switch {
		case ab == bb:
			return 0
		case !ab && bb:
			return -1
		default:
			return 1
		}
	case protoreflect.StringKind:
		return strings.Compare(av.String(), bv.String())
	case protoreflect.BytesKind:
		return bytes.Compare(av.Bytes(), bv.Bytes())
	case protoreflect.EnumKind:
		return cmp.Compare(int32(av.Enum()), int32(bv.Enum()))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return cmp.Compare(av.Int(), bv.Int())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return cmp.Compare(av.Uint(), bv.Uint())
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return cmp.Compare(av.Float(), bv.Float())
	case protoreflect.MessageKind:
		am := av.Message()
		bm := bv.Message()
		asec := am.Get(of.seconds).Int()
		bsec := bm.Get(of.seconds).Int()
		if c := cmp.Compare(asec, bsec); c != 0 {
			return c
		}
		ananos := am.Get(of.nanos).Int()
		bnanos := bm.Get(of.nanos).Int()
		return cmp.Compare(ananos, bnanos)
	default:
		return 0
	}
}

func orderHash(orderBy orderField) string {
	if orderBy.fieldPath == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(orderBy.fieldPath)
	b.WriteByte(':')
	b.WriteString(fmt.Sprintf("%d", orderBy.direction))
	b.WriteByte(';')
	h := sha512.Sum512([]byte(b.String()))
	return base64.StdEncoding.EncodeToString(h[:])
}

func hash(f protodb.Filter) (hash string, err error) {
	var b []byte
	if f != nil {
		b, err = f.Expr().MarshalVT()
		if err != nil {
			return "", err
		}
	}
	h := sha512.Sum512(b)
	return base64.StdEncoding.EncodeToString(h[:]), nil
}
