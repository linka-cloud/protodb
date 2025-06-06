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
	pf "go.linka.cloud/protofilters"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/token"
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

func (tx *tx) GetOne(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (proto.Message, bool, error) {
	return protodb.GetOne(ctx, tx, m, opts...)
}

func (tx *tx) Get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (out []proto.Message, info *protodb.PagingInfo, err error) {
	ctx, span := tracer.Start(ctx, "Tx.Get", trace.WithAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName()))))
	defer span.End()
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
			match, err := pf.Match(v, o.Filter)
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
		key := string(item.Key()[len(prefix):])
		if item.Version() <= inToken.Ts &&
			count < o.Paging.GetOffset() {
			// could be inlined, but it is easier to read this way
			if !o.Reverse && bytes.Compare(item.Key(), inToken.GetLastPrefix()) <= 0 {
				continue
			}
			if o.Reverse && bytes.Compare(item.Key(), inToken.GetLastPrefix()) >= 0 {
				continue
			}
		}

		tx.db.opts.logger.Tracef("checking %q", string(item.Key()))
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
				size = len(val)
				if err := tx.db.unmarshal(val, v); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return nil, nil, err
			}
		}
		tx.db.opts.logger.Tracef("key %q match filter", string(item.Key()))
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
		tx.txn.AddReadKey(item.Key())
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
	return item.ValueCopy(nil)
}

func (tx *tx) Set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	ctx, span := tracer.Start(ctx, "Tx.Set", trace.WithAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName()))))
	defer span.End()
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
	span.SetAttributes(
		attribute.String("prefix", string(k)),
		attribute.String("key", value),
		attribute.String("key_field", field),
	)
	if o.FieldMask != nil {
		span.SetAttributes(attribute.StringSlice("field_mask", o.FieldMask.GetPaths()))
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
	s := int64(len(b))
	span.SetAttributes(attribute.Int64("size", s))
	tx.size += s
	e := badger.NewEntry(k, b)
	if o.TTL != 0 {
		e = e.WithTTL(o.TTL)
		span.SetAttributes(attribute.String("ttl", o.TTL.String()))
	}
	if err := ctx.Err(); err != nil {
		tx.close()
		return nil, err
	}
	tx.db.opts.logger.Tracef("set key %q", string(k))
	return m, tx.txn.Set(ctx, e.Key, e.Value, e.ExpiresAt)
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
	ctx, span := tracer.Start(ctx, "Tx.Delete", trace.WithAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName()))))
	defer span.End()
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

	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("key", value),
		attribute.String("key_field", field),
	)

	if err := ctx.Err(); err != nil {
		tx.close()
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

func (tx *tx) Commit(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Tx.Commit")
	defer span.End()
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
