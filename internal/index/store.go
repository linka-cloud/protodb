// Copyright 2025 Linka Cloud  All rights reserved.
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
	"errors"
	"fmt"
	"iter"
	"math"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pfindex "go.linka.cloud/protofilters/index"
	"go.linka.cloud/protofilters/index/bitmap"
	_ "go.linka.cloud/protofilters/index/bitmap/sroar"
	pfreflect "go.linka.cloud/protofilters/reflect"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
)

const (
	uidShardShift = 32
	uidShardSize  = 4
	uidSize       = 8

	deltaAdd    = byte(1)
	deltaRemove = byte(2)
)

var tracer = otel.Tracer("protodb.index")

func maybeStartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func()) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, func() {}
	}
	var span trace.Span
	if len(attrs) == 0 {
		ctx, span = tracer.Start(ctx, name)
	} else {
		ctx, span = tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	}
	return ctx, func() { span.End() }
}

type Store struct {
	db       badgerd.DB
	resolver protodesc.Resolver
}

type UIDTxStore struct {
	txn      badgerd.Tx
	resolver protodesc.Resolver
}

func newUIDTxStore(txn badgerd.Tx, resolver protodesc.Resolver) *UIDTxStore {
	return &UIDTxStore{txn: txn, resolver: resolver}
}

func (s *UIDTxStore) Tx(ctx context.Context) (pfindex.UIDTx, error) {
	ctx, end := maybeStartSpan(ctx, "Index.UIDTxStore.Tx")
	defer end()
	return &tx{store: &Store{resolver: s.resolver}, txn: s.txn, owned: false}, nil
}

func (s *UIDTxStore) For(ctx context.Context, name protoreflect.FullName) (pfindex.FieldReader, error) {
	ctx, end := maybeStartSpan(ctx, "Index.UIDTxStore.For", attribute.String("message", string(name)))
	defer end()
	return buildFieldReader(s.txn, s.resolver, name, true, nil)
}

func (s *UIDTxStore) AddUID(ctx context.Context, uid uint64, v protoreflect.Value, fds ...protoreflect.FieldDescriptor) error {
	ctx, end := maybeStartSpan(ctx, "Index.UIDTxStore.AddUID", attribute.Int64("uid", int64(uid)))
	defer end()
	return (&tx{store: &Store{resolver: s.resolver}, txn: s.txn, owned: false}).AddUID(ctx, uid, v, fds...)
}

func (s *UIDTxStore) RemoveUID(ctx context.Context, uid uint64, f protoreflect.FieldDescriptor, v protoreflect.Value) error {
	ctx, end := maybeStartSpan(ctx, "Index.UIDTxStore.RemoveUID", attribute.Int64("uid", int64(uid)))
	defer end()
	return (&tx{store: &Store{resolver: s.resolver}, txn: s.txn, owned: false}).RemoveUID(ctx, uid, f, v)
}

func (s *UIDTxStore) ClearUID(ctx context.Context, uid uint64) error {
	ctx, end := maybeStartSpan(ctx, "Index.UIDTxStore.ClearUID", attribute.Int64("uid", int64(uid)))
	defer end()
	return (&tx{store: &Store{resolver: s.resolver}, txn: s.txn, owned: false}).ClearUID(ctx, uid)
}

func (s *Store) Tx(ctx context.Context) (pfindex.UIDTx, error) {
	ctx, end := maybeStartSpan(ctx, "Index.Store.Tx")
	defer end()
	txn, err := s.db.NewTransaction(ctx, true)
	if err != nil {
		return nil, err
	}
	return &tx{store: s, txn: txn, owned: true}, nil
}

func (s *Store) For(ctx context.Context, name protoreflect.FullName) (pfindex.FieldReader, error) {
	ctx, end := maybeStartSpan(ctx, "Index.Store.For", attribute.String("message", string(name)))
	defer end()
	txn, err := s.db.NewTransaction(ctx, false)
	if err != nil {
		return nil, err
	}
	defer txn.Close(ctx)
	reader, err := buildFieldReader(txn, s.resolver, name, true, nil)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (s *Store) AddUID(ctx context.Context, uid uint64, v protoreflect.Value, fds ...protoreflect.FieldDescriptor) error {
	ctx, end := maybeStartSpan(ctx, "Index.Store.AddUID", attribute.Int64("uid", int64(uid)))
	defer end()
	txn, err := s.db.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer txn.Close(ctx)
	if err := (&tx{store: s, txn: txn}).AddUID(ctx, uid, v, fds...); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func (s *Store) RemoveUID(ctx context.Context, uid uint64, f protoreflect.FieldDescriptor, v protoreflect.Value) error {
	ctx, end := maybeStartSpan(ctx, "Index.Store.RemoveUID", attribute.Int64("uid", int64(uid)))
	defer end()
	txn, err := s.db.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer txn.Close(ctx)
	if err := (&tx{store: s, txn: txn}).RemoveUID(ctx, uid, f, v); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func (s *Store) ClearUID(ctx context.Context, uid uint64) error {
	ctx, end := maybeStartSpan(ctx, "Index.Store.ClearUID", attribute.Int64("uid", int64(uid)))
	defer end()
	txn, err := s.db.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer txn.Close(ctx)
	if err := (&tx{store: s, txn: txn}).ClearUID(ctx, uid); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

type tx struct {
	store *Store
	txn   badgerd.Tx
	owned bool
}

func (t *tx) Commit(ctx context.Context) error {
	if !t.owned {
		return nil
	}
	return t.txn.Commit(ctx)
}

func (t *tx) Close() error {
	if !t.owned {
		return nil
	}
	return t.txn.Close(context.Background())
}

func (t *tx) For(ctx context.Context, name protoreflect.FullName) (pfindex.FieldReader, error) {
	ctx, end := maybeStartSpan(ctx, "Index.Tx.For", attribute.String("message", string(name)))
	defer end()
	return buildFieldReader(t.txn, t.store.resolver, name, false, t)
}

func (t *tx) AddUID(ctx context.Context, uid uint64, v protoreflect.Value, fds ...protoreflect.FieldDescriptor) error {
	if len(fds) == 0 {
		return nil
	}
	fieldPath := fieldPathFromNumbers(fds)
	encoded, err := encodeValue(fds[len(fds)-1], v)
	if err != nil {
		return err
	}
	shard, low := uidShard(uid)
	keyBytes := deltaKey(fds[0].ContainingMessage().FullName(), fieldPath, encoded, shard, uint64(low))
	return t.writeDelta(ctx, keyBytes, deltaAdd)
}

func (t *tx) RemoveUID(ctx context.Context, uid uint64, f protoreflect.FieldDescriptor, v protoreflect.Value) error {
	if f.IsMap() {
		return nil
	}
	if f.Kind() == protoreflect.MessageKind && !pfreflect.IsWKType(f.Message().FullName()) {
		return nil
	}
	fieldPath := fieldPathFromNumbers([]protoreflect.FieldDescriptor{f})
	encoded, err := encodeValue(f, v)
	if err != nil {
		return err
	}
	shard, low := uidShard(uid)
	keyBytes := deltaKey(f.ContainingMessage().FullName(), fieldPath, encoded, shard, uint64(low))
	return t.writeDelta(ctx, keyBytes, deltaRemove)
}

func (t *tx) ClearUID(ctx context.Context, uid uint64) error {
	shard, low := uidShard(uid)
	for _, pfx := range [][]byte{[]byte(protodb.Index + "/"), []byte(protodb.IndexDelta + "/")} {
		it := t.txn.Iterator(badger.IteratorOptions{Prefix: pfx, PrefetchValues: false})
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if bytes.HasPrefix(k, []byte(protodb.IndexDelta+"/")) {
				uid2, ok := parseDeltaUID(k)
				if !ok || uid2 != uid {
					continue
				}
				if err := t.writeDelta(ctx, k, deltaRemove); err != nil {
					it.Close()
					return err
				}
				continue
			}
			if !bytes.HasSuffix(k, shardSuffix(shard)) {
				continue
			}
			keyBytes := deltaKeyFromIndexKey(k, uint64(low))
			if keyBytes == nil {
				continue
			}
			if err := t.writeDelta(ctx, keyBytes, deltaRemove); err != nil {
				it.Close()
				return err
			}
		}
		it.Close()
	}
	return nil
}

func (t *tx) readBitmap(ctx context.Context, key []byte) (bitmap.Bitmap, error) {
	item, err := t.txn.Get(ctx, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
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

func (t *tx) writeBitmap(ctx context.Context, key []byte, bm bitmap.Bitmap) error {
	return t.txn.Set(ctx, key, bm.Bytes(), 0)
}

func (t *tx) writeDelta(ctx context.Context, key []byte, op byte) error {
	return t.txn.Set(ctx, key, []byte{op}, 0)
}

type fieldReader struct {
	fields map[protoreflect.Name][]pfindex.Field
	key    *keyField
}

func (f *fieldReader) Get(_ context.Context, n protoreflect.Name) iter.Seq2[pfindex.Field, error] {
	if f.key != nil && f.key.name == n {
		return func(yield func(pfindex.Field, error) bool) {
			f.key.iterate(yield)
		}
	}
	return func(yield func(pfindex.Field, error) bool) {
		for _, v := range f.fields[n] {
			if !yield(v, nil) {
				return
			}
		}
	}
}

type keyField struct {
	name       protoreflect.Name
	fds        []protoreflect.FieldDescriptor
	dataPrefix []byte
	txn        badgerd.Tx
	entries    []keyEntry
	built      bool
}

type keyEntry struct {
	value  string
	bitmap bitmap.Bitmap
}

func (k *keyField) iterate(yield func(pfindex.Field, error) bool) {
	if k == nil {
		return
	}
	if !k.built {
		prefix := []byte(protodb.UID + "/")
		it := k.txn.Iterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) < len(prefix)+8 {
				continue
			}
			uid := y.BytesToU64(key[len(prefix):])
			var dataKey []byte
			if err := item.Value(func(val []byte) error {
				dataKey = append(dataKey[:0], val...)
				return nil
			}); err != nil {
				it.Close()
				if !yield(nil, err) {
					return
				}
				return
			}
			if !bytes.HasPrefix(dataKey, k.dataPrefix) {
				continue
			}
			keyValue := string(dataKey[len(k.dataPrefix):])
			bm := bitmap.NewWith(2)
			bm.Set(uid)
			k.entries = append(k.entries, keyEntry{value: keyValue, bitmap: bm})
		}
		it.Close()
		k.built = true
	}
	for _, e := range k.entries {
		entry := &field{
			descriptors: k.fds,
			value:       protoreflect.ValueOfString(e.value),
			bitmap:      e.bitmap,
		}
		if !yield(entry, nil) {
			return
		}
	}
}

type field struct {
	tx          *tx
	value       protoreflect.Value
	descriptors []protoreflect.FieldDescriptor
	prefix      []byte
	bitmap      bitmap.Bitmap
}

func (f *field) Value() protoreflect.Value {
	return f.value
}

func (f *field) Descriptors() []protoreflect.FieldDescriptor {
	return f.descriptors
}

func (f *field) Bitmap(ctx context.Context) (bitmap.Bitmap, error) {
	if f.bitmap != nil {
		return f.bitmap, nil
	}
	if f.tx == nil {
		return nil, errors.New("bitmap unavailable without transaction")
	}
	return readValueBitmap(f.tx.txn, f.prefix)
}

func typePrefix(name protoreflect.FullName) []byte {
	return []byte(protodb.Index + "/" + string(name) + "/")
}

func deltaTypePrefix(name protoreflect.FullName) []byte {
	return []byte(protodb.IndexDelta + "/" + string(name) + "/")
}

func valuePrefix(name protoreflect.FullName, fieldPath string, encodedValue []byte) []byte {
	fieldPathBytes := []byte(fieldPath)
	prefix := make([]byte, 0, len(typePrefix(name))+8+len(fieldPathBytes)+len(encodedValue))
	prefix = append(prefix, typePrefix(name)...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(fieldPathBytes)))
	prefix = append(prefix, lenBuf...)
	prefix = append(prefix, fieldPathBytes...)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(encodedValue)))
	prefix = append(prefix, lenBuf...)
	prefix = append(prefix, encodedValue...)
	return prefix
}

func deltaValuePrefix(name protoreflect.FullName, fieldPath string, encodedValue []byte) []byte {
	fieldPathBytes := []byte(fieldPath)
	prefix := make([]byte, 0, len(deltaTypePrefix(name))+8+len(fieldPathBytes)+len(encodedValue))
	prefix = append(prefix, deltaTypePrefix(name)...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(fieldPathBytes)))
	prefix = append(prefix, lenBuf...)
	prefix = append(prefix, fieldPathBytes...)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(encodedValue)))
	prefix = append(prefix, lenBuf...)
	prefix = append(prefix, encodedValue...)
	return prefix
}

func deltaValuePrefixFromBase(prefix []byte) []byte {
	base := []byte(protodb.Index + "/")
	delta := []byte(protodb.IndexDelta + "/")
	if !bytes.HasPrefix(prefix, base) {
		return nil
	}
	out := make([]byte, 0, len(delta)+len(prefix)-len(base))
	out = append(out, delta...)
	out = append(out, prefix[len(base):]...)
	return out
}

func shardKey(name protoreflect.FullName, fieldPath string, encodedValue []byte, shard uint32) []byte {
	prefix := valuePrefix(name, fieldPath, encodedValue)
	return append(prefix, shardSuffix(shard)...)
}

func deltaKey(name protoreflect.FullName, fieldPath string, encodedValue []byte, shard uint32, uid uint64) []byte {
	prefix := deltaValuePrefix(name, fieldPath, encodedValue)
	key := append(prefix, shardSuffix(shard)...)
	buf := make([]byte, uidSize)
	binary.BigEndian.PutUint64(buf, uid)
	return append(key, buf...)
}

func deltaKeyFromIndexKey(key []byte, uid uint64) []byte {
	if bytes.HasPrefix(key, []byte(protodb.IndexDelta+"/")) {
		return key
	}
	if !bytes.HasPrefix(key, []byte(protodb.Index+"/")) {
		return nil
	}
	base := append([]byte{}, []byte(protodb.IndexDelta)...)
	base = append(base, key[len(protodb.Index):]...)
	buf := make([]byte, uidSize)
	binary.BigEndian.PutUint64(buf, uid)
	return append(base, buf...)
}

func shardSuffix(shard uint32) []byte {
	buf := make([]byte, uidShardSize)
	binary.BigEndian.PutUint32(buf, shard)
	return buf
}

func parseShard(key []byte) (uint32, bool) {
	if len(key) < uidShardSize {
		return 0, false
	}
	return binary.BigEndian.Uint32(key[len(key)-uidShardSize:]), true
}

func parseDeltaUID(key []byte) (uint64, bool) {
	if len(key) < uidShardSize+uidSize {
		return 0, false
	}
	uid := binary.BigEndian.Uint64(key[len(key)-uidSize:])
	shard := binary.BigEndian.Uint32(key[len(key)-uidSize-uidShardSize : len(key)-uidSize])
	return (uint64(shard) << uidShardShift) | uid, true
}

func parseFieldValue(prefix, key []byte) (string, []byte, bool) {
	return parseFieldValueWithSuffix(prefix, key, uidShardSize)
}

func parseFieldValueWithSuffix(prefix, key []byte, suffixLen int) (string, []byte, bool) {
	if !bytes.HasPrefix(key, prefix) {
		return "", nil, false
	}
	rest := key[len(prefix):]
	if len(rest) <= suffixLen+8 {
		return "", nil, false
	}
	valueEnd := len(rest) - suffixLen
	if valueEnd < 8 {
		return "", nil, false
	}
	fieldLen := int(binary.BigEndian.Uint32(rest[:4]))
	if fieldLen < 0 || 4+fieldLen+4 > valueEnd {
		return "", nil, false
	}
	fieldStart := 4
	fieldStop := fieldStart + fieldLen
	valueLen := int(binary.BigEndian.Uint32(rest[fieldStop : fieldStop+4]))
	valueStart := fieldStop + 4
	valueStop := valueStart + valueLen
	if valueLen < 0 || valueStop > valueEnd {
		return "", nil, false
	}
	fieldPath := string(rest[fieldStart:fieldStop])
	value := rest[valueStart:valueStop]
	return fieldPath, value, true
}

func uidShard(uid uint64) (uint32, uint32) {
	return uint32(uid >> uidShardShift), uint32(uid)
}

func fieldPathFromNumbers(fds []protoreflect.FieldDescriptor) string {
	parts := make([]string, 0, len(fds))
	for _, fd := range fds {
		parts = append(parts, fmt.Sprintf("%d", fd.Number()))
	}
	return strings.Join(parts, ".")
}

func fieldPathFromNames(fds []protoreflect.FieldDescriptor) string {
	parts := make([]string, 0, len(fds))
	for _, fd := range fds {
		parts = append(parts, string(fd.Name()))
	}
	return strings.Join(parts, ".")
}

func lookupByNumberPath(md protoreflect.MessageDescriptor, fieldPath string) ([]protoreflect.FieldDescriptor, error) {
	return lookupByPath(md, fieldPath, func(cur protoreflect.MessageDescriptor, part string) (protoreflect.FieldDescriptor, error) {
		num, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid field number %q", part)
		}
		fd := cur.Fields().ByNumber(protoreflect.FieldNumber(num))
		if fd == nil {
			return nil, fmt.Errorf("%s does not contain field number %d", cur.FullName(), num)
		}
		return fd, nil
	})
}

func lookupByNamePath(md protoreflect.MessageDescriptor, fieldPath string) ([]protoreflect.FieldDescriptor, error) {
	return lookupByPath(md, fieldPath, func(cur protoreflect.MessageDescriptor, part string) (protoreflect.FieldDescriptor, error) {
		fd := cur.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return nil, fmt.Errorf("%s does not contain field '%s'", cur.FullName(), part)
		}
		return fd, nil
	})
}

func lookupByPath(md protoreflect.MessageDescriptor, fieldPath string, lookup func(protoreflect.MessageDescriptor, string) (protoreflect.FieldDescriptor, error)) ([]protoreflect.FieldDescriptor, error) {
	if fieldPath == "" {
		return nil, fmt.Errorf("empty field path")
	}
	parts := strings.Split(fieldPath, ".")
	var fds []protoreflect.FieldDescriptor
	cur := md
	for i, part := range parts {
		fd, err := lookup(cur, part)
		if err != nil {
			return nil, err
		}
		fds = append(fds, fd)
		if i == len(parts)-1 {
			continue
		}
		if fd.Kind() != protoreflect.MessageKind {
			return nil, fmt.Errorf("%s does not contain '%s'", md.FullName(), fieldPath)
		}
		cur = fd.Message()
	}
	return fds, nil
}

type keyFieldNamer interface {
	KeyFieldName(protoreflect.MessageDescriptor) (string, bool)
}

func keyFieldName(resolver protodesc.Resolver, md protoreflect.MessageDescriptor) (string, bool) {
	r, ok := resolver.(keyFieldNamer)
	if ok {
		return r.KeyFieldName(md)
	}
	return protodb.KeyFieldName(md)
}

func buildFieldReader(txn badgerd.Tx, resolver protodesc.Resolver, name protoreflect.FullName, precompute bool, owner *tx) (*fieldReader, error) {
	d, err := resolver.FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor %s is not a message", name)
	}
	var key *keyField
	keyName, ok := keyFieldName(resolver, md)
	if ok {
		fds, err := lookupByNamePath(md, keyName)
		if err == nil {
			key = &keyField{
				name:       protoreflect.Name(fieldPathFromNames(fds)),
				fds:        fds,
				dataPrefix: []byte(protodb.Data + "/" + string(name) + "/"),
				txn:        txn,
			}
		}
	}
	prefix := typePrefix(name)
	valuesByField := make(map[string]map[string]struct{})
	{
		it := txn.Iterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			fieldPath, value, ok := parseFieldValue(prefix, key)
			if !ok {
				continue
			}
			if _, exists := valuesByField[fieldPath]; !exists {
				valuesByField[fieldPath] = make(map[string]struct{})
			}
			valuesByField[fieldPath][string(value)] = struct{}{}
		}
		it.Close()
	}
	{
		deltaPrefix := deltaTypePrefix(name)
		it := txn.Iterator(badger.IteratorOptions{Prefix: deltaPrefix, PrefetchValues: false})
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			fieldPath, value, ok := parseFieldValueWithSuffix(deltaPrefix, key, uidShardSize+uidSize)
			if !ok {
				continue
			}
			if _, exists := valuesByField[fieldPath]; !exists {
				valuesByField[fieldPath] = make(map[string]struct{})
			}
			valuesByField[fieldPath][string(value)] = struct{}{}
		}
		it.Close()
	}
	fields := make(map[protoreflect.Name][]pfindex.Field)
	for fieldPath, values := range valuesByField {
		fds, err := lookupByNumberPath(md, fieldPath)
		if err != nil {
			return nil, err
		}
		namePath := fieldPathFromNames(fds)
		for value := range values {
			v, err := decodeValue(fds[len(fds)-1], []byte(value))
			if err != nil {
				return nil, err
			}
			entry := &field{
				tx:          owner,
				descriptors: fds,
				value:       v,
				prefix:      valuePrefix(name, fieldPath, []byte(value)),
			}
			if precompute {
				bm, err := readValueBitmap(txn, entry.prefix)
				if err != nil {
					return nil, err
				}
				entry.bitmap = bm
			}
			fields[protoreflect.Name(namePath)] = append(fields[protoreflect.Name(namePath)], entry)
		}
	}
	return &fieldReader{fields: fields, key: key}, nil
}

func readValueBitmap(txn badgerd.Tx, prefix []byte) (bitmap.Bitmap, error) {
	it := txn.Iterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
	defer it.Close()
	bm := bitmap.NewWith(1024)
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		shard, ok := parseShard(key)
		if !ok {
			continue
		}
		var data []byte
		if err := item.Value(func(val []byte) error {
			data = append(data[:0], val...)
			return nil
		}); err != nil {
			return nil, err
		}
		if len(data) == 0 {
			continue
		}
		rb := bitmap.NewFrom(data)
		for v := range rb.Iter() {
			uid := (uint64(shard) << uidShardShift) | v
			bm.Set(uid)
		}
	}
	deltaPrefix := deltaValuePrefixFromBase(prefix)
	if deltaPrefix != nil {
		it := txn.Iterator(badger.IteratorOptions{Prefix: deltaPrefix, PrefetchValues: false})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			uid, ok := parseDeltaUID(key)
			if !ok {
				continue
			}
			op := deltaAdd
			if err := it.Item().Value(func(val []byte) error {
				if len(val) > 0 {
					op = val[0]
				}
				return nil
			}); err != nil {
				return nil, err
			}
			switch op {
			case deltaAdd:
				bm.Set(uid)
			case deltaRemove:
				bm.Remove(uid)
			}
		}
	}
	return bm, nil
}

func valueBitmap(txn badgerd.Tx, name protoreflect.FullName, fieldPath string, fd protoreflect.FieldDescriptor, v protoreflect.Value) (bitmap.Bitmap, []byte, error) {
	encoded, err := encodeValue(fd, v)
	if err != nil {
		return nil, nil, err
	}
	prefix := valuePrefix(name, fieldPath, encoded)
	bm, err := readValueBitmap(txn, prefix)
	if err != nil {
		return nil, nil, err
	}
	return bm, encoded, nil
}

func encodeValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) ([]byte, error) {
	if !v.IsValid() {
		return nil, nil
	}
	var b []byte
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if v.Bool() {
			b = []byte{1}
		} else {
			b = []byte{0}
		}
	case protoreflect.StringKind:
		b = []byte(v.String())
	case protoreflect.BytesKind:
		b = v.Bytes()
	case protoreflect.EnumKind:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.Enum()))
		b = buf
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v.Int()))
		b = buf
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.Int()))
		b = buf
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v.Uint()))
		b = buf
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, v.Uint())
		b = buf
	case protoreflect.FloatKind:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, math.Float32bits(float32(v.Float())))
		b = buf
	case protoreflect.DoubleKind:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v.Float()))
		b = buf
	case protoreflect.MessageKind:
		if !pfreflect.IsWKType(fd.Message().FullName()) {
			return nil, fmt.Errorf("unsupported index type: %s", fd.Message().FullName())
		}
		switch pfreflect.WKType(fd.Message().FullName()) {
		case pfreflect.Timestamp:
			if !v.Message().IsValid() {
				return nil, nil
			}
			buf := make([]byte, 12)
			binary.BigEndian.PutUint64(buf, uint64(v.Message().Get(fd.Message().Fields().Get(0)).Int()))
			binary.BigEndian.PutUint32(buf[8:], uint32(v.Message().Get(fd.Message().Fields().Get(1)).Int()))
			b = buf
		case pfreflect.Duration:
			if !v.Message().IsValid() {
				return nil, nil
			}
			buf := make([]byte, 12)
			binary.BigEndian.PutUint64(buf, uint64(v.Message().Get(fd.Message().Fields().Get(0)).Int()))
			binary.BigEndian.PutUint32(buf[8:], uint32(v.Message().Get(fd.Message().Fields().Get(1)).Int()))
			b = buf
		default:
			return nil, fmt.Errorf("unsupported index type: %s", fd.Message().FullName())
		}
	default:
		return nil, fmt.Errorf("unsupported index type: %s", fd.Kind())
	}
	return b, nil
}

func decodeValue(fd protoreflect.FieldDescriptor, encoded []byte) (protoreflect.Value, error) {
	if len(encoded) == 0 {
		return protoreflect.Value{}, nil
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(len(encoded) > 0 && encoded[0] == 1), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(string(encoded)), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(encoded), nil
	case protoreflect.EnumKind:
		if len(encoded) != 8 {
			return protoreflect.Value{}, fmt.Errorf("invalid enum value")
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(binary.BigEndian.Uint64(encoded))), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if len(encoded) != 4 {
			return protoreflect.Value{}, fmt.Errorf("invalid int32 value")
		}
		return protoreflect.ValueOfInt32(int32(binary.BigEndian.Uint32(encoded))), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if len(encoded) != 8 {
			return protoreflect.Value{}, fmt.Errorf("invalid int64 value")
		}
		return protoreflect.ValueOfInt64(int64(binary.BigEndian.Uint64(encoded))), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if len(encoded) != 4 {
			return protoreflect.Value{}, fmt.Errorf("invalid uint32 value")
		}
		return protoreflect.ValueOfUint32(binary.BigEndian.Uint32(encoded)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if len(encoded) != 8 {
			return protoreflect.Value{}, fmt.Errorf("invalid uint64 value")
		}
		return protoreflect.ValueOfUint64(binary.BigEndian.Uint64(encoded)), nil
	case protoreflect.FloatKind:
		if len(encoded) != 4 {
			return protoreflect.Value{}, fmt.Errorf("invalid float value")
		}
		return protoreflect.ValueOfFloat32(math.Float32frombits(binary.BigEndian.Uint32(encoded))), nil
	case protoreflect.DoubleKind:
		if len(encoded) != 8 {
			return protoreflect.Value{}, fmt.Errorf("invalid double value")
		}
		return protoreflect.ValueOfFloat64(math.Float64frombits(binary.BigEndian.Uint64(encoded))), nil
	case protoreflect.MessageKind:
		if !pfreflect.IsWKType(fd.Message().FullName()) {
			return protoreflect.Value{}, fmt.Errorf("unsupported index type: %s", fd.Message().FullName())
		}
		switch pfreflect.WKType(fd.Message().FullName()) {
		case pfreflect.Timestamp:
			if len(encoded) != 12 {
				return protoreflect.Value{}, fmt.Errorf("invalid timestamp value")
			}
			secs := int64(binary.BigEndian.Uint64(encoded))
			nanos := int32(binary.BigEndian.Uint32(encoded[8:]))
			return protoreflect.ValueOfMessage((&timestamppb.Timestamp{Seconds: secs, Nanos: nanos}).ProtoReflect()), nil
		case pfreflect.Duration:
			if len(encoded) != 12 {
				return protoreflect.Value{}, fmt.Errorf("invalid duration value")
			}
			secs := int64(binary.BigEndian.Uint64(encoded))
			nanos := int32(binary.BigEndian.Uint32(encoded[8:]))
			return protoreflect.ValueOfMessage((&durationpb.Duration{Seconds: secs, Nanos: nanos}).ProtoReflect()), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("unsupported index type: %s", fd.Message().FullName())
		}
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported index type: %s", fd.Kind())
	}
}

var _ pfindex.UIDStore = (*tx)(nil)
var _ pfindex.UIDTx = (*tx)(nil)
var _ pfindex.UIDStore = (*Store)(nil)
var _ pfindex.UIDTxer = (*Store)(nil)
