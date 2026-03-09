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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
	"go.linka.cloud/protofilters/filters"
	pfindex "go.linka.cloud/protofilters/index"
	pfreflect "go.linka.cloud/protofilters/reflect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
	protopts "go.linka.cloud/protodb/protodb"
)

var idxTracer = otel.Tracer("protodb.indexer")

type FileRegistry interface {
	RangeFiles(func(protoreflect.FileDescriptor) bool)
}

type Tx interface {
	Txn() badgerd.Tx
	UID(ctx context.Context, key []byte, inc bool) (uint64, bool, error)
}

type Indexer struct {
	reg       protodesc.Resolver
	freg      FileRegistry
	unmarshal func([]byte, proto.Message) error
	mu        sync.RWMutex
	entries   map[protoreflect.FullName]entryCache
}

type FindOpts struct {
	Offset  uint64
	Limit   uint64
	Reverse bool
}

type entryCache struct {
	sig     string
	entries []string
}

func NewIndexer(reg protodesc.Resolver, freg FileRegistry, unmarshal func([]byte, proto.Message) error) *Indexer {
	return &Indexer{reg: reg, freg: freg, unmarshal: unmarshal}
}

func (idx *Indexer) RebuildIfNeeded(ctx context.Context, tx Tx, files ...protoreflect.FileDescriptor) error {
	ctx, span := idxTracer.Start(ctx, "Indexer.RebuildIfNeeded")
	defer span.End()
	return idx.Rebuild(ctx, tx, files...)
}

func (idx *Indexer) Rebuild(ctx context.Context, tx Tx, files ...protoreflect.FileDescriptor) error {
	ctx, span := idxTracer.Start(ctx, "Indexer.Rebuild")
	defer span.End()
	msgs := collectMessagesFromFiles(files)
	if len(files) == 0 {
		msgs = collectMessageDescriptors(idx.freg)
	}
	span.SetAttributes(
		attribute.Int("index.messages", len(msgs)),
		attribute.Int("index.files", len(files)),
	)
	if len(msgs) == 0 {
		return nil
	}
	for _, md := range msgs {
		newEnts := idx.CollectEntries(md)
		if len(newEnts) == 0 {
			continue
		}
		oldEnts, err := indexSchemaEntries(ctx, tx.Txn(), md)
		if err != nil {
			return err
		}
		idxExists, err := indexHasEntries(ctx, tx, md)
		if err != nil {
			return err
		}
		if len(oldEnts) == 0 {
			if err := idx.ensureIndexSchema(ctx, tx, md, newEnts, idxExists); err != nil {
				return err
			}
			continue
		}
		removed, added := diffEntries(oldEnts, newEnts)
		if !idxExists {
			if err := idx.ensureIndexSchema(ctx, tx, md, newEnts, false); err != nil {
				return err
			}
			continue
		}
		if err := idx.updateIndexEntries(ctx, tx, md, newEnts, removed, added); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Indexer) SchemaEntries(ctx context.Context, tx badgerd.Tx, md protoreflect.MessageDescriptor) ([]string, error) {
	return indexSchemaEntries(ctx, tx, md)
}

func (idx *Indexer) CollectEntries(md protoreflect.MessageDescriptor) []string {
	sig := entrySig(md)
	idx.mu.RLock()
	if ent, ok := idx.entries[md.FullName()]; ok && ent.sig == sig {
		entries := append([]string(nil), ent.entries...)
		idx.mu.RUnlock()
		return entries
	}
	idx.mu.RUnlock()
	entries := collectIndexEntries(md)
	idx.mu.Lock()
	if idx.entries == nil {
		idx.entries = map[protoreflect.FullName]entryCache{}
	}
	idx.entries[md.FullName()] = entryCache{sig: sig, entries: entries}
	idx.mu.Unlock()
	return append([]string(nil), entries...)
}

func (idx *Indexer) DiffEntries(oldEntries, newEntries []string) (removed, added []string) {
	return diffEntries(oldEntries, newEntries)
}

func (idx *Indexer) Selector(_ context.Context, _ protoreflect.FullName, fds ...protoreflect.FieldDescriptor) (bool, error) {
	if len(fds) == 0 {
		return false, nil
	}
	fd := fds[len(fds)-1]
	if !proto.HasExtension(fd.Options(), protopts.E_Index) {
		return false, nil
	}
	if !isIndexableFieldPathDescriptors(fds) {
		return false, nil
	}
	return true, nil
}

func (idx *Indexer) IndexableFilter(m proto.Message, f protodb.Filter) (bool, error) {
	if f == nil || f.Expr() == nil {
		return false, nil
	}
	return isIndexableExpr(m.ProtoReflect().New(), f.Expr())
}

func (idx *Indexer) EnforceUnique(ctx context.Context, tx Tx, m proto.Message, uid uint64) error {
	ctx, span := idxTracer.Start(ctx, "Indexer.EnforceUnique")
	defer span.End()
	vals, err := collectUniqueValues(m.ProtoReflect())
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int("index.unique_values", len(vals)))
	if len(vals) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(vals))
	for _, uv := range vals {
		if !isIndexableFieldPathDescriptors(uv.fds) {
			continue
		}
		fieldPath := fieldPathFromNumbers(uv.fds)
		errPath := fieldPathFromNames(uv.fds)
		bm, enc, err := valueBitmap(tx.Txn(), m.ProtoReflect().Descriptor().FullName(), fieldPath, uv.fds[len(uv.fds)-1], uv.value)
		if err != nil {
			return err
		}
		key := fieldPath + "\x00" + string(enc)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		card := bm.Cardinality()
		if card == 0 {
			continue
		}
		if card == 1 {
			if bm.Contains(uid) {
				continue
			}
			return fmt.Errorf("unique index violation on %s", errPath)
		}
		return fmt.Errorf("unique index violation on %s", errPath)
	}
	return nil
}

func entrySig(md protoreflect.MessageDescriptor) string {
	var b strings.Builder
	seen := map[protoreflect.FullName]struct{}{}
	var walk func(msg protoreflect.MessageDescriptor)
	walk = func(msg protoreflect.MessageDescriptor) {
		if _, ok := seen[msg.FullName()]; ok {
			return
		}
		seen[msg.FullName()] = struct{}{}
		b.WriteString("msg:")
		b.WriteString(string(msg.FullName()))
		b.WriteByte(';')
		fields := msg.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			b.WriteString("f:")
			b.WriteString(string(fd.Name()))
			b.WriteByte('#')
			b.WriteString(strconv.Itoa(int(fd.Number())))
			b.WriteByte('|')
			b.WriteString(fd.Kind().String())
			b.WriteByte('|')
			b.WriteString(fd.Cardinality().String())
			if fd.IsMap() {
				b.WriteString("|map")
			}
			if fd.IsList() {
				b.WriteString("|list")
			}
			if fd.Kind() == protoreflect.MessageKind {
				b.WriteString("|msg=")
				b.WriteString(string(fd.Message().FullName()))
			}
			if proto.HasExtension(fd.Options(), protopts.E_Index) {
				b.WriteString("|idx")
				idxOpt := proto.GetExtension(fd.Options(), protopts.E_Index).(*protopts.Index)
				if idxOpt.GetUnique() {
					b.WriteString(":u")
				}
			}
			if proto.HasExtension(fd.Options(), protopts.E_Key) {
				b.WriteString("|key")
			}
			b.WriteByte(';')
			if fd.Kind() == protoreflect.MessageKind && !pfreflect.IsWKType(fd.Message().FullName()) {
				walk(fd.Message())
			}
		}
	}
	walk(md)
	return b.String()
}

func (idx *Indexer) FindKeys(ctx context.Context, tx Tx, name protoreflect.FullName, f protodb.Filter) ([]string, []string, error) {
	ctx, span := idxTracer.Start(ctx, "Indexer.FindKeys")
	defer span.End()
	span.SetAttributes(attribute.String("index.type", string(name)))
	uids, err := idx.FindUIDs(ctx, tx, name, f, FindOpts{})
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0)
	for _, uid := range uids {
		item, err := tx.Txn().Get(ctx, protodb.UIDKey(uid))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			return nil, nil, err
		}
		var key string
		if err := item.Value(func(val []byte) error {
			key = string(val)
			return nil
		}); err != nil {
			return nil, nil, err
		}
		if key != "" {
			keys = append(keys, key)
		}
	}
	return keys, nil, nil
}

func (idx *Indexer) FindUIDs(ctx context.Context, tx Tx, name protoreflect.FullName, f protodb.Filter, opts FindOpts) ([]uint64, error) {
	ix := pfindex.NewUID(newUIDTxStore(tx.Txn(), idx.reg), idx.Selector)
	out := make([]uint64, 0)
	for uid, err := range ix.Find(ctx, name, f, pfindex.FindOptions{Offset: opts.Offset, Limit: opts.Limit, Reverse: opts.Reverse}) {
		if err != nil {
			return nil, err
		}
		out = append(out, uid)
	}
	return out, nil
}

func (idx *Indexer) Insert(ctx context.Context, tx Tx, key []byte, m proto.Message) error {
	ctx, span := idxTracer.Start(ctx, "Indexer.Insert")
	defer span.End()
	if m != nil {
		span.SetAttributes(attribute.String("index.type", string(m.ProtoReflect().Descriptor().FullName())))
	}
	uid, ok, err := tx.UID(ctx, key, false)
	if err != nil {
		return err
	}
	if !ok {
		return badger.ErrKeyNotFound
	}
	ix := pfindex.NewUID(newUIDTxStore(tx.Txn(), idx.reg), idx.Selector)
	return ix.Insert(ctx, uid, m)
}

func (idx *Indexer) Update(ctx context.Context, tx Tx, key []byte, oldMsg, newMsg proto.Message) error {
	ctx, span := idxTracer.Start(ctx, "Indexer.Update")
	defer span.End()
	if newMsg != nil {
		span.SetAttributes(attribute.String("index.type", string(newMsg.ProtoReflect().Descriptor().FullName())))
	}
	span.SetAttributes(
		attribute.Bool("index.has_old", oldMsg != nil),
		attribute.Bool("index.has_new", newMsg != nil),
	)
	uid, ok, err := tx.UID(ctx, key, false)
	if err != nil {
		return err
	}
	if !ok {
		return badger.ErrKeyNotFound
	}
	ix := pfindex.NewUID(newUIDTxStore(tx.Txn(), idx.reg), idx.Selector)
	if oldMsg != nil {
		txv, err := newUIDTxStore(tx.Txn(), idx.reg).Tx(ctx)
		if err != nil {
			return err
		}
		if err := idx.removeValues(ctx, txv, uid, oldMsg.ProtoReflect()); err != nil {
			return err
		}
	}
	if newMsg == nil {
		return nil
	}
	return ix.Insert(ctx, uid, newMsg)
}

func (idx *Indexer) removeValues(ctx context.Context, tx pfindex.UIDTx, uid uint64, msg protoreflect.Message, fds ...protoreflect.FieldDescriptor) error {
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		path := append(fds, fd)
		rval := msg.Get(fd)
		if fd.IsMap() {
			continue
		}
		if fd.IsList() {
			if fd.Kind() == protoreflect.MessageKind {
				list := rval.List()
				for j := 0; j < list.Len(); j++ {
					child := list.Get(j).Message()
					if err := idx.removeValues(ctx, tx, uid, child, path...); err != nil {
						return err
					}
				}
				continue
			}
			ok, err := idx.Selector(ctx, msg.Descriptor().FullName(), path...)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			list := rval.List()
			for j := 0; j < list.Len(); j++ {
				if err := tx.RemoveUID(ctx, uid, fd, list.Get(j)); err != nil {
					return err
				}
			}
			continue
		}
		if fd.Kind() == protoreflect.MessageKind && !pfreflect.IsWKType(fd.Message().FullName()) {
			if !rval.Message().IsValid() {
				continue
			}
			if err := idx.removeValues(ctx, tx, uid, rval.Message(), path...); err != nil {
				return err
			}
			continue
		}
		if fd.HasOptionalKeyword() && !msg.Has(fd) {
			continue
		}
		ok, err := idx.Selector(ctx, msg.Descriptor().FullName(), path...)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := tx.RemoveUID(ctx, uid, fd, rval); err != nil {
			return err
		}
	}
	return nil
}

func indexHasEntries(ctx context.Context, tx Tx, md protoreflect.MessageDescriptor) (bool, error) {
	for _, pfx := range []string{protodb.Index, protodb.IndexDelta} {
		ok, err := indexHasPrefix(ctx, tx.Txn(), md, pfx)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func indexHasPrefix(ctx context.Context, tx badgerd.Tx, md protoreflect.MessageDescriptor, prefix string) (bool, error) {
	pfx := fmt.Appendf(nil, "%s/%s/", prefix, md.FullName())
	it := tx.Iterator(badger.IteratorOptions{Prefix: pfx, PrefetchValues: false})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func indexSchemaKey(name protoreflect.FullName) []byte {
	return fmt.Appendf(nil, "%s/index/%s", protodb.Internal, name)
}

func indexSchemaEntries(ctx context.Context, tx badgerd.Tx, md protoreflect.MessageDescriptor) ([]string, error) {
	item, err := tx.Get(ctx, indexSchemaKey(md.FullName()))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var out []byte
	if err := item.Value(func(val []byte) error {
		out = append(out[:0], val...)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return strings.Split(string(out), "\n"), nil
}

func setIndexSchemaEntries(ctx context.Context, tx badgerd.Tx, md protoreflect.MessageDescriptor, entries []string) error {
	if len(entries) == 0 {
		return nil
	}
	return tx.Set(ctx, indexSchemaKey(md.FullName()), []byte(strings.Join(entries, "\n")), 0)
}

func (idx *Indexer) ensureIndexSchema(ctx context.Context, tx Tx, md protoreflect.MessageDescriptor, entries []string, exists bool) error {
	if exists {
		return setIndexSchemaEntries(ctx, tx.Txn(), md, entries)
	}
	if err := idx.addIndexEntries(ctx, tx, md, entries); err != nil {
		return err
	}
	return setIndexSchemaEntries(ctx, tx.Txn(), md, entries)
}

func (idx *Indexer) updateIndexEntries(ctx context.Context, tx Tx, md protoreflect.MessageDescriptor, entries, removed, added []string) error {
	if len(removed) > 0 {
		if err := idx.dropIndexEntries(ctx, tx, md, removed); err != nil {
			return err
		}
	}
	if len(added) > 0 {
		if err := idx.addIndexEntries(ctx, tx, md, added); err != nil {
			return err
		}
	}
	if len(removed) == 0 && len(added) == 0 {
		return nil
	}
	return setIndexSchemaEntries(ctx, tx.Txn(), md, entries)
}

func diffEntries(oldEntries, newEntries []string) (removed, added []string) {
	oldSet := make(map[string]struct{}, len(oldEntries))
	newSet := make(map[string]struct{}, len(newEntries))
	for _, v := range oldEntries {
		oldSet[v] = struct{}{}
	}
	for _, v := range newEntries {
		newSet[v] = struct{}{}
	}
	for _, v := range oldEntries {
		if _, ok := newSet[v]; !ok {
			removed = append(removed, v)
		}
	}
	for _, v := range newEntries {
		if _, ok := oldSet[v]; !ok {
			added = append(added, v)
		}
	}
	return removed, added
}

func (idx *Indexer) dropIndexEntries(ctx context.Context, tx Tx, md protoreflect.MessageDescriptor, entries []string) error {
	removeSet := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		path := entryPath(entry)
		removeSet[path] = struct{}{}
	}
	if len(removeSet) == 0 {
		return nil
	}
	for _, pfx := range []string{protodb.Index, protodb.IndexDelta} {
		keys, err := scanIndexKeys(ctx, tx.Txn(), md, removeSet, pfx)
		if err != nil {
			return err
		}
		if err := deleteKeys(ctx, tx.Txn(), keys); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Indexer) addIndexEntries(ctx context.Context, tx Tx, md protoreflect.MessageDescriptor, entries []string) error {
	if idx.unmarshal == nil {
		return errors.New("indexer unmarshal missing")
	}
	allowed := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		allowed[entryPath(entry)] = struct{}{}
	}
	selector := func(_ context.Context, _ protoreflect.FullName, fds ...protoreflect.FieldDescriptor) (bool, error) {
		if len(fds) == 0 {
			return false, nil
		}
		if fds[len(fds)-1].IsMap() {
			return false, nil
		}
		path := fieldPathFromNumbers(fds)
		_, ok := allowed[path]
		return ok, nil
	}
	partial := pfindex.NewUID(newUIDTxStore(tx.Txn(), idx.reg), selector)
	dataPrefix := fmt.Appendf(nil, "%s/%s/", protodb.Data, md.FullName())
	dataIt := tx.Txn().Iterator(badger.IteratorOptions{Prefix: dataPrefix, PrefetchValues: false})
	defer dataIt.Close()
	for dataIt.Rewind(); dataIt.Valid(); dataIt.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := dataIt.Item()
		key := item.KeyCopy(nil)
		msg := dynamicpb.NewMessage(md)
		if err := item.Value(func(val []byte) error {
			return idx.unmarshal(val, msg)
		}); err != nil {
			return err
		}
		uid, ok, err := tx.UID(ctx, key, true)
		if err != nil {
			return err
		}
		if !ok {
			uk := protodb.UIDKey(uid)
			if err := tx.Txn().Set(ctx, uk, key, 0); err != nil {
				return err
			}
			urk := protodb.UIDRevKey(key)
			if err := tx.Txn().Set(ctx, urk, y.U64ToBytes(uid), 0); err != nil {
				return err
			}
		}
		if err := partial.Insert(ctx, uid, msg); err != nil {
			return err
		}
	}
	return nil
}

func indexFieldFromKey(base, key []byte) (string, bool) {
	if !bytes.HasPrefix(key, base) {
		return "", false
	}
	rest := key[len(base):]
	if len(rest) < 8 {
		return "", false
	}
	fieldLen := int(binary.BigEndian.Uint32(rest[:4]))
	if fieldLen <= 0 || 4+fieldLen+4 > len(rest) {
		return "", false
	}
	fieldStart := 4
	fieldStop := fieldStart + fieldLen
	return string(rest[fieldStart:fieldStop]), true
}

func entryPath(entry string) string {
	return strings.Split(entry, "|")[0]
}

func scanIndexKeys(ctx context.Context, tx badgerd.Tx, md protoreflect.MessageDescriptor, removeSet map[string]struct{}, prefix string) ([][]byte, error) {
	base := fmt.Appendf(nil, "%s/%s/", prefix, md.FullName())
	it := tx.Iterator(badger.IteratorOptions{Prefix: base, PrefetchValues: false})
	defer it.Close()
	var keys [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		path, ok := indexFieldFromKey(base, it.Item().Key())
		if !ok {
			continue
		}
		if _, ok := removeSet[path]; !ok {
			continue
		}
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	return keys, nil
}

func deleteKeys(ctx context.Context, tx badgerd.Tx, keys [][]byte) error {
	for _, key := range keys {
		if err := tx.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func collectIndexEntries(md protoreflect.MessageDescriptor) []string {
	entries := make([]string, 0)
	seen := map[protoreflect.FullName]struct{}{}
	var walk func(msg protoreflect.MessageDescriptor, prefix string)
	walk = func(msg protoreflect.MessageDescriptor, prefix string) {
		if _, ok := seen[msg.FullName()]; ok {
			return
		}
		seen[msg.FullName()] = struct{}{}
		fields := msg.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			if fd.IsMap() {
				continue
			}
			if fd.Kind() == protoreflect.MessageKind && !pfreflect.IsWKType(fd.Message().FullName()) {
				walk(fd.Message(), prefix+fmt.Sprintf("%d", fd.Number())+".")
				continue
			}
			if !proto.HasExtension(fd.Options(), protopts.E_Index) || !isIndexableField(fd) {
				continue
			}
			entries = append(entries, fmt.Sprintf("%s%d|%s|%d", prefix, fd.Number(), fd.Kind(), fd.Cardinality()))
		}
	}
	walk(md, "")
	sort.Strings(entries)
	return entries
}

func collectMessageDescriptors(freg FileRegistry) []protoreflect.MessageDescriptor {
	if freg == nil {
		return nil
	}
	var out []protoreflect.MessageDescriptor
	freg.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		out = append(out, collectMessages(fd.Messages())...)
		return true
	})
	return out
}

func collectMessagesFromFiles(files []protoreflect.FileDescriptor) []protoreflect.MessageDescriptor {
	var out []protoreflect.MessageDescriptor
	for _, fd := range files {
		out = append(out, collectMessages(fd.Messages())...)
	}
	return out
}

func collectMessages(mds protoreflect.MessageDescriptors) []protoreflect.MessageDescriptor {
	var out []protoreflect.MessageDescriptor
	for i := 0; i < mds.Len(); i++ {
		md := mds.Get(i)
		out = append(out, md)
		out = append(out, collectMessages(md.Messages())...)
	}
	return out
}

type uniqueValue struct {
	fds   []protoreflect.FieldDescriptor
	value protoreflect.Value
}

func collectUniqueValues(msg protoreflect.Message, fds ...protoreflect.FieldDescriptor) ([]uniqueValue, error) {
	var out []uniqueValue
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		path := append(fds, fd)
		rval := msg.Get(fd)
		if fd.IsMap() {
			continue
		}
		if fd.IsList() {
			vals, err := collectUniqueFromList(fd, rval, path...)
			if err != nil {
				return nil, err
			}
			out = append(out, vals...)
			continue
		}
		if fd.Kind() == protoreflect.MessageKind && !pfreflect.IsWKType(fd.Message().FullName()) {
			if !rval.Message().IsValid() {
				continue
			}
			vals, err := collectUniqueValues(rval.Message(), path...)
			if err != nil {
				return nil, err
			}
			out = append(out, vals...)
			continue
		}
		if fd.HasOptionalKeyword() && !msg.Has(fd) {
			continue
		}
		if idx := proto.GetExtension(fd.Options(), protopts.E_Index); idx != nil {
			if idx.(*protopts.Index).GetUnique() {
				out = append(out, uniqueValue{fds: path, value: rval})
			}
		}
	}
	return out, nil
}

func collectUniqueFromList(fd protoreflect.FieldDescriptor, val protoreflect.Value, fds ...protoreflect.FieldDescriptor) ([]uniqueValue, error) {
	if fd.Kind() == protoreflect.MessageKind {
		return collectUniqueFromMsgList(val, fds...)
	}
	idx := proto.GetExtension(fd.Options(), protopts.E_Index)
	if idx == nil || !idx.(*protopts.Index).GetUnique() {
		return nil, nil
	}
	list := val.List()
	out := make([]uniqueValue, 0, list.Len())
	for j := 0; j < list.Len(); j++ {
		out = append(out, uniqueValue{fds: fds, value: list.Get(j)})
	}
	return out, nil
}

func collectUniqueFromMsgList(val protoreflect.Value, fds ...protoreflect.FieldDescriptor) ([]uniqueValue, error) {
	list := val.List()
	var out []uniqueValue
	for j := 0; j < list.Len(); j++ {
		child := list.Get(j).Message()
		vals, err := collectUniqueValues(child, fds...)
		if err != nil {
			return nil, err
		}
		out = append(out, vals...)
	}
	return out, nil
}

func isIndexableExpr(msg protoreflect.Message, expr *filters.Expression) (bool, error) {
	if expr == nil {
		return true, nil
	}
	if expr.Condition != nil {
		ok, err := isIndexableFieldPath(msg, expr.Condition.GetField())
		if err != nil || !ok {
			return ok, err
		}
	}
	for _, v := range expr.AndExprs {
		ok, err := isIndexableExpr(msg, v)
		if err != nil || !ok {
			return ok, err
		}
	}
	for _, v := range expr.OrExprs {
		ok, err := isIndexableExpr(msg, v)
		if err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func isIndexableFieldPath(msg protoreflect.Message, fieldPath string) (bool, error) {
	if fieldPath == "" {
		return false, nil
	}
	fds, err := pfreflect.Lookup(msg, fieldPath)
	if err != nil {
		return false, err
	}
	fd := fds[len(fds)-1]
	if proto.HasExtension(fd.Options(), protopts.E_Index) {
		return isIndexableFieldPathDescriptors(fds), nil
	}
	if !isKeyField(msg.Descriptor(), fds) {
		return false, nil
	}
	return isIndexableFieldPathDescriptors(fds), nil
}

func isKeyField(md protoreflect.MessageDescriptor, fds []protoreflect.FieldDescriptor) bool {
	if md == nil {
		return false
	}
	if len(fds) != 1 {
		return false
	}
	field, ok := protodb.KeyFieldName(md)
	if !ok {
		return false
	}
	return field == string(fds[0].Name())
}

func isIndexableFieldPathDescriptors(fds []protoreflect.FieldDescriptor) bool {
	if len(fds) == 0 {
		return false
	}
	last := fds[len(fds)-1]
	for i, fd := range fds {
		if fd.IsMap() {
			return false
		}
		if fd.Kind() == protoreflect.MessageKind {
			n := fd.Message().FullName()
			if n != "google.protobuf.Timestamp" && n != "google.protobuf.Duration" {
				if i == len(fds)-1 {
					return false
				}
				continue
			}
		}
	}
	return isIndexableLeaf(last)
}

func isIndexableField(fd protoreflect.FieldDescriptor) bool {
	return isIndexableLeaf(fd)
}

func isIndexableLeaf(fd protoreflect.FieldDescriptor) bool {
	switch fd.Kind() {
	case protoreflect.BoolKind,
		protoreflect.Int32Kind,
		protoreflect.Int64Kind,
		protoreflect.Uint32Kind,
		protoreflect.Uint64Kind,
		protoreflect.Sint32Kind,
		protoreflect.Sint64Kind,
		protoreflect.Fixed32Kind,
		protoreflect.Fixed64Kind,
		protoreflect.Sfixed32Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.FloatKind,
		protoreflect.DoubleKind,
		protoreflect.StringKind,
		protoreflect.BytesKind,
		protoreflect.EnumKind:
		return true
	case protoreflect.MessageKind:
		n := fd.Message().FullName()
		return n == "google.protobuf.Timestamp" || n == "google.protobuf.Duration"
	default:
		return false
	}
}
