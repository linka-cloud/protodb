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

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/internal/protodb"
	protopts "go.linka.cloud/protodb/protodb"
)

func TestRebuildIndexes(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	m1 := dynamicpb.NewMessage(md)
	m1.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("k1"))
	m1.Set(md.Fields().ByName("status"), protoreflect.ValueOfString("up"))
	_, err = db.Set(ctx, m1)
	require.NoError(t, err)

	m2 := dynamicpb.NewMessage(md)
	m2.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("k2"))
	m2.Set(md.Fields().ByName("status"), protoreflect.ValueOfString("down"))
	_, err = db.Set(ctx, m2)
	require.NoError(t, err)

	filter := filters.Where("status").StringEquals("up")
	indexable, err := db.idx.IndexableFilter(dynamicpb.NewMessage(md), filter)
	require.NoError(t, err)
	require.True(t, indexable)
	res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)

	require.NoError(t, deleteIndexPrefix(ctx, db, md))
	count, err := countIndexKeys(db, md)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 0)

	require.NoError(t, db.rebuildIndexes(ctx))

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestIndexUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	m1 := newIndexedMessage(md, "k1", "up", []string{"edge"}, "admin", "alpha")
	_, err = db.Set(ctx, m1)
	require.NoError(t, err)

	m2 := newIndexedMessage(md, "k2", "down", []string{"core"}, "user", "beta")
	_, err = db.Set(ctx, m2)
	require.NoError(t, err)

	filter := filters.Where("status").StringEquals("up")
	res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)

	m2.Set(md.Fields().ByName("status"), protoreflect.ValueOfString("up"))
	_, err = db.Set(ctx, m2)
	require.NoError(t, err)

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 2)

	require.NoError(t, db.Delete(ctx, m1))
	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestIndexableFilterWithInferredIDKey(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIDOnlyFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.idonly.Doc")
	require.NoError(t, err)

	filter := filters.Where("id").StringEquals("d1")
	indexable, err := db.idx.IndexableFilter(dynamicpb.NewMessage(md), filter)
	require.NoError(t, err)
	require.True(t, indexable)
}

func TestIndexRepeatedAndNested(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	m1 := newIndexedMessage(md, "k1", "up", []string{"edge", "core"}, "admin", "alpha")
	_, err = db.Set(ctx, m1)
	require.NoError(t, err)

	m2 := newIndexedMessage(md, "k2", "down", []string{"core"}, "user", "beta")
	_, err = db.Set(ctx, m2)
	require.NoError(t, err)

	res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filters.Where("tags").StringEquals("edge")))
	require.NoError(t, err)
	require.Len(t, res, 1)

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filters.Where("meta.role").StringEquals("admin")))
	require.NoError(t, err)
	require.Len(t, res, 1)

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filters.Where("status").StringEquals("up").OrWhere("tags").StringEquals("edge")))
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestIndexNestedMessageAndRepeatedFields(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildNestedIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.nested.Doc")
	require.NoError(t, err)

	_, err = db.Set(ctx, newNestedDoc(md, "k1", "active", []string{"red", "blue"}, "admin"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newNestedDoc(md, "k2", "inactive", []string{"green"}, "user"))
	require.NoError(t, err)

	res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filters.Where("meta.role").StringEquals("admin")))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "k1", keyFromMessage(t, res[0]))

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filters.Where("tags").StringEquals("red")))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "k1", keyFromMessage(t, res[0]))

	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filters.Where("tags").StringEquals("green")))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "k2", keyFromMessage(t, res[0]))
}

func TestUniqueIndexViolation(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildUniqueIndexedDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.unique.User")
	require.NoError(t, err)

	_, err = db.Set(ctx, newUniqueUser(md, "u1", "a@example.com"))
	require.NoError(t, err)

	_, err = db.Set(ctx, newUniqueUser(md, "u2", "a@example.com"))
	require.Error(t, err)
}

func TestIndexFallbackForNonIndexed(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	m1 := newIndexedMessage(md, "k1", "up", []string{"edge"}, "admin", "alpha")
	_, err = db.Set(ctx, m1)
	require.NoError(t, err)

	filter := filters.Where("description").StringEquals("alpha")
	indexable, err := db.idx.IndexableFilter(dynamicpb.NewMessage(md), filter)
	require.NoError(t, err)
	require.False(t, indexable)

	require.NoError(t, deleteIndexPrefix(ctx, db, md))
	res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter))
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestIndexPaging(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	_, err = db.Set(ctx, newIndexedMessage(md, "k1", "up", nil, "admin", "a"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(md, "k2", "up", nil, "admin", "b"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(md, "k3", "up", nil, "admin", "c"))
	require.NoError(t, err)

	filter := filters.Where("status").StringEquals("up")
	paging := &protodb.Paging{Limit: 1}
	res, pi, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter), protodb.WithPaging(paging))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.True(t, pi.GetHasNext())
	require.Equal(t, "k1", keyFromMessage(t, res[0]))

	paging = &protodb.Paging{Limit: 1, Token: pi.GetToken()}
	res, pi, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter), protodb.WithPaging(paging))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.True(t, pi.GetHasNext())
	require.Equal(t, "k2", keyFromMessage(t, res[0]))

	paging = &protodb.Paging{Limit: 1, Token: pi.GetToken()}
	res, pi, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter), protodb.WithPaging(paging))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.False(t, pi.GetHasNext())
	require.Equal(t, "k3", keyFromMessage(t, res[0]))
}

func TestIndexReversePaging(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	_, err = db.Set(ctx, newIndexedMessage(md, "k1", "up", nil, "admin", "a"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(md, "k2", "up", nil, "admin", "b"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(md, "k3", "up", nil, "admin", "c"))
	require.NoError(t, err)

	filter := filters.Where("status").StringEquals("up")
	paging := &protodb.Paging{Limit: 1}
	res, pi, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter), protodb.WithPaging(paging), protodb.WithReverse())
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.True(t, pi.GetHasNext())
	require.Equal(t, "k3", keyFromMessage(t, res[0]))

	paging = &protodb.Paging{Limit: 1, Token: pi.GetToken()}
	res, pi, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(filter), protodb.WithPaging(paging), protodb.WithReverse())
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.True(t, pi.GetHasNext())
	require.Equal(t, "k2", keyFromMessage(t, res[0]))
}

func TestIndexAndOr(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fd := buildIndexedFileDescriptor(t)
	require.NoError(t, db.RegisterProto(ctx, fd))

	md, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	_, err = db.Set(ctx, newIndexedMessage(md, "k1", "up", []string{"edge"}, "admin", "alpha"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(md, "k2", "down", []string{"core"}, "user", "beta"))
	require.NoError(t, err)

	andFilter := filters.Where("status").StringEquals("up").AndWhere("meta.role").StringEquals("admin")
	res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(andFilter))
	require.NoError(t, err)
	require.Len(t, res, 1)

	orFilter := filters.Where("status").StringEquals("down").OrWhere("tags").StringEquals("edge")
	res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(orFilter))
	require.NoError(t, err)
	require.Len(t, res, 2)
}

func TestIndexSchemaChange(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fdV1 := buildIndexedFileDescriptorV1(t)
	require.NoError(t, db.RegisterProto(ctx, fdV1))

	mdV1, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)

	_, err = db.Set(ctx, newIndexedMessage(mdV1, "k1", "up", nil, "admin", "alpha"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(mdV1, "k2", "down", nil, "admin", "beta"))
	require.NoError(t, err)

	statusCount, err := countIndexFieldKeys(db, mdV1, "status")
	require.NoError(t, err)
	require.Greater(t, statusCount, 0)
	entries, err := schemaEntries(ctx, db, mdV1)
	require.NoError(t, err)
	require.Contains(t, entries, "2|string|1")

	fdV2 := buildIndexedFileDescriptorV2(t)
	require.NoError(t, db.RegisterProto(ctx, fdV2))
	fdV2Desc, err := protodesc.NewFile(fdV2, db.reg)
	require.NoError(t, err)
	require.NoError(t, db.rebuildIndexes(ctx, fdV2Desc))

	mdV2 := fdV2Desc.Messages().ByName("Indexed")
	require.NotNil(t, mdV2)
	require.NotNil(t, mdV2.Fields().ByName("state"))
	require.Nil(t, mdV2.Fields().ByName("status"))
	newEntries := db.idx.CollectEntries(mdV2)
	removed, added := db.idx.DiffEntries(entries, newEntries)
	require.NotContains(t, removed, "2|string|1")
	require.Contains(t, removed, "3|string|3")
	require.Contains(t, removed, "4.1|string|1")
	require.Empty(t, added)

	statusCount, err = countIndexFieldKeysNumber(db, mdV1.FullName(), "2")
	require.NoError(t, err)
	require.Equal(t, 2, statusCount)

	stateCount, err := countIndexFieldKeys(db, mdV2, "state")
	require.NoError(t, err)
	require.Equal(t, 2, stateCount)
	entries, err = schemaEntries(ctx, db, mdV2)
	require.NoError(t, err)
	require.Contains(t, entries, "2|string|1")

}

func TestIndexRenamedField(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	dbif, err := Open(ctx, WithPath(path), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	db := dbif.(*db)
	fdV1 := buildIndexedFileDescriptorV1(t)
	require.NoError(t, db.RegisterProto(ctx, fdV1))

	mdV1, err := lookupMessage(db, "tests.index.Indexed")
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(mdV1, "k1", "up", nil, "admin", "alpha"))
	require.NoError(t, err)
	_, err = db.Set(ctx, newIndexedMessage(mdV1, "k2", "down", nil, "admin", "beta"))
	require.NoError(t, err)

	statusCount, err := countIndexFieldKeys(db, mdV1, "status")
	require.NoError(t, err)
	require.Greater(t, statusCount, 0)

	fdV2 := buildIndexedFileDescriptorV2(t)
	require.NoError(t, db.RegisterProto(ctx, fdV2))
	fdV2Desc, err := protodesc.NewFile(fdV2, db.reg)
	require.NoError(t, err)
	require.NoError(t, db.rebuildIndexes(ctx, fdV2Desc))

	mdV2 := fdV2Desc.Messages().ByName("Indexed")
	require.NotNil(t, mdV2)
	require.NotNil(t, mdV2.Fields().ByName("state"))
	require.Nil(t, mdV2.Fields().ByName("status"))

	statusCount, err = countIndexFieldKeysNumber(db, mdV1.FullName(), "2")
	require.NoError(t, err)
	require.Equal(t, 2, statusCount)

	stateCount, err := countIndexFieldKeys(db, mdV2, "state")
	require.NoError(t, err)
	require.Equal(t, 2, stateCount)

}

func buildIndexedFileDescriptor(t *testing.T) *descriptorpb.FileDescriptorProto {
	return buildIndexedFileDescriptorV1(t)
}

func buildIDOnlyFileDescriptor(t *testing.T) *descriptorpb.FileDescriptorProto {
	idField := protobuilder.NewField("id", protobuilder.FieldTypeString()).SetNumber(1)
	msg := protobuilder.NewMessage("Doc").AddField(idField)
	file := protobuilder.NewFile("tests/id_only.proto").
		SetPackageName(protoreflect.FullName("tests.idonly")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg)
	fd, err := file.Build()
	require.NoError(t, err)
	return protodesc.ToFileDescriptorProto(fd)
}

func buildIndexedFileDescriptorV1(t *testing.T) *descriptorpb.FileDescriptorProto {
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	indexOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(indexOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(false)})

	keyField := protobuilder.NewField("key", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(keyOpts)
	statusField := protobuilder.NewField("status", protobuilder.FieldTypeString()).
		SetNumber(2).
		SetOptions(indexOpts)
	tagsField := protobuilder.NewField("tags", protobuilder.FieldTypeString()).
		SetNumber(3).
		SetOptions(indexOpts).
		SetRepeated()
	metaRole := protobuilder.NewField("role", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(indexOpts)
	metaMsg := protobuilder.NewMessage("Meta").AddField(metaRole)
	metaField := protobuilder.NewField("meta", protobuilder.FieldTypeMessage(metaMsg)).
		SetNumber(4)
	descField := protobuilder.NewField("description", protobuilder.FieldTypeString()).
		SetNumber(5)
	msg := protobuilder.NewMessage("Indexed").
		AddField(keyField).
		AddField(statusField).
		AddField(tagsField).
		AddField(metaField).
		AddField(descField).
		AddNestedMessage(metaMsg)
	file := protobuilder.NewFile("tests/index.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	return protodesc.ToFileDescriptorProto(fd)
}

func buildIndexedFileDescriptorV2(t *testing.T) *descriptorpb.FileDescriptorProto {
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	indexOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(indexOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(false)})

	keyField := protobuilder.NewField("key", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(keyOpts)
	stateField := protobuilder.NewField("state", protobuilder.FieldTypeString()).
		SetNumber(2).
		SetOptions(indexOpts)
	descField := protobuilder.NewField("description", protobuilder.FieldTypeString()).
		SetNumber(5)
	msg := protobuilder.NewMessage("Indexed").
		AddField(keyField).
		AddField(stateField).
		AddField(descField).
		AddReservedRange(3, 5)
	file := protobuilder.NewFile("tests/index.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	return protodesc.ToFileDescriptorProto(fd)
}

func buildNestedIndexedFileDescriptor(t *testing.T) *descriptorpb.FileDescriptorProto {
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	indexOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(indexOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(false)})

	metaRole := protobuilder.NewField("role", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(indexOpts)
	metaMsg := protobuilder.NewMessage("Meta").AddField(metaRole)

	keyField := protobuilder.NewField("key", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(keyOpts)
	statusField := protobuilder.NewField("status", protobuilder.FieldTypeString()).
		SetNumber(2).
		SetOptions(indexOpts)
	tagsField := protobuilder.NewField("tags", protobuilder.FieldTypeString()).
		SetNumber(3).
		SetOptions(indexOpts).
		SetRepeated()
	metaField := protobuilder.NewField("meta", protobuilder.FieldTypeMessage(metaMsg)).
		SetNumber(4)

	msg := protobuilder.NewMessage("Doc").
		AddField(keyField).
		AddField(statusField).
		AddField(tagsField).
		AddField(metaField).
		AddNestedMessage(metaMsg)

	file := protobuilder.NewFile("tests/nested.proto").
		SetPackageName(protoreflect.FullName("tests.nested")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	return protodesc.ToFileDescriptorProto(fd)
}

func buildUniqueIndexedDescriptor(t *testing.T) *descriptorpb.FileDescriptorProto {
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	uniqueOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(uniqueOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(true)})

	keyField := protobuilder.NewField("key", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(keyOpts)
	emailField := protobuilder.NewField("email", protobuilder.FieldTypeString()).
		SetNumber(2).
		SetOptions(uniqueOpts)

	msg := protobuilder.NewMessage("User").
		AddField(keyField).
		AddField(emailField)

	file := protobuilder.NewFile("tests/unique.proto").
		SetPackageName(protoreflect.FullName("tests.unique")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	return protodesc.ToFileDescriptorProto(fd)
}

func lookupMessage(db *db, name string) (protoreflect.MessageDescriptor, error) {
	d, err := db.Resolver().FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, err
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor %s is not a message", name)
	}
	return md, nil
}

func newIndexedMessage(md protoreflect.MessageDescriptor, key, status string, tags []string, role, description string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if status != "" {
		m.Set(md.Fields().ByName("status"), protoreflect.ValueOfString(status))
	}
	if len(tags) > 0 {
		list := m.Mutable(md.Fields().ByName("tags")).List()
		for _, tag := range tags {
			list.Append(protoreflect.ValueOfString(tag))
		}
	}
	if role != "" {
		metaField := md.Fields().ByName("meta")
		meta := dynamicpb.NewMessage(metaField.Message())
		meta.Set(metaField.Message().Fields().ByName("role"), protoreflect.ValueOfString(role))
		m.Set(metaField, protoreflect.ValueOfMessage(meta))
	}
	if description != "" {
		m.Set(md.Fields().ByName("description"), protoreflect.ValueOfString(description))
	}
	return m
}

func newIndexedMessageV2(md protoreflect.MessageDescriptor, key, state, description string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if state != "" {
		m.Set(md.Fields().ByName("state"), protoreflect.ValueOfString(state))
	}
	if description != "" {
		m.Set(md.Fields().ByName("description"), protoreflect.ValueOfString(description))
	}
	return m
}

func newNestedDoc(md protoreflect.MessageDescriptor, key, status string, tags []string, role string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if status != "" {
		m.Set(md.Fields().ByName("status"), protoreflect.ValueOfString(status))
	}
	if len(tags) > 0 {
		list := m.Mutable(md.Fields().ByName("tags")).List()
		for _, tag := range tags {
			list.Append(protoreflect.ValueOfString(tag))
		}
	}
	if role != "" {
		metaField := md.Fields().ByName("meta")
		meta := dynamicpb.NewMessage(metaField.Message())
		meta.Set(metaField.Message().Fields().ByName("role"), protoreflect.ValueOfString(role))
		m.Set(metaField, protoreflect.ValueOfMessage(meta))
	}
	return m
}

func newUniqueUser(md protoreflect.MessageDescriptor, key, email string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if email != "" {
		m.Set(md.Fields().ByName("email"), protoreflect.ValueOfString(email))
	}
	return m
}

func keyFromMessage(t *testing.T, msg proto.Message) string {
	pm := msg.ProtoReflect()
	fd := pm.Descriptor().Fields().ByName("key")
	require.NotNil(t, fd)
	return pm.Get(fd).String()
}

func countIndexFieldKeys(db *db, md protoreflect.MessageDescriptor, fieldPath string) (int, error) {
	count := 0
	path, err := fieldNumberPath(md, fieldPath)
	if err != nil {
		return 0, err
	}
	for _, base := range []string{protodb.Index, protodb.IndexDelta} {
		prefix := indexFieldPrefixWithBase(md.FullName(), path, base)
		if err := db.bdb.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func countIndexFieldKeysNumber(db *db, name protoreflect.FullName, numberPath string) (int, error) {
	count := 0
	for _, base := range []string{protodb.Index, protodb.IndexDelta} {
		prefix := indexFieldPrefixWithBase(name, numberPath, base)
		if err := db.bdb.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func fieldNumberPath(md protoreflect.MessageDescriptor, namePath string) (string, error) {
	if namePath == "" {
		return "", fmt.Errorf("empty field path")
	}
	parts := strings.Split(namePath, ".")
	cur := md
	var nums []string
	for _, part := range parts {
		fd := cur.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			return "", fmt.Errorf("%s does not contain '%s'", cur.FullName(), namePath)
		}
		nums = append(nums, fmt.Sprintf("%d", fd.Number()))
		if fd.Kind() == protoreflect.MessageKind {
			cur = fd.Message()
		} else if part != parts[len(parts)-1] {
			return "", fmt.Errorf("%s does not contain '%s'", md.FullName(), namePath)
		}
	}
	return strings.Join(nums, "."), nil
}

func schemaEntries(ctx context.Context, db *db, md protoreflect.MessageDescriptor) ([]string, error) {
	tx, err := db.bdb.NewTransaction(ctx, false)
	if err != nil {
		return nil, err
	}
	defer tx.Close(ctx)
	return db.idx.SchemaEntries(ctx, tx, md)
}

func indexFieldPrefixWithBase(name protoreflect.FullName, fieldPath string, base string) []byte {
	basePrefix := []byte(fmt.Sprintf("%s/%s/", base, name))
	fieldBytes := []byte(fieldPath)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(fieldBytes)))
	prefix := make([]byte, 0, len(basePrefix)+4+len(fieldBytes))
	prefix = append(prefix, basePrefix...)
	prefix = append(prefix, buf...)
	prefix = append(prefix, fieldBytes...)
	return prefix
}

func deleteIndexPrefix(ctx context.Context, db *db, md protoreflect.MessageDescriptor) error {
	var keys [][]byte
	for _, base := range []string{protodb.Index, protodb.IndexDelta} {
		prefix := []byte(fmt.Sprintf("%s/%s/", base, md.FullName()))
		if err := db.bdb.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if len(keys) == 0 {
		return nil
	}
	tx, err := db.bdb.NewTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Close(ctx)
	for _, key := range keys {
		if err := tx.Delete(ctx, key); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func countIndexKeys(db *db, md protoreflect.MessageDescriptor) (int, error) {
	count := 0
	for _, base := range []string{protodb.Index, protodb.IndexDelta} {
		prefix := []byte(fmt.Sprintf("%s/%s/", base, md.FullName()))
		if err := db.bdb.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return count, nil
}
