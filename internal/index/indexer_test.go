package index

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"go.linka.cloud/protofilters/index/bitmap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/protodb"
	regpkg "go.linka.cloud/protodb/internal/registry"
	protopts "go.linka.cloud/protodb/protodb"
)

func TestCollectEntriesCacheAndRefresh(t *testing.T) {
	idx := NewIndexer(nil, nil)
	md1 := buildIndexerDocDescriptorV1(t)
	md2 := buildIndexerDocDescriptorV2(t)

	e1 := idx.CollectEntries(md1)
	require.Len(t, e1, 2)
	e1[0] = "mutated"
	e1b := idx.CollectEntries(md1)
	require.NotEqual(t, "mutated", e1b[0])

	e2 := idx.CollectEntries(md2)
	require.Len(t, e2, 3)
	require.Contains(t, e2, "5|string|1")
}

func TestIndexableFilter(t *testing.T) {
	idx := NewIndexer(nil, nil)
	md := buildIndexerDocDescriptorV1(t)
	m := dynamicpb.NewMessage(md)

	ok, err := idx.IndexableFilter(m, filters.Where("status").StringEquals("up").AndWhere("meta.role").StringEquals("admin"))
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = idx.IndexableFilter(m, filters.Where("status").StringEquals("up").AndWhere("description").StringEquals("x"))
	require.NoError(t, err)
	require.False(t, ok)

	_, err = idx.IndexableFilter(m, filters.Where("meta.nope").StringEquals("x"))
	require.Error(t, err)

	nestedKey := buildIndexerNestedKeyDescriptor(t)
	nestedMsg := dynamicpb.NewMessage(nestedKey)
	ok, err = idx.IndexableFilter(nestedMsg, filters.Where("metadata.id").StringEquals("x"))
	require.NoError(t, err)
	require.True(t, ok)
}

func TestEnforceUnique(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	idx := NewIndexer(nil, nil)
	md := buildUniqueDocDescriptor(t)
	emailFD := md.Fields().ByName("email")
	require.NotNil(t, emailFD)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)

	enc, err := encodeValue(emailFD, protoreflect.ValueOfString("a@example.com"))
	require.NoError(t, err)
	bm := bitmap.NewWith(1)
	bm.Set(7)
	require.NoError(t, tx.Set(ctx, shardKey(md.FullName(), "2", enc, 0), bm.Bytes(), 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	rtx, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer rtx.Close(ctx)

	same := dynamicpb.NewMessage(md)
	same.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("u7"))
	same.Set(emailFD, protoreflect.ValueOfString("a@example.com"))
	err = idx.EnforceUnique(ctx, indexTx{tx: rtx}, same, 7)
	require.NoError(t, err)

	other := dynamicpb.NewMessage(md)
	other.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("u9"))
	other.Set(emailFD, protoreflect.ValueOfString("a@example.com"))
	err = idx.EnforceUnique(ctx, indexTx{tx: rtx}, other, 9)
	require.ErrorContains(t, err, "unique index violation")
}

func TestEnforceUniqueDeduplicatesCollectedValues(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	idx := NewIndexer(nil, nil)
	md := buildUniqueTagsDescriptor(t)
	tagsFD := md.Fields().ByName("tags")
	require.NotNil(t, tagsFD)

	tx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	enc, err := encodeValue(tagsFD, protoreflect.ValueOfString("dup"))
	require.NoError(t, err)
	bm := bitmap.NewWith(1)
	bm.Set(42)
	require.NoError(t, tx.Set(ctx, shardKey(md.FullName(), "2", enc, 0), bm.Bytes(), 0))
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, tx.Close(ctx))

	rtx, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer rtx.Close(ctx)

	m := dynamicpb.NewMessage(md)
	m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString("k1"))
	l := m.Mutable(tagsFD).List()
	l.Append(protoreflect.ValueOfString("dup"))
	l.Append(protoreflect.ValueOfString("dup"))
	err = idx.EnforceUnique(ctx, indexTx{tx: rtx}, m, 42)
	require.NoError(t, err)
}

func TestOrderedUIDGroupsSeq(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	md := buildIndexerDocDescriptorV1(t)
	statusFD := md.Fields().ByName("status")
	require.NotNil(t, statusFD)
	path := "2"

	wtx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)

	downEnc, err := encodeValue(statusFD, protoreflect.ValueOfString("down"))
	require.NoError(t, err)
	upEnc, err := encodeValue(statusFD, protoreflect.ValueOfString("up"))
	require.NoError(t, err)
	midEnc, err := encodeValue(statusFD, protoreflect.ValueOfString("mid"))
	require.NoError(t, err)

	bmDown := bitmap.NewWith(1)
	bmDown.Set(2)
	require.NoError(t, wtx.Set(ctx, shardKey(md.FullName(), path, downEnc, 0), bmDown.Bytes(), 0))
	bmUp := bitmap.NewWith(1)
	bmUp.Set(1)
	require.NoError(t, wtx.Set(ctx, shardKey(md.FullName(), path, upEnc, 0), bmUp.Bytes(), 0))
	require.NoError(t, wtx.Set(ctx, deltaKey(md.FullName(), path, midEnc, 0, 3), []byte{deltaAdd}, 0))
	require.NoError(t, wtx.Set(ctx, deltaKey(md.FullName(), path, upEnc, 0, 4), []byte{deltaAdd}, 0))
	require.NoError(t, wtx.Commit(ctx))
	require.NoError(t, wtx.Close(ctx))

	rtx, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer rtx.Close(ctx)

	idx := NewIndexer(nil, nil)
	fds := []protoreflect.FieldDescriptor{statusFD}

	ascSeq, err := idx.OrderedUIDGroupsSeq(ctx, indexTx{tx: rtx}, md.FullName(), fds, false)
	require.NoError(t, err)
	var asc [][]uint64
	for uids, err := range ascSeq {
		require.NoError(t, err)
		asc = append(asc, uids)
	}
	require.Equal(t, [][]uint64{{3}, {1, 4}, {2}}, asc)

	descSeq, err := idx.OrderedUIDGroupsSeq(ctx, indexTx{tx: rtx}, md.FullName(), fds, true)
	require.NoError(t, err)
	var desc [][]uint64
	for uids, err := range descSeq {
		require.NoError(t, err)
		desc = append(desc, uids)
	}
	require.Equal(t, [][]uint64{{1, 4}, {3}}, desc)
}

func TestOrderedUIDGroupsSeqContextCanceled(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	md := buildIndexerDocDescriptorV1(t)
	statusFD := md.Fields().ByName("status")
	require.NotNil(t, statusFD)

	wtx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	enc, err := encodeValue(statusFD, protoreflect.ValueOfString("up"))
	require.NoError(t, err)
	bm := bitmap.NewWith(1)
	bm.Set(1)
	require.NoError(t, wtx.Set(ctx, shardKey(md.FullName(), "2", enc, 0), bm.Bytes(), 0))
	require.NoError(t, wtx.Commit(ctx))
	require.NoError(t, wtx.Close(ctx))

	rtx, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer rtx.Close(ctx)

	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	idx := NewIndexer(nil, nil)
	seq, err := idx.OrderedUIDGroupsSeq(cctx, indexTx{tx: rtx}, md.FullName(), []protoreflect.FieldDescriptor{statusFD}, false)
	require.NoError(t, err)
	for _, err := range seq {
		require.ErrorIs(t, err, context.Canceled)
		break
	}
}

func TestIndexFieldFromKey(t *testing.T) {
	name := protoreflect.FullName("tests.index.Doc")
	fieldPath := "2.1"
	key := shardKey(name, fieldPath, []byte("value"), 0)
	base := []byte(protodb.Index + "/tests.index.Doc/")

	path, ok := indexFieldFromKey(base, key)
	require.True(t, ok)
	require.Equal(t, fieldPath, path)

	_, ok = indexFieldFromKey(base, []byte("bad"))
	require.False(t, ok)

	malformed := append([]byte{}, base...)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 10)
	malformed = append(malformed, lenBuf...)
	malformed = append(malformed, []byte("x")...)
	_, ok = indexFieldFromKey(base, malformed)
	require.False(t, ok)
}

func TestDeleteKeys(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	wtx, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	k1 := []byte("k/1")
	k2 := []byte("k/2")
	require.NoError(t, wtx.Set(ctx, k1, []byte("1"), 0))
	require.NoError(t, wtx.Set(ctx, k2, []byte("2"), 0))
	require.NoError(t, deleteKeys(ctx, wtx, [][]byte{k1, k2}))
	require.NoError(t, wtx.Commit(ctx))
	require.NoError(t, wtx.Close(ctx))

	rtx, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer rtx.Close(ctx)
	_, err = rtx.Get(ctx, k1)
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
	_, err = rtx.Get(ctx, k2)
	require.ErrorIs(t, err, badger.ErrKeyNotFound)
}

type indexTx struct {
	tx badgerd.Tx
}

func (x indexTx) Txn() badgerd.Tx {
	return x.tx
}

func (x indexTx) UID(context.Context, []byte, bool) (uint64, bool, error) {
	return 0, false, nil
}

func buildIndexerDocDescriptorV1(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fd := buildIndexerDocDescriptorFile(t, false)
	md := fd.Messages().ByName("Doc")
	require.NotNil(t, md)
	return md
}

func buildIndexerDocDescriptorV2(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fd := buildIndexerDocDescriptorFile(t, true)
	md := fd.Messages().ByName("Doc")
	require.NotNil(t, md)
	return md
}

func buildIndexerDocDescriptorFile(t *testing.T, includeLevel bool) protoreflect.FileDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)
	idxOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(idxOpts, protopts.E_Index, &protopts.Index{})

	metaRole := protobuilder.NewField("role", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(idxOpts)
	meta := protobuilder.NewMessage("Meta").AddField(metaRole)

	msg := protobuilder.NewMessage("Doc").
		AddField(protobuilder.NewField("key", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)).
		AddField(protobuilder.NewField("status", protobuilder.FieldTypeString()).SetNumber(2).SetOptions(idxOpts)).
		AddField(protobuilder.NewField("description", protobuilder.FieldTypeString()).SetNumber(3)).
		AddField(protobuilder.NewField("meta", protobuilder.FieldTypeMessage(meta)).SetNumber(4)).
		AddNestedMessage(meta)
	if includeLevel {
		msg.AddField(protobuilder.NewField("level", protobuilder.FieldTypeString()).SetNumber(5).SetOptions(idxOpts))
	}

	file := protobuilder.NewFile("tests/indexer_doc.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	return fd
}

func buildUniqueDocDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)
	unique := &descriptorpb.FieldOptions{}
	proto.SetExtension(unique, protopts.E_Index, &protopts.Index{Unique: proto.Bool(true)})

	msg := protobuilder.NewMessage("User").
		AddField(protobuilder.NewField("key", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)).
		AddField(protobuilder.NewField("email", protobuilder.FieldTypeString()).SetNumber(2).SetOptions(unique))

	file := protobuilder.NewFile("tests/indexer_unique.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	md := fd.Messages().ByName("User")
	require.NotNil(t, md)
	return md
}

func buildIndexerNestedKeyDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	meta := protobuilder.NewMessage("Metadata").
		AddField(protobuilder.NewField("id", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts))

	msg := protobuilder.NewMessage("NestedKeyDoc").
		AddField(protobuilder.NewField("metadata", protobuilder.FieldTypeMessage(meta)).SetNumber(1)).
		AddField(protobuilder.NewField("status", protobuilder.FieldTypeString()).SetNumber(2).SetOptions(&descriptorpb.FieldOptions{})).
		AddNestedMessage(meta)

	file := protobuilder.NewFile("tests/indexer_nested_key.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)

	fd, err := file.Build()
	require.NoError(t, err)
	md := fd.Messages().ByName("NestedKeyDoc")
	require.NotNil(t, md)
	return md
}

func buildUniqueTagsDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)
	unique := &descriptorpb.FieldOptions{}
	proto.SetExtension(unique, protopts.E_Index, &protopts.Index{Unique: proto.Bool(true)})

	msg := protobuilder.NewMessage("WithTags").
		AddField(protobuilder.NewField("key", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)).
		AddField(protobuilder.NewField("tags", protobuilder.FieldTypeString()).SetNumber(2).SetOptions(unique).SetRepeated())

	file := protobuilder.NewFile("tests/indexer_unique_tags.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	md := fd.Messages().ByName("WithTags")
	require.NotNil(t, md)
	return md
}

func TestCollectEntriesDeterministic(t *testing.T) {
	reg, err := regpkg.New()
	require.NoError(t, err)
	idx := NewIndexer(reg, nil)
	md := buildIndexerDocDescriptorV1(t)
	first := idx.CollectEntries(md)
	second := idx.CollectEntries(md)
	require.Equal(t, first, second)
}
