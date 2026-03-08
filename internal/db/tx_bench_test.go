package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	iprotodb "go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/pb"
	protopts "go.linka.cloud/protodb/protodb"
)

func BenchmarkTxSet(b *testing.B) {
	ctx := context.Background()
	db, md := openBenchDB(b)
	payload := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	b.Run("insert", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db)
			require.NoError(b, err)
			_, err = tx.set(ctx, newBenchDoc(md, fmt.Sprintf("insert-%d", i), "up", payload))
			require.NoError(b, err)
			require.NoError(b, tx.Commit(ctx))
		}
	})

	seedTx, err := newTx(ctx, db)
	require.NoError(b, err)
	_, err = seedTx.set(ctx, newBenchDoc(md, "update-key", "up", payload))
	require.NoError(b, err)
	require.NoError(b, seedTx.Commit(ctx))

	b.Run("update", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db)
			require.NoError(b, err)
			_, err = tx.set(ctx, newBenchDoc(md, "update-key", "up", fmt.Sprintf("%s-%d", payload, i)))
			require.NoError(b, err)
			require.NoError(b, tx.Commit(ctx))
		}
	})

	seedMaskTx, err := newTx(ctx, db)
	require.NoError(b, err)
	_, err = seedMaskTx.set(ctx, newBenchDoc(md, "mask-key", "up", payload))
	require.NoError(b, err)
	require.NoError(b, seedMaskTx.Commit(ctx))

	b.Run("update_with_fieldmask", func(b *testing.B) {
		b.ReportAllocs()
		opts := []iprotodb.SetOption{iprotodb.WithWriteFieldMaskPaths("payload")}
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db)
			require.NoError(b, err)
			m := newBenchDoc(md, "mask-key", "", fmt.Sprintf("mask-%d", i))
			_, err = tx.set(ctx, m, opts...)
			require.NoError(b, err)
			require.NoError(b, tx.Commit(ctx))
		}
	})
}

func BenchmarkTxGet(b *testing.B) {
	ctx := context.Background()
	db, md := openBenchDB(b)
	seedBenchDocs(b, ctx, db, md, 2000)

	b.Run("direct_key", func(b *testing.B) {
		query := newBenchDoc(md, "k-01000", "", "")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db, iprotodb.WithReadOnly())
			require.NoError(b, err)
			_, _, err = tx.get(ctx, query)
			tx.Close()
			require.NoError(b, err)
		}
	})

	b.Run("filter_scan", func(b *testing.B) {
		query := dynamicpb.NewMessage(md)
		opt := []iprotodb.GetOption{
			iprotodb.WithFilter(filters.Where("status").StringEquals("up")),
			iprotodb.WithPaging(&pb.Paging{Limit: 50}),
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db, iprotodb.WithReadOnly())
			require.NoError(b, err)
			_, _, err = tx.get(ctx, query, opt...)
			tx.Close()
			require.NoError(b, err)
		}
	})

	b.Run("filter_indexed", func(b *testing.B) {
		query := dynamicpb.NewMessage(md)
		opts := []iprotodb.GetOption{
			iprotodb.WithFilter(filters.Where("status").StringEquals("up")),
			iprotodb.WithPaging(&pb.Paging{Limit: 50}),
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db, iprotodb.WithReadOnly())
			require.NoError(b, err)
			_, _, err = tx.get(ctx, query, opts...)
			tx.Close()
			require.NoError(b, err)
		}
	})

	b.Run("filter_non_indexed", func(b *testing.B) {
		query := dynamicpb.NewMessage(md)
		opts := []iprotodb.GetOption{
			iprotodb.WithFilter(filters.Where("payload").StringHasPrefix("seed-payload")),
			iprotodb.WithPaging(&pb.Paging{Limit: 50}),
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := newTx(ctx, db, iprotodb.WithReadOnly())
			require.NoError(b, err)
			_, _, err = tx.get(ctx, query, opts...)
			tx.Close()
			require.NoError(b, err)
		}
	})
}

func openBenchDB(b *testing.B) (*db, protoreflect.MessageDescriptor) {
	b.Helper()
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(b.TempDir()), WithApplyDefaults(true))
	require.NoError(b, err)
	b.Cleanup(func() { dbif.Close() })
	d := dbif.(*db)
	require.NoError(b, d.RegisterProto(ctx, buildBenchDescriptor(b)))
	md, err := lookupMessage(d, "tests.bench.Doc")
	require.NoError(b, err)
	return d, md
}

func seedBenchDocs(b *testing.B, ctx context.Context, d *db, md protoreflect.MessageDescriptor, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		status := "down"
		if i%2 == 0 {
			status = "up"
		}
		_, err := d.Set(ctx, newBenchDoc(md, fmt.Sprintf("k-%05d", i), status, "seed-payload-value"))
		require.NoError(b, err)
	}
}

func newBenchDoc(md protoreflect.MessageDescriptor, key, status, payload string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if status != "" {
		m.Set(md.Fields().ByName("status"), protoreflect.ValueOfString(status))
	}
	if payload != "" {
		m.Set(md.Fields().ByName("payload"), protoreflect.ValueOfString(payload))
	}
	return m
}

func buildBenchDescriptor(t testing.TB) *descriptorpb.FileDescriptorProto {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	keyField := protobuilder.NewField("key", protobuilder.FieldTypeString()).
		SetNumber(1).
		SetOptions(keyOpts)
	statusOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(statusOpts, protopts.E_Index, &protopts.Index{})
	statusField := protobuilder.NewField("status", protobuilder.FieldTypeString()).
		SetNumber(2).
		SetOptions(statusOpts)
	payloadField := protobuilder.NewField("payload", protobuilder.FieldTypeString()).SetNumber(3)

	msg := protobuilder.NewMessage("Doc").
		AddField(keyField).
		AddField(statusField).
		AddField(payloadField)

	file := protobuilder.NewFile("tests/bench.proto").
		SetPackageName(protoreflect.FullName("tests.bench")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require.NoError(t, err)
	return protodesc.ToFileDescriptorProto(fd)
}
