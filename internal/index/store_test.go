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
	"context"
	"math"
	"testing"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/stretchr/testify/require"
	pfindex "go.linka.cloud/protofilters/index"
	"go.linka.cloud/protofilters/index/bitmap"
	_ "go.linka.cloud/protofilters/index/bitmap/roaring"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.linka.cloud/protodb/internal/badgerd"
)

func TestValuePrefixRoundTrip(t *testing.T) {
	name := protoreflect.FullName("tests.index.Indexed")
	fieldPath := "1.2"
	encoded := []byte{0x00, '/', 0xff, 'a', 0x01}
	key := shardKey(name, fieldPath, encoded, 3)
	gotField, gotValue, ok := parseFieldValue(typePrefix(name), key)
	require.True(t, ok)
	require.Equal(t, fieldPath, gotField)
	require.Equal(t, encoded, gotValue)

	shard, ok := parseShard(key)
	require.True(t, ok)
	require.Equal(t, uint32(3), shard)
}

func TestParseFieldValueInvalid(t *testing.T) {
	name := protoreflect.FullName("tests.index.Indexed")
	prefix := typePrefix(name)
	short := append([]byte{}, prefix...)
	short = append(short, 0x00)
	invalidLen := append([]byte{}, prefix...)
	invalidLen = append(invalidLen, make([]byte, 8)...)
	invalidValue := append([]byte{}, prefix...)
	invalidValue = append(invalidValue, []byte{0, 0, 0, 1, 'a', 0, 0, 0, 2, 'b'}...)
	invalidValue = append(invalidValue, shardSuffix(0)...)

	for _, tt := range [][]byte{[]byte("bad"), short, invalidLen, invalidValue} {
		_, _, ok := parseFieldValue(prefix, tt)
		require.False(t, ok)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	md := buildTypesDescriptor(t)
	fields := md.Fields()
	values := map[protoreflect.Name]protoreflect.Value{
		"b":   protoreflect.ValueOfBool(true),
		"s":   protoreflect.ValueOfString("a/b\x00"),
		"bin": protoreflect.ValueOfBytes([]byte{0x00, 0xff, '/'}),
		"e":   protoreflect.ValueOfEnum(1),
		"i32": protoreflect.ValueOfInt32(-123),
		"i64": protoreflect.ValueOfInt64(-987654321),
		"u32": protoreflect.ValueOfUint32(123456),
		"u64": protoreflect.ValueOfUint64(1234567890),
		"f":   protoreflect.ValueOfFloat32(3.14),
		"d":   protoreflect.ValueOfFloat64(2.718281828),
		"ts":  protoreflect.ValueOfMessage((&timestamppb.Timestamp{Seconds: 123, Nanos: 456}).ProtoReflect()),
		"dur": protoreflect.ValueOfMessage((&durationpb.Duration{Seconds: 9, Nanos: 8}).ProtoReflect()),
	}
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		val := values[fd.Name()]
		enc, err := encodeValue(fd, val)
		require.NoError(t, err)
		dec, err := decodeValue(fd, enc)
		require.NoError(t, err)
		assertValueEqual(t, fd, val, dec)
	}

	empty, err := encodeValue(fields.ByName("s"), protoreflect.Value{})
	require.NoError(t, err)
	require.Len(t, empty, 0)
	dec, err := decodeValue(fields.ByName("s"), empty)
	require.NoError(t, err)
	require.False(t, dec.IsValid())
}

func TestReadValueBitmap(t *testing.T) {
	ctx := context.Background()
	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	name := protoreflect.FullName("tests.index.Indexed")
	fieldPath := "status"
	encoded := []byte("up")
	prefix := valuePrefix(name, fieldPath, encoded)

	bt, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	bm0 := bitmap.New()
	bm0.Set(1)
	bm0.Set(2)
	bm1 := bitmap.New()
	bm1.Set(5)
	require.NoError(t, bt.Set(ctx, shardKey(name, fieldPath, encoded, 0), bm0.Bytes(), 0))
	require.NoError(t, bt.Set(ctx, shardKey(name, fieldPath, encoded, 1), bm1.Bytes(), 0))
	require.NoError(t, bt.Commit(ctx))
	require.NoError(t, bt.Close(ctx))

	rt, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	got, err := readValueBitmap(rt, prefix)
	require.NoError(t, err)
	require.NoError(t, rt.Close(ctx))

	vals := map[uint64]struct{}{}
	for v := range got.Iter() {
		vals[v] = struct{}{}
	}
	_, ok := vals[uint64(1)]
	require.True(t, ok)
	_, ok = vals[uint64(2)]
	require.True(t, ok)
	_, ok = vals[(uint64(1)<<uidShardShift)|5]
	require.True(t, ok)
}

func TestBuildFieldReader(t *testing.T) {
	ctx := context.Background()
	md := buildStringDescriptor(t)

	registry := &protoregistry.Files{}
	require.NoError(t, registry.RegisterFile(md.ParentFile()))

	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	bt, err := db.NewTransaction(ctx, true)
	require.NoError(t, err)
	encodedA := []byte("a/b")
	encodedB := []byte{0x00, 0xff}
	require.NoError(t, bt.Set(ctx, shardKey(md.FullName(), "1", encodedA, 0), bitmap.New().Bytes(), 0))
	require.NoError(t, bt.Set(ctx, shardKey(md.FullName(), "1", encodedB, 0), bitmap.New().Bytes(), 0))
	require.NoError(t, bt.Commit(ctx))
	require.NoError(t, bt.Close(ctx))

	rt, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	reader, err := buildFieldReader(rt, registry, md.FullName(), true, nil)
	require.NoError(t, err)
	seq := reader.Get(ctx, "status")
	values := map[string]struct{}{}
	seq(func(f pfindex.Field, err error) bool {
		require.NoError(t, err)
		values[f.Value().String()] = struct{}{}
		return true
	})
	require.Len(t, values, 2)
	require.NoError(t, rt.Close(ctx))
}

func TestBuildFieldReaderUsesInferredKeyField(t *testing.T) {
	ctx := context.Background()
	md := buildIDDescriptor(t)

	reg := &protoregistry.Files{}
	require.NoError(t, reg.RegisterFile(md.ParentFile()))

	db, err := badgerd.Open(ctx, badgerd.WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	rt, err := db.NewTransaction(ctx, false)
	require.NoError(t, err)
	reader, err := buildFieldReader(rt, reg, md.FullName(), true, nil)
	require.NoError(t, err)
	require.NotNil(t, reader.key)
	require.Equal(t, protoreflect.Name("id"), reader.key.name)
	require.NoError(t, rt.Close(ctx))
}

func buildTypesDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	keyEnum := protobuilder.NewEnum("Status").
		AddValue(protobuilder.NewEnumValue("UNKNOWN").SetNumber(0)).
		AddValue(protobuilder.NewEnumValue("UP").SetNumber(1))

	tsDesc, err := protoregistry.GlobalFiles.FindDescriptorByName("google.protobuf.Timestamp")
	require.NoError(t, err)
	durDesc, err := protoregistry.GlobalFiles.FindDescriptorByName("google.protobuf.Duration")
	require.NoError(t, err)

	msg := protobuilder.NewMessage("Types").
		AddField(protobuilder.NewField("b", protobuilder.FieldTypeBool()).SetNumber(1)).
		AddField(protobuilder.NewField("s", protobuilder.FieldTypeString()).SetNumber(2)).
		AddField(protobuilder.NewField("bin", protobuilder.FieldTypeBytes()).SetNumber(3)).
		AddField(protobuilder.NewField("e", protobuilder.FieldTypeEnum(keyEnum)).SetNumber(4)).
		AddField(protobuilder.NewField("i32", protobuilder.FieldTypeInt32()).SetNumber(5)).
		AddField(protobuilder.NewField("i64", protobuilder.FieldTypeInt64()).SetNumber(6)).
		AddField(protobuilder.NewField("u32", protobuilder.FieldTypeUint32()).SetNumber(7)).
		AddField(protobuilder.NewField("u64", protobuilder.FieldTypeUint64()).SetNumber(8)).
		AddField(protobuilder.NewField("f", protobuilder.FieldTypeFloat()).SetNumber(9)).
		AddField(protobuilder.NewField("d", protobuilder.FieldTypeDouble()).SetNumber(10)).
		AddField(protobuilder.NewField("ts", protobuilder.FieldTypeImportedMessage(tsDesc.(protoreflect.MessageDescriptor))).SetNumber(11)).
		AddField(protobuilder.NewField("dur", protobuilder.FieldTypeImportedMessage(durDesc.(protoreflect.MessageDescriptor))).SetNumber(12))

	file := protobuilder.NewFile("tests/types.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddEnum(keyEnum).
		AddMessage(msg)
	fd, err := file.Build()
	require.NoError(t, err)
	md := fd.Messages().ByName("Types")
	require.NotNil(t, md)
	return md
}

func buildStringDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	msg := protobuilder.NewMessage("Indexed").
		AddField(protobuilder.NewField("status", protobuilder.FieldTypeString()).SetNumber(1))
	file := protobuilder.NewFile("tests/index.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg)
	fd, err := file.Build()
	require.NoError(t, err)
	md := fd.Messages().ByName("Indexed")
	require.NotNil(t, md)
	return md
}

func buildIDDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	msg := protobuilder.NewMessage("WithID").
		AddField(protobuilder.NewField("id", protobuilder.FieldTypeString()).SetNumber(1)).
		AddField(protobuilder.NewField("status", protobuilder.FieldTypeString()).SetNumber(2))
	file := protobuilder.NewFile("tests/with_id.proto").
		SetPackageName(protoreflect.FullName("tests.index")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg)
	fd, err := file.Build()
	require.NoError(t, err)
	md := fd.Messages().ByName("WithID")
	require.NotNil(t, md)
	return md
}

func assertValueEqual(t *testing.T, fd protoreflect.FieldDescriptor, exp, got protoreflect.Value) {
	switch fd.Kind() {
	case protoreflect.FloatKind:
		require.InEpsilon(t, exp.Float(), got.Float(), 1e-6)
	case protoreflect.DoubleKind:
		require.InEpsilon(t, exp.Float(), got.Float(), 1e-12)
	case protoreflect.BytesKind:
		require.Equal(t, exp.Bytes(), got.Bytes())
	case protoreflect.MessageKind:
		require.True(t, proto.Equal(exp.Message().Interface(), got.Message().Interface()))
	default:
		require.Equal(t, exp.Interface(), got.Interface())
	}
	if fd.Kind() == protoreflect.FloatKind {
		require.False(t, math.IsNaN(exp.Float()))
	}
}
