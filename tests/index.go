// Copyright 2026 Linka Cloud  All rights reserved.
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

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jhump/protoreflect/v2/protobuilder"
	require2 "github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb"
	protopts "go.linka.cloud/protodb/protodb"
)

func TestIndexCRUD(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	fd := buildIndexCRUDFile(t)
	require.NoError(db.Register(ctx, fd))
	time.Sleep(100 * time.Millisecond)

	md := fd.Messages().ByName("Indexed")
	require.NotNil(md)

	t.Run("simple", func(t *testing.T) {
		fUp := filters.Where("status").StringEquals("up")
		fDown := filters.Where("status").StringEquals("down")
		m1 := newIndexCRUDMessage(md, "k1", "up")
		m2 := newIndexCRUDMessage(md, "k2", "down")

		t.Run("create", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			_, err = tx.Set(ctx, m1)
			require.NoError(err)
			_, err = tx.Set(ctx, m2)
			require.NoError(err)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fUp))
			t.Logf("query simple/create status=up took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 1)
			require.Equal("k1", keyFromDynamic(t, res[0]))
		})

		t.Run("update", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			m1.Set(md.Fields().ByName("status"), protoreflect.ValueOfString("down"))
			_, err = tx.Set(ctx, m1)
			require.NoError(err)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fUp))
			t.Logf("query simple/update status=up took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 0)
			start = time.Now()
			res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fDown))
			t.Logf("query simple/update status=down took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 2)
		})

		t.Run("delete", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			require.NoError(tx.Delete(ctx, m1))
			require.NoError(tx.Delete(ctx, m2))
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fDown))
			t.Logf("query simple/delete status=down took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 0)
		})
	})

	t.Run("order-by", func(t *testing.T) {
		m0 := newIndexCRUDMessage(md, "k0", "down")
		m1 := newIndexCRUDMessage(md, "k2", "up")
		m2 := newIndexCRUDMessage(md, "k1", "up")
		tx, err := db.Tx(ctx)
		require.NoError(err)
		defer tx.Close()
		_, err = tx.Set(ctx, m0)
		require.NoError(err)
		_, err = tx.Set(ctx, m1)
		require.NoError(err)
		_, err = tx.Set(ctx, m2)
		require.NoError(err)
		require.NoError(tx.Commit(ctx))

		res, pi, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithOrderByAsc("status"), protodb.WithPaging(&protodb.Paging{Limit: 2}))
		require.NoError(err)
		require.Len(res, 2)
		require.True(pi.GetHasNext())
		require.Equal("k0", keyFromDynamic(t, res[0]))
		require.Equal("k1", keyFromDynamic(t, res[1]))

		res, pi, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithOrderByAsc("status"), protodb.WithPaging(&protodb.Paging{Limit: 2, Token: pi.GetToken()}))
		require.NoError(err)
		require.Len(res, 1)
		require.False(pi.GetHasNext())
		require.Equal("k2", keyFromDynamic(t, res[0]))

		res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithOrderByDesc("key"))
		require.NoError(err)
		require.Len(res, 3)
		require.Equal("k2", keyFromDynamic(t, res[0]))
		require.Equal("k1", keyFromDynamic(t, res[1]))
		require.Equal("k0", keyFromDynamic(t, res[2]))

		_, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithOrderByAsc("status"), protodb.WithReverse())
		require.Error(err)

		tx, err = db.Tx(ctx)
		require.NoError(err)
		defer tx.Close()
		require.NoError(tx.Delete(ctx, m0))
		require.NoError(tx.Delete(ctx, m1))
		require.NoError(tx.Delete(ctx, m2))
		require.NoError(tx.Commit(ctx))
	})

	t.Run("unique", func(t *testing.T) {
		ufd := buildIndexCRUDUniqueFile(t)
		require.NoError(db.Register(ctx, ufd))
		time.Sleep(100 * time.Millisecond)
		umd := ufd.Messages().ByName("User")
		require.NotNil(umd)
		fA := filters.Where("email").StringEquals("a@example.com")
		fB := filters.Where("email").StringEquals("b@example.com")
		u := newUniqueCRUDUser(umd, "u1", "a@example.com")

		t.Run("create", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			_, err = tx.Set(ctx, u)
			require.NoError(err)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(umd), protodb.WithFilter(fA))
			t.Logf("query unique/create email=a took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 1)
		})

		t.Run("update", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			u.Set(umd.Fields().ByName("email"), protoreflect.ValueOfString("b@example.com"))
			_, err = tx.Set(ctx, u)
			require.NoError(err)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(umd), protodb.WithFilter(fA))
			t.Logf("query unique/update email=a took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 0)
			start = time.Now()
			res, _, err = db.Get(ctx, dynamicpb.NewMessage(umd), protodb.WithFilter(fB))
			t.Logf("query unique/update email=b took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 1)
		})

		t.Run("delete", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			require.NoError(tx.Delete(ctx, u))
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(umd), protodb.WithFilter(fB))
			t.Logf("query unique/delete email=b took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 0)
		})
	})

	t.Run("high-cardinality", func(t *testing.T) {
		const (
			count       = 100_000
			updateStart = count / 10
			updateCount = count / 10
			deleteCount = count / 20
			div         = 100
		)
		oldStatus := fmt.Sprintf("s%06d", updateStart)
		newStatus := fmt.Sprintf("s%07d", count+1)
		fOld := filters.Where("status").StringEquals(oldStatus)
		fNew := filters.Where("status").StringEquals(newStatus)

		t.Run("create", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			step := count / div
			for i := range count {
				key := fmt.Sprintf("k%06d", i)
				status := fmt.Sprintf("s%06d", i)
				_, err = tx.Set(ctx, newIndexCRUDMessage(md, key, status))
				require.NoError(err)
				if i%step == 0 {
					p := int(float64(i) / float64(count) * 100)
					t.Logf("%d%%: inserted %d items", p, i)
				}
			}
			t.Logf("100%%: inserted %d items", count)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fOld))
			t.Logf("query high-cardinality/create status=%s took %v", oldStatus, time.Since(start))
			require.NoError(err)
			require.Len(res, 1)
		})

		t.Run("update", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			step := updateCount / div
			for i := range updateCount {
				id := updateStart + i
				_, err = tx.Set(ctx, newIndexCRUDMessage(md, fmt.Sprintf("k%06d", id), newStatus))
				require.NoError(err)
				if i%step == 0 {
					p := int(float64(i) / float64(updateCount) * 100)
					t.Logf("%d%%: updated %d items", p, i)
				}
			}
			t.Logf("100%%: updated %d items", updateCount)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fOld))
			t.Logf("query high-cardinality/update old-status took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, 0)
			start = time.Now()
			res, _, err = db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fNew))
			t.Logf("query high-cardinality/update new-status took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, updateCount)
		})

		t.Run("delete", func(t *testing.T) {
			tx, err := db.Tx(ctx)
			require.NoError(err)
			defer tx.Close()
			step := deleteCount / div
			for i := range deleteCount {
				id := updateStart + i
				require.NoError(tx.Delete(ctx, newIndexCRUDMessage(md, fmt.Sprintf("k%06d", id), newStatus)))
				if i%step == 0 {
					p := int(float64(i) / float64(deleteCount) * 100)
					t.Logf("%d%%: deleted %d items", p, i)
				}
			}
			t.Logf("100%%: deleted %d items", deleteCount)
			require.NoError(tx.Commit(ctx))

			start := time.Now()
			res, _, err := db.Get(ctx, dynamicpb.NewMessage(md), protodb.WithFilter(fNew))
			t.Logf("query high-cardinality/delete new-status took %v", time.Since(start))
			require.NoError(err)
			require.Len(res, updateCount-deleteCount)
		})
	})
}

func TestIndexCreation(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	fdV1 := buildIndexCreationFile(t, false)
	require.NoError(db.Register(ctx, fdV1))
	time.Sleep(100 * time.Millisecond)

	mdV1 := fdV1.Messages().ByName("IndexCreate")
	require.NotNil(mdV1)

	const (
		count = 100_000
		div   = 100
	)
	target := fmt.Sprintf("s%06d", count/2)
	f := filters.Where("status").StringEquals(target)

	t.Run("insert without index", func(t *testing.T) {
		tx, err := db.Tx(ctx)
		require.NoError(err)
		defer tx.Close()
		step := count / div
		for i := range count {
			key := fmt.Sprintf("k%06d", i)
			status := fmt.Sprintf("s%06d", i)
			_, err = tx.Set(ctx, newIndexCreationMessage(mdV1, key, status))
			require.NoError(err)
			if i%step == 0 {
				p := int(float64(i) / float64(count) * 100)
				t.Logf("%d%%: inserted %d items", p, i)
			}
		}
		t.Logf("100%%: inserted %d items", count)
		require.NoError(tx.Commit(ctx))
	})

	t.Run("query before index", func(t *testing.T) {
		start := time.Now()
		res, _, err := db.Get(ctx, dynamicpb.NewMessage(mdV1), protodb.WithFilter(f))
		d := time.Since(start)
		t.Logf("query before index took %v", d)
		require.NoError(err)
		require.Len(res, 1)
		require.Equal(fmt.Sprintf("k%06d", count/2), keyFromDynamic(t, res[0]))
	})

	t.Run("add index", func(t *testing.T) {
		fdV2 := buildIndexCreationFile(t, true)
		start := time.Now()
		require.NoError(db.Register(ctx, fdV2))
		t.Logf("register with index took %v", time.Since(start))
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("query after index", func(t *testing.T) {
		fdV2 := buildIndexCreationFile(t, true)
		mdV2 := fdV2.Messages().ByName("IndexCreate")
		require.NotNil(mdV2)
		start := time.Now()
		res, _, err := db.Get(ctx, dynamicpb.NewMessage(mdV2), protodb.WithFilter(f))
		d := time.Since(start)
		t.Logf("query after index took %v", d)
		require.NoError(err)
		require.Len(res, 1)
		require.Equal(fmt.Sprintf("k%06d", count/2), keyFromDynamic(t, res[0]))
	})
}

func buildIndexCRUDFile(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)
	idxOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(idxOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(false)})

	key := protobuilder.NewField("key", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)
	status := protobuilder.NewField("status", protobuilder.FieldTypeString()).SetNumber(2).SetOptions(idxOpts)
	msg := protobuilder.NewMessage("Indexed").AddField(key).AddField(status)
	file := protobuilder.NewFile("tests/index_crud.proto").
		SetPackageName(protoreflect.FullName("tests.indexcrud")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require2.NoError(t, err)
	return fd
}

func buildIndexCRUDUniqueFile(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)
	uniqueOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(uniqueOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(true)})

	key := protobuilder.NewField("key", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)
	email := protobuilder.NewField("email", protobuilder.FieldTypeString()).SetNumber(2).SetOptions(uniqueOpts)
	msg := protobuilder.NewMessage("User").AddField(key).AddField(email)
	file := protobuilder.NewFile("tests/index_crud_unique.proto").
		SetPackageName(protoreflect.FullName("tests.indexcrud.unique")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require2.NoError(t, err)
	return fd
}

func newIndexCRUDMessage(md protoreflect.MessageDescriptor, key, status string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if status != "" {
		m.Set(md.Fields().ByName("status"), protoreflect.ValueOfString(status))
	}
	return m
}

func newUniqueCRUDUser(md protoreflect.MessageDescriptor, key, email string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if email != "" {
		m.Set(md.Fields().ByName("email"), protoreflect.ValueOfString(email))
	}
	return m
}

func keyFromDynamic(t *testing.T, msg proto.Message) string {
	t.Helper()
	fd := msg.ProtoReflect().Descriptor().Fields().ByName("key")
	require2.NotNil(t, fd)
	return msg.ProtoReflect().Get(fd).String()
}

func buildIndexCreationFile(t *testing.T, indexed bool) protoreflect.FileDescriptor {
	t.Helper()
	keyOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(keyOpts, protopts.E_Key, true)

	statusOpts := &descriptorpb.FieldOptions{}
	if indexed {
		proto.SetExtension(statusOpts, protopts.E_Index, &protopts.Index{Unique: proto.Bool(false)})
	}

	key := protobuilder.NewField("key", protobuilder.FieldTypeString()).SetNumber(1).SetOptions(keyOpts)
	status := protobuilder.NewField("status", protobuilder.FieldTypeString()).SetNumber(2)
	if indexed {
		status.SetOptions(statusOpts)
	}
	msg := protobuilder.NewMessage("IndexCreate").AddField(key).AddField(status)
	file := protobuilder.NewFile("tests/index_creation.proto").
		SetPackageName(protoreflect.FullName("tests.indexcreation")).
		SetSyntax(protoreflect.Proto3).
		AddMessage(msg).
		AddImportedDependency(protopts.File_protodb_protodb_proto)
	fd, err := file.Build()
	require2.NoError(t, err)
	return fd
}

func newIndexCreationMessage(md protoreflect.MessageDescriptor, key, status string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(md)
	if key != "" {
		m.Set(md.Fields().ByName("key"), protoreflect.ValueOfString(key))
	}
	if status != "" {
		m.Set(md.Fields().ByName("status"), protoreflect.ValueOfString(status))
	}
	return m
}
