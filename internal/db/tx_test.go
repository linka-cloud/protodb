package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/token"
	v1alpha1 "go.linka.cloud/protodb/protodb/v1alpha1"
)

func TestBuildOrderPlan(t *testing.T) {
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(t.TempDir()), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	d := dbif.(*db)
	require.NoError(t, d.RegisterProto(ctx, buildIndexedFileDescriptor(t)))
	md, err := lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)
	m := dynamicpb.NewMessage(md)

	t.Run("empty field", func(t *testing.T) {
		_, err := buildOrderPlan(m.ProtoReflect(), &v1alpha1.OrderBy{})
		require.ErrorContains(t, err, "cannot be empty")
	})

	t.Run("non sortable repeated field", func(t *testing.T) {
		_, err := buildOrderPlan(m.ProtoReflect(), &v1alpha1.OrderBy{Field: "tags"})
		require.ErrorContains(t, err, "not sortable")
	})

	t.Run("nested sortable indexed field", func(t *testing.T) {
		pl, err := buildOrderPlan(m.ProtoReflect(), &v1alpha1.OrderBy{Field: "meta.role"})
		require.NoError(t, err)
		require.Equal(t, "meta.role", pl.fieldPath)
		require.Equal(t, v1alpha1.OrderDirectionAsc, pl.direction)
	})

	t.Run("default direction asc", func(t *testing.T) {
		pl, err := buildOrderPlan(m.ProtoReflect(), &v1alpha1.OrderBy{Field: "status"})
		require.NoError(t, err)
		require.Equal(t, v1alpha1.OrderDirectionAsc, pl.direction)
	})

	t.Run("invalid direction", func(t *testing.T) {
		_, err := buildOrderPlan(m.ProtoReflect(), &v1alpha1.OrderBy{Field: "status", Direction: v1alpha1.OrderDirection(99)})
		require.ErrorContains(t, err, "invalid direction")
	})
}

func TestGetContinuationValidation(t *testing.T) {
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(t.TempDir()), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()
	d := dbif.(*db)

	require.NoError(t, d.RegisterProto(ctx, buildIndexedFileDescriptor(t)))
	require.NoError(t, d.RegisterProto(ctx, buildUniqueIndexedDescriptor(t)))

	indexedMD, err := lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)
	uniqueMD, err := lookupMessage(d, "tests.unique.User")
	require.NoError(t, err)

	_, err = d.Set(ctx, newIndexedMessage(indexedMD, "k1", "up", nil, "admin", "a"))
	require.NoError(t, err)
	_, err = d.Set(ctx, newIndexedMessage(indexedMD, "k2", "up", nil, "admin", "b"))
	require.NoError(t, err)
	_, err = d.Set(ctx, newUniqueUser(uniqueMD, "u1", "a@example.com"))
	require.NoError(t, err)

	t.Run("invalid token decode", func(t *testing.T) {
		_, _, err := d.Get(ctx, dynamicpb.NewMessage(indexedMD), protodb.WithPaging(&v1alpha1.Paging{Token: "%%%"}))
		require.ErrorIs(t, err, token.ErrInvalid)
	})

	t.Run("token type mismatch", func(t *testing.T) {
		_, pi, err := d.Get(ctx, dynamicpb.NewMessage(indexedMD), protodb.WithPaging(&v1alpha1.Paging{Limit: 1}))
		require.NoError(t, err)
		require.NotEmpty(t, pi.GetToken())

		_, _, err = d.Get(ctx, dynamicpb.NewMessage(uniqueMD), protodb.WithPaging(&v1alpha1.Paging{Limit: 1, Token: pi.GetToken()}))
		require.ErrorIs(t, err, token.ErrInvalid)
		require.ErrorContains(t, err, "token valid for")
	})

	t.Run("token filters mismatch", func(t *testing.T) {
		_, pi, err := d.Get(ctx, dynamicpb.NewMessage(indexedMD), protodb.WithPaging(&v1alpha1.Paging{Limit: 1}))
		require.NoError(t, err)

		_, _, err = d.Get(ctx, dynamicpb.NewMessage(indexedMD),
			protodb.WithFilter(filters.Where("status").StringEquals("up")),
			protodb.WithPaging(&v1alpha1.Paging{Limit: 1, Token: pi.GetToken()}),
		)
		require.ErrorIs(t, err, token.ErrInvalid)
		require.ErrorContains(t, err, "filters mismatch")
	})

	t.Run("token order mismatch", func(t *testing.T) {
		_, pi, err := d.Get(ctx, dynamicpb.NewMessage(indexedMD),
			protodb.WithOrderByAsc("status"),
			protodb.WithPaging(&v1alpha1.Paging{Limit: 1}),
		)
		require.NoError(t, err)

		_, _, err = d.Get(ctx, dynamicpb.NewMessage(indexedMD),
			protodb.WithOrderByDesc("status"),
			protodb.WithPaging(&v1alpha1.Paging{Limit: 1, Token: pi.GetToken()}),
		)
		require.ErrorIs(t, err, token.ErrInvalid)
		require.ErrorContains(t, err, "order mismatch")
	})

	t.Run("missing continuation key", func(t *testing.T) {
		plan, err := buildOrderPlan(dynamicpb.NewMessage(indexedMD).ProtoReflect(), &v1alpha1.OrderBy{Field: "key", Direction: v1alpha1.OrderDirectionAsc})
		require.NoError(t, err)
		fhash, err := hash(nil)
		require.NoError(t, err)
		tk, err := (&token.Token{Type: string(indexedMD.FullName()), FiltersHash: fhash, OrderHash: orderHash(plan)}).Encode()
		require.NoError(t, err)

		_, _, err = d.Get(ctx, dynamicpb.NewMessage(indexedMD),
			protodb.WithOrderByAsc("key"),
			protodb.WithPaging(&v1alpha1.Paging{Limit: 1, Token: tk}),
		)
		require.ErrorIs(t, err, token.ErrInvalid)
		require.ErrorContains(t, err, "missing continuation key")
	})
}

func TestFieldMaskTouchesIndexed(t *testing.T) {
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(t.TempDir()), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()

	d := dbif.(*db)
	require.NoError(t, d.RegisterProto(ctx, buildIndexedFileDescriptor(t)))
	md, err := lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)

	entries := d.idx.CollectEntries(md)
	require.NotEmpty(t, entries)

	require.True(t, fieldMaskTouchesIndexed(md, nil, entries))
	require.True(t, fieldMaskTouchesIndexed(md, []string{"status"}, entries))
	require.True(t, fieldMaskTouchesIndexed(md, []string{"meta.role"}, entries))
	require.False(t, fieldMaskTouchesIndexed(md, []string{"description"}, entries))
	require.True(t, fieldMaskTouchesIndexed(md, []string{"nope"}, entries))

	path, err := fieldMaskPathToNumberPath(md, "meta.role")
	require.NoError(t, err)
	require.Equal(t, "4.1", path)

	_, err = fieldMaskPathToNumberPath(md, "meta.nope")
	require.Error(t, err)
}

func TestUIDMalformedReverseValue(t *testing.T) {
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(t.TempDir()), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()
	d := dbif.(*db)

	key := []byte(protodb.Data + "/tests.index.Indexed/k1")
	urk := protodb.UIDRevKey(key)

	btx, err := d.bdb.NewTransaction(ctx, true)
	require.NoError(t, err)
	require.NoError(t, btx.Set(ctx, urk, []byte("bad"), 0))
	require.NoError(t, btx.Commit(ctx))
	require.NoError(t, btx.Close(ctx))

	rtx, err := d.bdb.NewTransaction(ctx, false)
	require.NoError(t, err)
	defer rtx.Close(ctx)

	_, _, err = (&tx{db: d, txn: rtx}).uid(ctx, key, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid uid value")
}

func TestOrderAndMatchHelpers(t *testing.T) {
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(t.TempDir()), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()
	d := dbif.(*db)

	require.NoError(t, d.RegisterProto(ctx, buildIndexedFileDescriptor(t)))
	md, err := lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)

	m1 := newIndexedMessage(md, "k1", "up", nil, "admin", "a")
	m2 := newIndexedMessage(md, "k2", "down", nil, "admin", "b")

	statusFDs, err := lookupNumberPath(md, "2")
	require.NoError(t, err)
	statusOrder := orderField{path: statusFDs}

	v1, ok1 := fieldValue(m1.ProtoReflect(), statusFDs)
	v2, ok2 := fieldValue(m2.ProtoReflect(), statusFDs)
	require.True(t, ok1)
	require.True(t, ok2)
	require.Greater(t, compareMaybeValue(statusOrder, v1, ok1, v2, ok2), 0)
	require.Equal(t, 0, compareMaybeValue(statusOrder, protoreflect.Value{}, false, protoreflect.Value{}, false))
	require.Less(t, compareMaybeValue(statusOrder, protoreflect.Value{}, false, v1, ok1), 0)

	metaFDs, err := lookupNumberPath(md, "4")
	require.NoError(t, err)
	_, ok := fieldValue(newIndexedMessage(md, "k3", "up", nil, "", "").ProtoReflect(), metaFDs)
	require.False(t, ok)

	pl1, err := buildOrderPlan(dynamicpb.NewMessage(md).ProtoReflect(), &v1alpha1.OrderBy{Field: "status", Direction: v1alpha1.OrderDirectionAsc})
	require.NoError(t, err)
	pl2, err := buildOrderPlan(dynamicpb.NewMessage(md).ProtoReflect(), &v1alpha1.OrderBy{Field: "status", Direction: v1alpha1.OrderDirectionAsc})
	require.NoError(t, err)
	pl3, err := buildOrderPlan(dynamicpb.NewMessage(md).ProtoReflect(), &v1alpha1.OrderBy{Field: "status", Direction: v1alpha1.OrderDirectionDesc})
	require.NoError(t, err)
	require.Equal(t, orderHash(pl1), orderHash(pl2))
	require.NotEqual(t, orderHash(pl1), orderHash(pl3))

	f := filters.Where("key").StringHasPrefix("ab")
	require.True(t, IsKeyOnlyFilter(f, "key"))
	p, ok := MatchPrefixOnly(f)
	require.True(t, ok)
	require.Equal(t, "ab", p)

	andFilter := filters.Where("key").StringHasPrefix("ab").AndWhere("key").StringEquals("ab1")
	require.True(t, IsKeyOnlyFilter(andFilter, "key"))

	orFilter := filters.Where("key").StringEquals("k0").OrWhere("key").StringHasPrefix("ab")
	ok, err = MatchKey(orFilter, "ab2")
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = MatchKey(andFilter, "ab2")
	require.NoError(t, err)
	require.False(t, ok)

	nonPrefix := filters.Where("key").StringEquals("ab")
	_, ok = MatchPrefixOnly(nonPrefix)
	require.False(t, ok)

	require.False(t, IsKeyOnlyFilter(filters.Where("status").StringEquals("up"), "key"))

	fh, err := hash(nil)
	require.NoError(t, err)
	require.NotEmpty(t, fh)
}

func lookupNumberPath(md protoreflect.MessageDescriptor, path string) ([]protoreflect.FieldDescriptor, error) {
	parts := []rune(path)
	cur := md
	out := make([]protoreflect.FieldDescriptor, 0, len(parts))
	field := ""
	for _, r := range parts {
		if r == '.' {
			fd, err := lookupNumberField(cur, field)
			if err != nil {
				return nil, err
			}
			out = append(out, fd)
			cur = fd.Message()
			field = ""
			continue
		}
		field += string(r)
	}
	if field == "" {
		return nil, fmt.Errorf("invalid path %q", path)
	}
	fd, err := lookupNumberField(cur, field)
	if err != nil {
		return nil, err
	}
	return append(out, fd), nil
}

func lookupNumberField(md protoreflect.MessageDescriptor, n string) (protoreflect.FieldDescriptor, error) {
	for i := 0; i < md.Fields().Len(); i++ {
		fd := md.Fields().Get(i)
		if fmt.Sprintf("%d", fd.Number()) == n {
			return fd, nil
		}
	}
	return nil, fmt.Errorf("field number %s not found", n)
}

func TestGetWithMalformedTokenReturnsInvalid(t *testing.T) {
	ctx := context.Background()
	dbif, err := Open(ctx, WithPath(t.TempDir()), WithApplyDefaults(true))
	require.NoError(t, err)
	defer dbif.Close()
	d := dbif.(*db)
	require.NoError(t, d.RegisterProto(ctx, buildIndexedFileDescriptor(t)))
	md, err := lookupMessage(d, "tests.index.Indexed")
	require.NoError(t, err)
	_, _, err = d.Get(ctx, dynamicpb.NewMessage(md), protodb.WithPaging(&v1alpha1.Paging{Token: "!!!!"}))
	require.Error(t, err)
	require.ErrorIs(t, err, token.ErrInvalid)
	require.NotErrorIs(t, err, badger.ErrKeyNotFound)
}
