// Copyright 2021 Linka Cloud  All rights reserved.
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

package embed

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	assert2 "github.com/stretchr/testify/assert"
	require2 "github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/pb"
	testpb "go.linka.cloud/protodb/tests/pb"
)

var (
	i0 = &testpb.Interface{
		Name: "eth0",
		Addresses: []*testpb.IPAddress{{
			Address: &testpb.IPAddress_IPV4{IPV4: "10.0.0.1/24"},
		}},
		Status: testpb.StatusUp,
	}

	i0Old proto.Message

	i1 = &testpb.Interface{
		Name: "eth1",
		Addresses: []*testpb.IPAddress{{
			Address: &testpb.IPAddress_IPV4{IPV4: "10.0.1.1/24"},
		}},
		Status: testpb.StatusDown,
		Mtu:    9000,
	}
)

func init() {
	i0.Default()
	i1.Default()
}

func TestEmbed(t *testing.T) {
	dbPath := "TestEmbed"
	defer os.RemoveAll(dbPath)
	require := require2.New(t)
	assert := assert2.New(t)
	equal := func(e, g proto.Message) {
		if !assert.True(proto.Equal(e, g)) {
			assert.Equal(e, g)
		}
	}
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := protodb.Open(ctx, protodb.WithPath(dbPath), protodb.WithApplyDefaults(true))
	require.NoError(err)
	assert.NotNil(db)
	defer db.Close()

	watches := make(chan protodb.Event)
	go func() {
		ch, err := db.Watch(ctx, &testpb.Interface{})
		require.NoError(err)
		for e := range ch {
			watches <- e
		}
		close(watches)
	}()

	r, err := db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e := <-watches
	assert.Equal(protodb.EventTypeEnter, e.Type())
	assert.Nil(e.Old())
	equal(i0, e.New())

	is, i, err := db.Get(ctx, &testpb.Interface{}, nil)
	require.NoError(err)
	assert.NotNil(i)
	assert.Len(is, 1)
	equal(i0, is[0])

	r, err = db.Put(ctx, i1)
	require.NoError(err)
	require.NotNil(r)
	equal(i1, r)
	e = <-watches
	assert.Equal(protodb.EventTypeEnter, e.Type())
	assert.Nil(e.Old())
	equal(i1, e.New())

	is, i, err = db.Get(ctx, &testpb.Interface{}, nil)
	require.NoError(err)
	assert.NotNil(i)
	assert.Len(is, 2)
	equal(i0, is[0])
	equal(i1, is[1])

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusDown
	r, err = db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e = <-watches
	assert.Equal(protodb.EventTypeUpdate, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusUp
	r, err = db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e = <-watches
	assert.Equal(protodb.EventTypeUpdate, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	err = db.Delete(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	e = <-watches
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0, e.Old())
	assert.Nil(e.New())

	time.Sleep(time.Second)
	cancel()
	<-watches
}

func TestEmbedWatchWithFilter(t *testing.T) {
	dbPath := "TestEmbedWatchWithFilter"
	defer os.RemoveAll(dbPath)
	require := require2.New(t)
	assert := assert2.New(t)
	equal := func(e, g proto.Message) {
		if !assert.True(proto.Equal(e, g)) {
			assert.Equal(e, g)
		}
	}
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := protodb.Open(ctx, protodb.WithPath(dbPath), protodb.WithApplyDefaults(true))
	require.NoError(err)
	assert.NotNil(db)
	defer db.Close()

	watches := make(chan protodb.Event)
	go func() {
		ch, err := db.Watch(ctx, &testpb.Interface{}, &filters.FieldFilter{
			Field:  testpb.InterfaceFields.Status,
			Filter: filters.NumberEquals(float64(testpb.StatusUp)),
		})
		require.NoError(err)
		for e := range ch {
			watches <- e
		}
		close(watches)
	}()

	r, err := db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e := <-watches
	assert.Equal(protodb.EventTypeEnter, e.Type())
	assert.Nil(e.Old())
	equal(i0, e.New())

	// some noise
	r, err = db.Put(ctx, i1)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i1, r)

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusDown
	r, err = db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e = <-watches
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusUp
	r, err = db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e = <-watches
	assert.Equal(protodb.EventTypeEnter, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	err = db.Delete(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	e = <-watches
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0, e.Old())
	assert.Nil(e.New())

	time.Sleep(time.Second)
	cancel()
	<-watches
}

func TestRegister(t *testing.T) {
	dbPath := "TestRegister"
	defer os.RemoveAll(dbPath)
	require := require2.New(t)
	assert := assert2.New(t)

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := protodb.Open(ctx, protodb.WithPath(dbPath), protodb.WithApplyDefaults(true))
	require.NoError(err)
	assert.NotNil(db)
	defer db.Close()

	require.NoError(db.Register(ctx, (&testpb.Interface{}).ProtoReflect().Descriptor().ParentFile()))
}

func TestBatchInsertAndQuery(t *testing.T) {
	dbPath := "TestBatchInsertAndQuery"
	defer os.RemoveAll(dbPath)
	require := require2.New(t)
	assert := assert2.New(t)

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := protodb.Open(ctx, protodb.WithPath(dbPath), protodb.WithApplyDefaults(true))
	require.NoError(err)
	assert.NotNil(db)
	defer db.Close()

	tx, err := db.Tx(ctx)
	require.NoError(err)
	start := time.Now()
	max := 100_000
	for i := 0; i < max; i++ {
		n := fmt.Sprintf("eth%d", i)
		i := &testpb.Interface{
			Name: n,
		}
		m, err := tx.Put(ctx, i)
		require.NoError(err)
		i, ok := m.(*testpb.Interface)
		require.True(ok)
		assert.Equal(uint32(1500), i.Mtu)
	}
	require.NoError(tx.Commit(ctx))
	t.Logf("inserted %d items in %v", max, time.Since(start))
	batch := 100
	tk := ""
	regex := `^eth\d*0$`
	for i := 0; i*batch <= max/10; i++ {
		start = time.Now()
		paging := &pb.Paging{Limit: uint64(batch), Offset: uint64(i * batch), Token: tk}
		ms, pinfo, err := db.Get(ctx, &testpb.Interface{}, paging, &filters.FieldFilter{Field: "name", Filter: filters.StringRegex(regex)})
		require.NoError(err)
		if i%10 == 0 {
			t.Logf("queried name=~\"%s\" (offset: %v, limit: %v) on %d items in %v", regex, paging.GetOffset(), paging.GetLimit(), max, time.Since(start))
		}
		require.NotNil(pinfo)
		assert.NotEmpty(pinfo.Token)
		tk = pinfo.Token
		assert.Equal(i*batch+len(ms) < max/10, pinfo.HasNext)
		if i*batch < max/10 {
			assert.Len(ms, batch)
		} else {
			assert.Len(ms, 0)
		}
	}
}
