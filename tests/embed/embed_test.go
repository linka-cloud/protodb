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
	"testing"
	"time"

	assert2 "github.com/stretchr/testify/assert"
	require2 "github.com/stretchr/testify/require"
	"go.linka.cloud/protofilters"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/tests/pb"
)

var (
	i0 = &pb.Interface{
		Name: "eth0",
		Addresses: []*pb.IPAddress{{
			Address: &pb.IPAddress_IPV4{IPV4: "10.0.0.1/24"},
		}},
		Status: pb.StatusUp,
	}

	i0Old proto.Message

	i1 = &pb.Interface{
		Name: "eth1",
		Addresses: []*pb.IPAddress{{
			Address: &pb.IPAddress_IPV4{IPV4: "10.0.1.1/24"},
		}},
		Status: pb.StatusDown,
		Mtu:    9000,
	}
)

func init() {
	i0.Default()
	i1.Default()
}

func TestEmbed(t *testing.T) {
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
	db, err := protodb.Open(ctx, protodb.WithPath(":memory:"), protodb.WithApplyDefaults(true))
	require.NoError(err)
	assert.NotNil(db)
	defer db.Close()

	watches := make(chan protodb.Event)
	go func() {
		ch, err := db.Watch(ctx, &pb.Interface{})
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

	is, i, err := db.Get(ctx, &pb.Interface{}, nil)
	require.NoError(err)
	assert.Nil(i)
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

	is, i, err = db.Get(ctx, &pb.Interface{}, nil)
	require.NoError(err)
	assert.Nil(i)
	assert.Len(is, 2)
	equal(i0, is[0])
	equal(i1, is[1])

	i0Old = proto.Clone(i0)
	i0.Status = pb.StatusDown
	r, err = db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e = <-watches
	assert.Equal(protodb.EventTypeUpdate, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	i0.Status = pb.StatusUp
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
	db, err := protodb.Open(ctx, protodb.WithPath(":memory:"), protodb.WithApplyDefaults(true))
	require.NoError(err)
	assert.NotNil(db)
	defer db.Close()

	watches := make(chan protodb.Event)
	go func() {
		ch, err := db.Watch(ctx, &pb.Interface{}, &protofilters.FieldFilter{
			Field:  pb.InterfaceFields.Status,
			Filter: protofilters.NumberEquals(float64(pb.StatusUp)),
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
	i0.Status = pb.StatusDown
	r, err = db.Put(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	assert.Equal(i0, r)
	e = <-watches
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	i0.Status = pb.StatusUp
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