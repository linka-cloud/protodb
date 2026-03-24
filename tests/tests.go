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

package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jhump/protoreflect/v2/protobuilder"
	"github.com/sirupsen/logrus"
	assert2 "github.com/stretchr/testify/assert"
	require2 "github.com/stretchr/testify/require"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/protodb/v1alpha1"
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

type Case struct {
	Name string
	Run  func(t *testing.T, client protodb.Client)
}

var Tests = []Case{
	{Name: "Test", Run: Test},
	{Name: "TestBatchWatch", Run: TestBatchWatch},
	{Name: "TestWatchWithFilter", Run: TestWatchWithFilter},
	{Name: "TestRegister", Run: TestRegister},
	{Name: "TestBatchInsertAndQuery", Run: TestBatchInsertAndQuery},
	{Name: "TestOversizeBatchInsert", Run: TestOversizeBatchInsert},
	{Name: "TestSeq", Run: TestSeq},
	{Name: "TestPredefinedErrors", Run: TestPredefinedErrors},
	{Name: "TestFieldMask", Run: TestFieldMask},
	{Name: "TestMessageWithKeyOption", Run: TestMessageWithKeyOption},
	{Name: "TestStaticKey", Run: TestStaticKey},
	{Name: "TestIndexCRUD", Run: TestIndexCRUD},
	{Name: "TestIndexCreation", Run: TestIndexCreation},
}

func Test(t *testing.T, db protodb.Client) {
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

	watches := make(chan protodb.Event)
	watchErr := make(chan error, 1)
	winit := make(chan struct{})
	go func() {
		ch, err := db.Watch(ctx, &testpb.Interface{})
		if err != nil {
			watchErr <- fmt.Errorf("watch interface: %w", err)
			close(winit)
			close(watches)
			return
		}
		close(winit)
		for e := range ch {
			watches <- e
		}
		close(watches)
	}()
	<-winit
	r, err := db.Set(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	equal(i0, r)
	e := recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeEnter, e.Type())
	assert.Nil(e.Old())
	equal(i0, e.New())

	is, i, err := db.Get(ctx, &testpb.Interface{})
	require.NoError(err)
	assert.NotNil(i)
	require.Len(is, 1)
	equal(i0, is[0])

	r, err = db.Set(ctx, i1)
	require.NoError(err)
	require.NotNil(r)
	equal(i1, r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeEnter, e.Type())
	assert.Nil(e.Old())
	equal(i1, e.New())

	is, i, err = db.Get(ctx, &testpb.Interface{})
	require.NoError(err)
	assert.NotNil(i)
	assert.Len(is, 2)
	equal(i0, is[0])
	equal(i1, is[1])

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusDown
	r, err = db.Set(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	equal(i0, r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeUpdate, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusUp
	r, err = db.Set(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	equal(i0, r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeUpdate, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	err = db.Delete(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0, e.Old())
	assert.Nil(e.New())

	cancel()
	select {
	case <-watches:
	case <-time.After(time.Second):
		t.Fatal("watch did not close")
	}
}

func TestBatchWatch(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	assert := assert2.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	watches := make(chan protodb.Event)
	watchErr := make(chan error, 1)
	winit := make(chan struct{})
	go func() {
		ch, err := db.Watch(ctx, &testpb.KV{})
		if err != nil {
			watchErr <- fmt.Errorf("watch kv: %w", err)
			close(winit)
			close(watches)
			return
		}
		close(winit)
		for e := range ch {
			watches <- e
		}
		t.Logf("watcher closed")
		close(watches)
	}()
	<-winit
	tx, err := db.Tx(ctx)
	require.NoError(err)
	count := 100_000
	logCount := 1000
	t.Logf("creating %d records", count)
	for i := range count {
		if i%logCount == 0 {
			t.Logf("creating: %d/%d", i, count)
		}
		_, err = tx.Set(ctx, &testpb.KV{Key: fmt.Sprintf("%d", i), Value: fmt.Sprintf("%d", i)})
		require.NoError(err)
	}
	require.NoError(tx.Commit(ctx))
	t.Logf("retrieving %d events", count)
	for i := range count {
		if i%logCount == 0 {
			t.Logf("retrieve create events: %d/%d", i, count)
		}
		e := recvEvent(t, ctx, watches, watchErr)
		assert.Equal(protodb.EventTypeEnter, e.Type())
		assert.Nil(e.Old())
		require.NotNil(e.New())
	}
	t.Logf("updating %d records", count)
	tx, err = db.Tx(ctx)
	require.NoError(err)
	for i := range count {
		if i%logCount == 0 {
			t.Logf("updating: %d/%d", i, count)
		}
		_, err = tx.Set(ctx, &testpb.KV{Key: fmt.Sprintf("%d", i), Value: fmt.Sprintf("%dx", i)})
		require.NoError(err)
	}
	require.NoError(tx.Commit(ctx))
	for i := range count {
		if i%logCount == 0 {
			t.Logf("retrieve update events: %d/%d", i, count)
		}
		e := recvEvent(t, ctx, watches, watchErr)
		assert.Equal(protodb.EventTypeUpdate, e.Type())
		assert.NotNil(e.Old())
		require.NotNil(e.New())
	}
	t.Logf("deleting %d records", count)
	tx, err = db.Tx(ctx)
	require.NoError(err)
	for i := range count {
		if i%logCount == 0 {
			t.Logf("deleting: %d/%d", i, count)
		}
		require.NoError(tx.Delete(ctx, &testpb.KV{Key: fmt.Sprintf("%d", i)}))
	}
	require.NoError(tx.Commit(ctx))
	for i := range count {
		if i%logCount == 0 {
			t.Logf("retrieve delete events: %d/%d", i, count)
		}
		e := recvEvent(t, ctx, watches, watchErr)
		assert.Equal(protodb.EventTypeLeave, e.Type())
		require.NotNil(e.Old())
		assert.Nil(e.New())
	}
}

func TestWatchWithFilter(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	assert := assert2.New(t)
	equal := func(e, g proto.Message) {
		if !assert.True(proto.Equal(e, g)) {
			assert.Equal(e, g)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	watches := make(chan protodb.Event)
	watchErr := make(chan error, 1)
	winit := make(chan struct{})
	go func() {
		ch, err := db.Watch(ctx, &testpb.Interface{},
			protodb.WithFilter(
				filters.Where(testpb.InterfaceFields.Status).NumberEquals(float64(testpb.StatusUp)),
			),
		)
		if err != nil {
			watchErr <- fmt.Errorf("watch interface with filter: %w", err)
			close(winit)
			close(watches)
			return
		}
		close(winit)
		for e := range ch {
			watches <- e
		}
		close(watches)
	}()
	<-winit
	r, err := db.Set(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	equal(i0, r)
	e := recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeEnter, e.Type())
	assert.Nil(e.Old())
	equal(i0, e.New())

	// some noise
	r, err = db.Set(ctx, i1)
	require.NoError(err)
	require.NotNil(r)
	equal(i1, r)

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusDown
	r, err = db.Set(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	equal(i0, r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	i0.Status = testpb.StatusUp
	r, err = db.Set(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	equal(i0, r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeEnter, e.Type())
	equal(i0Old, e.Old())
	equal(i0, e.New())

	i0Old = proto.Clone(i0)
	err = db.Delete(ctx, i0)
	require.NoError(err)
	require.NotNil(r)
	e = recvEvent(t, ctx, watches, watchErr)
	assert.Equal(protodb.EventTypeLeave, e.Type())
	equal(i0, e.Old())
	assert.Nil(e.New())

	cancel()
	select {
	case <-watches:
	case <-time.After(time.Second):
		t.Fatal("watch did not close")
	}
}

func recvEvent(t *testing.T, ctx context.Context, watches <-chan protodb.Event, watchErr <-chan error) protodb.Event {
	t.Helper()
	select {
	case err := <-watchErr:
		t.Fatalf("watch failed: %v", err)
	case e, ok := <-watches:
		if !ok {
			t.Fatal("watch closed before expected event")
		}
		if e == nil {
			t.Fatal("watch returned nil event")
		}
		return e
	case <-ctx.Done():
		t.Fatalf("timeout waiting watch event: %v", ctx.Err())
	}
	return nil
}

func TestRegister(t *testing.T, db protodb.Client) {
	require := require2.New(t)

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var err error

	baseFile := buildDynamicFile(t, false)
	require.NoError(db.Register(ctx, baseFile))

	baseMsg := baseFile.Messages().ByName("Dyn")
	if baseMsg == nil {
		t.Fatalf("dynamic message Dyn not found")
	}
	waitNoError(t, time.Second, func() error {
		_, _, err := db.Get(ctx, dynamicpb.NewMessage(baseMsg))
		return err
	})
	dyn := dynamicpb.NewMessage(baseMsg)
	setFieldValue(t, dyn, "name", protoreflect.ValueOfString("dyn0"))
	setFieldValue(t, dyn, "value", protoreflect.ValueOfUint32(1200))
	_, err = db.Set(ctx, dyn)
	require.NoError(err)

	query := dynamicpb.NewMessage(baseMsg)
	setFieldValue(t, query, "name", protoreflect.ValueOfString("dyn0"))
	got, ok, err := db.GetOne(ctx, query)
	require.NoError(err)
	require.True(ok)
	require.True(proto.Equal(dyn, got))

	updatedFile := buildDynamicFile(t, true)
	require.NoError(db.Register(ctx, updatedFile))

	updatedMsg := updatedFile.Messages().ByName("Dyn")
	if updatedMsg == nil {
		t.Fatalf("dynamic message Dyn not found after update")
	}
	waitNoError(t, time.Second, func() error {
		_, _, err := db.Get(ctx, dynamicpb.NewMessage(updatedMsg))
		return err
	})
	updatedQuery := dynamicpb.NewMessage(updatedMsg)
	setFieldValue(t, updatedQuery, "name", protoreflect.ValueOfString("dyn0"))
	updatedGot, ok, err := db.GetOne(ctx, updatedQuery)
	require.NoError(err)
	require.True(ok)
	updatedExpected := dynamicpb.NewMessage(updatedMsg)
	setFieldValue(t, updatedExpected, "name", protoreflect.ValueOfString("dyn0"))
	setFieldValue(t, updatedExpected, "value", protoreflect.ValueOfUint32(1200))
	require.True(proto.Equal(updatedExpected, updatedGot))

	prevFile := buildBreakingFile(t, false)
	require.NoError(db.Register(ctx, prevFile))
	prevMsg := prevFile.Messages().ByName("Break")
	if prevMsg == nil {
		t.Fatalf("dynamic message Break not found")
	}
	waitNoError(t, time.Second, func() error {
		_, _, err := db.Get(ctx, dynamicpb.NewMessage(prevMsg))
		return err
	})

	updatedFile = buildBreakingFile(t, true)
	err = db.Register(ctx, updatedFile)
	require.Error(err)
	require.Contains(err.Error(), "wire breaking changes detected")

	compatiblePrev := buildCompatibleFile(t, false)
	require.NoError(db.Register(ctx, compatiblePrev))

	compatibleNext := buildCompatibleFile(t, true)
	require.NoError(db.Register(ctx, compatibleNext))
}

func buildBreakingFile(t *testing.T, updated bool) protoreflect.FileDescriptor {
	t.Helper()
	msg := protobuilder.NewMessage("Break")
	if !updated {
		msg.AddField(protobuilder.NewField("name", protobuilder.FieldTypeString()).SetNumber(1))
	}
	file := protobuilder.NewFile("breaking.proto").
		SetPackageName("acme.v1").
		SetSyntax(protoreflect.Proto2).
		AddMessage(msg)
	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build breaking file: %v", err)
	}
	return fd
}

func buildDynamicFile(t *testing.T, updated bool) protoreflect.FileDescriptor {
	t.Helper()
	msg := protobuilder.NewMessage("Dyn").
		AddField(protobuilder.NewField("name", protobuilder.FieldTypeString()).SetNumber(1)).
		AddField(protobuilder.NewField("value", protobuilder.FieldTypeUint32()).SetNumber(2))
	if updated {
		msg.AddField(protobuilder.NewField("extra", protobuilder.FieldTypeString()).SetNumber(3))
	}
	file := protobuilder.NewFile("dynamic.proto").
		SetPackageName("acme.v1").
		SetSyntax(protoreflect.Proto2).
		AddMessage(msg)
	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build dynamic file: %v", err)
	}
	return fd
}

func buildCompatibleFile(t *testing.T, updated bool) protoreflect.FileDescriptor {
	t.Helper()
	msg := protobuilder.NewMessage("Compat")
	fieldType := protobuilder.FieldTypeInt32()
	if updated {
		fieldType = protobuilder.FieldTypeUint32()
	}
	msg.AddField(protobuilder.NewField("values", fieldType).SetNumber(1).SetRepeated())
	file := protobuilder.NewFile("compatible.proto").
		SetPackageName("acme.v1").
		SetSyntax(protoreflect.Proto2).
		AddMessage(msg)
	fd, err := file.Build()
	if err != nil {
		t.Fatalf("build compatible file: %v", err)
	}
	return fd
}

func setFieldValue(t *testing.T, msg proto.Message, name string, value protoreflect.Value) {
	t.Helper()
	fd := msg.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		t.Fatalf("field %s not found", name)
	}
	msg.ProtoReflect().Set(fd, value)
}

func TestBatchInsertAndQuery(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	assert := assert2.New(t)

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tx, err := db.Tx(ctx)
	require.NoError(err)
	start := time.Now()
	max := 100_000
	for i := range max {
		n := fmt.Sprintf("eth%d", i)
		i := &testpb.Interface{
			Name: n,
		}
		m, err := tx.Set(ctx, i, protodb.WithTTL(10*time.Minute))
		require.NoError(err)
		i, ok := m.(*testpb.Interface)
		require.True(ok)
		assert.Equal(uint32(1500), i.Mtu)
	}
	require.NoError(tx.Commit(ctx))
	t.Logf("inserted %d items in %v", max, time.Since(start))
	batch := 10
	tk := ""
	regex := `^eth\d*0$`
	for i := 0; i*batch <= max/10; i++ {
		start = time.Now()
		paging := &v1alpha1.Paging{Limit: uint64(batch), Offset: uint64(i * batch), Token: tk}
		ms, pinfo, err := db.Get(ctx, &testpb.Interface{}, protodb.WithPaging(paging), protodb.WithFilter(protodb.Where("name").StringRegex(regex)))
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

func TestOversizeBatchInsert(t *testing.T, db protodb.Client) {
	require := require2.New(t)

	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tx, err := db.Tx(ctx)
	require.NoError(err)
	defer tx.Close()
	max := 1_000_000
	for i := range max {
		n := fmt.Sprintf("eth%d", i)
		iface := &testpb.Interface{
			Name: n,
		}
		_, err := tx.Set(ctx, iface, protodb.WithTTL(10*time.Minute))
		require.NoError(err)

		if (i % (max / 100)) == 0 {
			p := int(float64(i) / float64(max) * 100)
			t.Logf("%d%%: inserted %d items", p, i)
		}
	}
	t.Logf("100%%: inserted %d items", max)
	err = tx.Commit(ctx)
	require.NoError(err)
}

func TestSeq(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	assert := assert2.New(t)
	for i := range 10 {
		seq, err := db.NextSeq(context.Background(), "test")
		require.NoError(err)
		assert.Equal(uint64(i+1), seq)
	}
}

func TestPredefinedErrors(t *testing.T, db protodb.Client) {
	require := require2.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tx, err := db.Tx(ctx, protodb.WithReadOnly())
	require.NoError(err)
	defer tx.Close()

	_, err = tx.Set(ctx, &testpb.Interface{Name: "ro"})
	require.Error(err)
	require.ErrorIs(err, protodb.ErrReadOnlyTxn)

	_, err = db.NextSeq(ctx, "")
	require.Error(err)
	require.ErrorIs(err, protodb.ErrEmptyKey)

	_, _, err = db.Get(ctx, &testpb.Interface{}, protodb.WithPaging(&protodb.Paging{Limit: 1, Token: "invalid"}))
	require.Error(err)
	require.ErrorIs(err, protodb.ErrInvalidContinuationToken)
}

func TestFieldMask(t *testing.T, db protodb.Client) {
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

	_, err := db.Set(ctx, i0)
	require.NoError(err)

	i := &testpb.Interface{
		Status: testpb.StatusDown,
		Mtu:    1400,
		Name:   "eth0",
	}

	n := proto.Clone(i0).(*testpb.Interface)
	n.Status = testpb.StatusDown
	n.Mtu = 1400

	u, err := db.Set(ctx, i, protodb.WithWriteFieldMaskPaths(testpb.InterfaceFields.Status, testpb.InterfaceFields.Mtu))
	require.NoError(err)

	equal(n, u)

	ms, _, err := db.Get(ctx, u, protodb.WithReadFieldMaskPaths(testpb.InterfaceFields.Name))
	require.NoError(err)
	require.Len(ms, 1)
	equal(ms[0], &testpb.Interface{Name: "eth0"})
}

func TestMessageWithKeyOption(t *testing.T, db protodb.Client) {
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

	m := &testpb.MessageWithKeyOption{KeyField: 42}
	m2, err := db.Set(ctx, m)
	require.NoError(err)
	equal(m, m2)

	_, err = db.Set(ctx, &testpb.MessageWithKeyOption{KeyField: 10})
	require.NoError(err)

	ms, _, err := db.Get(ctx, &testpb.MessageWithKeyOption{})
	require.NoError(err)
	assert.Len(ms, 2)

	ms, _, err = db.Get(ctx, &testpb.MessageWithKeyOption{KeyField: 42})
	require.NoError(err)
	require.Len(ms, 1)
	equal(m, ms[0])
}

func TestStaticKey(t *testing.T, db protodb.Client) {
	require := require2.New(t)
	assert := assert2.New(t)

	equal := func(e, g proto.Message) {
		if !assert.True(proto.Equal(e, g)) {
			assert.Equal(e, g)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &testpb.MessageWithStaticKey{
		Name: "static message",
	}
	_, err := db.Set(ctx, m)
	require.NoError(err)

	ms, _, err := db.Get(ctx, &testpb.MessageWithStaticKey{})
	require.NoError(err)
	require.Len(ms, 1)
	equal(m, ms[0])

	m.Name = "other"

	_, err = db.Set(ctx, m)
	require.NoError(err)

	ms, _, err = db.Get(ctx, &testpb.MessageWithStaticKey{Name: "whatever"})
	require.NoError(err)
	require.Len(ms, 1)
	equal(m, ms[0])
}

func TestReplication(t *testing.T, data string, mode protodb.ReplicationMode) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	count := 3
	path := filepath.Join(data, "TestReplication")
	defer os.RemoveAll(path)
	c := NewCluster(path, count, mode)
	if err := c.StartAll(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.StopAll()
	logrus.Infof("all nodes started")

	db := c.Get(1)
	now := time.Now()
	tx, err := db.Tx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	insert := 25_000
	logrus.Infof("inserting %d records", insert)
	for i := range insert {
		name := fmt.Sprintf("iface-%06d", i)
		if i%(insert/100) == 0 {
			logrus.Infof("inserted: %d/%d", i, insert)
		}
		if _, err := tx.Set(ctx, &testpb.Interface{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	logrus.Infof("inserted: %d/%d", insert, insert)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	logrus.Infof("took: %v", time.Since(now))
	logger.C(ctx).Info("checking that db are in sync")
	want, _, err := c.dbs[0].Get(ctx, &testpb.Interface{})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := c.dbs[1].Get(ctx, &testpb.Interface{})
	if err != nil {
		t.Fatal(err)
	}
	if len(want) != len(got) {
		t.Fatalf("got: %v, want: %v", len(got), len(want))
	}
	for i := range want {
		g, w := got[i].(*testpb.Interface), want[i].(*testpb.Interface)
		if !g.EqualVT(w) {
			t.Fatalf("got: %v, want: %v", g, w)
		}
	}
	logger.C(ctx).Info("db are in sync")
	logger.C(ctx).Info("stopping db-0")
	if err := c.Stop(0); err != nil {
		t.Fatal(err)
	}
	logger.C(ctx).Info("closed db-0")
	logger.C(ctx).Infof("getting from db-0")
	if _, _, err := c.Get(0).Get(ctx, &testpb.Interface{}, protodb.WithPaging(&protodb.Paging{Limit: 1})); err == nil {
		t.Fatal("expected error")
	}
	logger.C(ctx).Infof("getting from db-1")
	waitNoError(t, 30*time.Second, func() error {
		_, _, err := c.Get(1).Get(ctx, &testpb.Interface{}, protodb.WithPaging(&protodb.Paging{Limit: 1}))
		if errors.Is(err, protodb.ErrNoLeaderConn) {
			return err
		}
		return nil
	})
	logger.C(ctx).Infof("setting in db-1")
	if _, err := c.Get(1).Set(ctx, &testpb.Interface{Name: "test"}); err != nil {
		t.Fatal(err)
	}
	logger.C(ctx).Infof("starting db-0")
	if err := c.Start(ctx, 0); err != nil {
		t.Fatal(err)
	}
	logger.C(ctx).Infof("getting from db-0")
	got, _, err = c.Get(0).Get(ctx, &testpb.Interface{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got: %v, want: 1", len(got))
	}
}
