// Copyright 2023 Linka Cloud  All rights reserved.
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

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	gerrs "go.linka.cloud/grpc-toolkit/errors"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/protofilters/filters"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"go.linka.cloud/protodb/internal/anypb"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/protodb/v1alpha1"
)

type Client interface {
	protodb.Registerer
	protodb.Reader
	protodb.Writer
	protodb.Watcher
	protodb.TxProvider
	protodb.SeqProvider
	protodb.Locker
	io.Closer
}

func NewClient(cc grpc.ClientConnInterface) (Client, error) {
	return &client{c: v1alpha1.NewProtoDBClient(cc)}, nil
}

type client struct {
	c     v1alpha1.ProtoDBClient
	locks map[string]grpc.ServerStreamingClient[v1alpha1.LockResponse]
	mu    sync.Mutex
}

func (c *client) RegisterProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) error {
	_, err := c.c.Register(ctx, &v1alpha1.RegisterRequest{File: file})
	return err
}

func (c *client) Register(ctx context.Context, file protoreflect.FileDescriptor) error {
	_, err := c.c.Register(ctx, &v1alpha1.RegisterRequest{File: protodesc.ToFileDescriptorProto(file)})
	return err
}

func (c *client) Descriptors(ctx context.Context) ([]*descriptorpb.DescriptorProto, error) {
	res, err := c.c.Descriptors(ctx, &v1alpha1.DescriptorsRequest{})
	if err != nil {
		return nil, err
	}
	return res.Results, nil
}

func (c *client) FileDescriptors(ctx context.Context) ([]*descriptorpb.FileDescriptorProto, error) {
	res, err := c.c.FileDescriptors(ctx, &v1alpha1.FileDescriptorsRequest{})
	if err != nil {
		return nil, err
	}
	return res.Results, nil
}

func (c *client) Get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) ([]proto.Message, *protodb.PagingInfo, error) {
	a, err := anypb.New(m)
	if err != nil {
		return nil, nil, err
	}
	o := getOps(opts...)
	var f *filters.Expression
	if o.Filter != nil {
		f = o.Filter.Expr()
	}
	res, err := c.c.Get(ctx, &v1alpha1.GetRequest{Search: a, Filter: f, Paging: o.Paging, FieldMask: o.FieldMask, Reverse: o.Reverse, One: o.One, OrderBy: o.OrderBy})
	if err != nil {
		return nil, nil, err
	}
	msgs := make([]proto.Message, 0, len(res.Results))
	for _, v := range res.Results {
		msg := m.ProtoReflect().New().Interface()
		if err := anypb.UnmarshalTo(v, msg); err != nil {
			return nil, nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, res.Paging, nil
}

func (c *client) GetOne(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (proto.Message, bool, error) {
	return protodb.GetOne(ctx, c, m, opts...)
}

func (c *client) Set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	a, err := anypb.New(m)
	if err != nil {
		return nil, err
	}
	o := setOps(opts...)
	var ttl *durationpb.Duration
	if o.TTL != 0 {
		ttl = durationpb.New(o.TTL)
	}
	res, err := c.c.Set(ctx, &v1alpha1.SetRequest{Payload: a, TTL: ttl, FieldMask: o.FieldMask})
	if err != nil {
		return nil, err
	}
	msg := m.ProtoReflect().New().Interface()
	if err := anypb.UnmarshalTo(res.Result, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *client) Delete(ctx context.Context, m proto.Message) error {
	a, err := anypb.New(m)
	if err != nil {
		return err
	}
	_, err = c.c.Delete(ctx, &v1alpha1.DeleteRequest{Payload: a})
	return err
}

func (c *client) NextSeq(ctx context.Context, name string) (uint64, error) {
	res, err := c.c.NextSeq(ctx, &v1alpha1.NextSeqRequest{Key: name})
	if err != nil {
		return 0, err
	}
	return res.Seq, nil
}

func (c *client) Lock(ctx context.Context, key string) error {
	s, err := c.c.Lock(ctx)
	if err != nil {
		return err
	}
	if err := s.Send(&v1alpha1.LockRequest{Key: key}); err != nil {
		return err
	}
	if _, err := s.Recv(); err != nil {
		if gerrs.IsContextCanceled(err) {
			return context.Canceled
		}
		return err
	}
	c.mu.Lock()
	c.locks[key] = s
	c.mu.Unlock()
	return nil
}

func (c *client) Unlock(_ context.Context, key string) error {
	c.mu.Lock()
	s, ok := c.locks[key]
	if ok {
		delete(c.locks, key)
	}
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("unlock %s: not locked", key)
	}
	if err := s.CloseSend(); err != nil {
		return fmt.Errorf("unlock %s: %w", key, err)
	}
	return nil
}

func (c *client) Watch(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (<-chan protodb.Event, error) {
	a, err := anypb.New(m)
	if err != nil {
		return nil, err
	}
	o := getOps(opts...)
	var f *filters.Expression
	if o.Filter != nil {
		f = o.Filter.Expr()
	}
	w, err := c.c.Watch(ctx, &v1alpha1.WatchRequest{Search: a, Filter: f})
	if err != nil {
		return nil, err
	}
	ch := make(chan protodb.Event)
	go func() {
		defer close(ch)
		defer w.CloseSend()
		for {
			e, err := w.Recv()
			if err != nil {
				logger.C(ctx).Errorf("watch %s: %v", m.ProtoReflect().Descriptor().FullName(), err)
				return
			}
			// we expect an empty event for clients that cannot create streams before receiving first from the server,
			// but we can ignore unknown events anyway
			if e.Type == protodb.EventTypeUnknown {
				continue
			}
			ch <- newEvent(e, err)
		}
	}()
	return ch, nil
}

func (c *client) Tx(ctx context.Context, opts ...protodb.TxOption) (protodb.Tx, error) {
	return c.newTx(ctx, opts...)
}

func (c *client) Close() error {
	return nil
}

func (c *client) newTx(ctx context.Context, opts ...protodb.TxOption) (protodb.Tx, error) {
	var o protodb.TxOpts
	for _, opt := range opts {
		opt(&o)
	}
	if o.ReadOnly {
		ctx = metadata.AppendToOutgoingContext(ctx, v1alpha1.ReadOnlyTxKey, "true")
	}
	txn, err := c.c.Tx(ctx)
	if err != nil {
		return nil, err
	}
	return &txc{ctx: ctx, txn: txn}, nil
}

type txc struct {
	ctx context.Context
	txn v1alpha1.ProtoDB_TxClient
}

func (t *txc) Get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) ([]proto.Message, *protodb.PagingInfo, error) {
	a, err := anypb.New(m)
	if err != nil {
		return nil, nil, err
	}
	o := getOps(opts...)
	var f *filters.Expression
	if o.Filter != nil {
		f = o.Filter.Expr()
	}
	if err := t.txn.Send(&v1alpha1.TxRequest{
		Request: &v1alpha1.TxRequest_Get{
			Get: &v1alpha1.GetRequest{Search: a, Filter: f, Paging: o.Paging, FieldMask: o.FieldMask, Reverse: o.Reverse, One: o.One, OrderBy: o.OrderBy},
		},
	}); err != nil {
		return nil, nil, err
	}
	res, err := t.txn.Recv()
	if err != nil {
		return nil, nil, err
	}
	if res.GetGet() == nil {
		return nil, nil, fmt.Errorf("no response")
	}
	msgs := make([]proto.Message, 0, len(res.GetGet().GetResults()))
	for _, v := range res.GetGet().GetResults() {
		msg := m.ProtoReflect().New().Interface()
		if err := anypb.UnmarshalTo(v, msg); err != nil {
			return nil, nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, res.GetGet().GetPaging(), nil
}

func (t *txc) GetOne(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (proto.Message, bool, error) {
	return protodb.GetOne(ctx, t, m, opts...)
}

func (t *txc) Set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	a, err := anypb.New(m)
	if err != nil {
		return nil, err
	}
	o := setOps(opts...)
	var ttl *durationpb.Duration
	if o.TTL != 0 {
		ttl = durationpb.New(o.TTL)
	}
	if err := t.txn.Send(&v1alpha1.TxRequest{
		Request: &v1alpha1.TxRequest_Set{
			Set: &v1alpha1.SetRequest{Payload: a, TTL: ttl, FieldMask: o.FieldMask},
		},
	}); err != nil {
		return nil, err
	}
	res, err := t.txn.Recv()
	if err != nil {
		return nil, err
	}
	if res.GetSet() == nil || res.GetSet().GetResult() == nil {
		return nil, fmt.Errorf("no response")
	}
	msg := m.ProtoReflect().New().Interface()
	if err := anypb.UnmarshalTo(res.GetSet().GetResult(), msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (t *txc) Delete(ctx context.Context, m proto.Message) error {
	a, err := anypb.New(m)
	if err != nil {
		return err
	}
	if err := t.txn.Send(&v1alpha1.TxRequest{
		Request: &v1alpha1.TxRequest_Delete{
			Delete: &v1alpha1.DeleteRequest{Payload: a},
		},
	}); err != nil {
		return err
	}
	res, err := t.txn.Recv()
	if err != nil {
		return err
	}
	if res.GetDelete() == nil {
		return fmt.Errorf("no response")
	}
	return nil
}

func (t *txc) Commit(ctx context.Context) error {
	if err := t.txn.Send(&v1alpha1.TxRequest{Request: &v1alpha1.TxRequest_Commit{Commit: wrapperspb.Bool(true)}}); err != nil {
		return err
	}
	res, err := t.txn.Recv()
	if err != nil {
		return err
	}
	if res.GetCommit() == nil {
		return fmt.Errorf("no response")
	}
	if res.GetCommit().GetError() != nil {
		return errors.New(res.GetCommit().GetError().GetValue())
	}
	return nil
}

func (t *txc) Close() {
	if err := t.txn.CloseSend(); err != nil {
		logger.From(t.ctx).Errorf("close: %w", err)
	}
}

type eventc struct {
	typ protodb.EventType
	old proto.Message
	new proto.Message
	err error
}

func newEvent(e *v1alpha1.WatchEvent, err error) *eventc {
	ev := &eventc{typ: e.Type, err: err}
	if e.Old != nil {
		m, err := anypb.UnmarshalNew(e.Old)
		if err != nil {
			ev.err = multierr.Append(ev.err, fmt.Errorf("unmarshal old: %w", err))
		}
		ev.old = m
	}
	if e.New != nil {
		m, err := anypb.UnmarshalNew(e.New)
		if err != nil {
			ev.err = multierr.Append(ev.err, fmt.Errorf("unmarshal new: %w", err))
		}
		ev.new = m
	}
	return ev
}

func (e *eventc) Type() protodb.EventType {
	return e.typ
}

func (e *eventc) Old() proto.Message {
	return e.old
}

func (e *eventc) New() proto.Message {
	return e.new
}

func (e *eventc) Err() error {
	return e.err
}

func getOps(opts ...protodb.GetOption) *protodb.GetOpts {
	o := &protodb.GetOpts{}
	for _, v := range opts {
		v(o)
	}
	return o
}
func setOps(opts ...protodb.SetOption) *protodb.SetOpts {
	o := &protodb.SetOpts{}
	for _, v := range opts {
		v(o)
	}
	return o
}
