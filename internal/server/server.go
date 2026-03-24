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

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	gerrs "go.linka.cloud/grpc-toolkit/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"go.linka.cloud/protodb/internal/anypb"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/rpcerrs"
	"go.linka.cloud/protodb/protodb/v1alpha1"
)

type Server interface {
	v1alpha1.ProtoDBServer
	RegisterService(r grpc.ServiceRegistrar)
}

func NewServer(db protodb.DB) (Server, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	return &server{db: db}, nil
}

type server struct {
	db protodb.DB
	v1alpha1.UnsafeProtoDBServer
}

func (s *server) Lock(ss grpc.BidiStreamingServer[v1alpha1.LockRequest, v1alpha1.LockResponse]) error {
	p, ok, err := s.maybeProxy(false)
	if err != nil {
		return err
	}
	if ok {
		return p.Lock(ss)
	}
	req, err := ss.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return gerrs.Canceled(err)
		}
		return gerrs.Internalf("failed to receive lock request: %v", err)
	}
	if err := s.db.Lock(ss.Context(), req.Key); err != nil {
		if errors.Is(err, context.Canceled) {
			return gerrs.Canceled(err)
		}
		return rpcerrs.ToStatus(err)
	}
	defer s.db.Unlock(ss.Context(), req.Key)
	if err := ss.Send(&v1alpha1.LockResponse{}); err != nil {
		return gerrs.Internalf("failed to send lock response: %v", err)
	}
	if _, err := ss.Recv(); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

func (s *server) RegisterService(r grpc.ServiceRegistrar) {
	v1alpha1.RegisterProtoDBServer(r, s)
}

func (s *server) Register(ctx context.Context, req *v1alpha1.RegisterRequest) (*v1alpha1.RegisterResponse, error) {
	p, ok, err := s.maybeProxy(false)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.Register(ctx, req)
	}
	if err := s.db.RegisterProto(ctx, req.File); err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.RegisterResponse{}, nil
}

func (s *server) Descriptors(ctx context.Context, req *v1alpha1.DescriptorsRequest) (*v1alpha1.DescriptorsResponse, error) {
	p, ok, err := s.maybeProxy(true)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.Descriptors(ctx, req)
	}
	des, err := s.db.Descriptors(ctx)
	if err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.DescriptorsResponse{Results: des}, nil
}

func (s *server) FileDescriptors(ctx context.Context, req *v1alpha1.FileDescriptorsRequest) (*v1alpha1.FileDescriptorsResponse, error) {
	p, ok, err := s.maybeProxy(true)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.FileDescriptors(ctx, req)
	}
	des, err := s.db.FileDescriptors(ctx)
	if err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.FileDescriptorsResponse{Results: des}, nil
}

func (s *server) Get(ctx context.Context, req *v1alpha1.GetRequest) (*v1alpha1.GetResponse, error) {
	p, ok, err := s.maybeProxy(true)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.Get(ctx, req)
	}
	a, i, err := s.get(ctx, s.db, req)
	if err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.GetResponse{Results: a, Paging: i}, nil
}

func (s *server) Set(ctx context.Context, req *v1alpha1.SetRequest) (*v1alpha1.SetResponse, error) {
	p, ok, err := s.maybeProxy(false)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.Set(ctx, req)
	}
	a, err := s.set(ctx, s.db, req)
	if err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.SetResponse{Result: a}, nil
}

func (s *server) Delete(ctx context.Context, req *v1alpha1.DeleteRequest) (*v1alpha1.DeleteResponse, error) {
	p, ok, err := s.maybeProxy(false)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.Delete(ctx, req)
	}
	if err := s.delete(ctx, s.db, req); err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.DeleteResponse{}, nil
}

func (s *server) NextSeq(ctx context.Context, req *v1alpha1.NextSeqRequest) (*v1alpha1.NextSeqResponse, error) {
	p, ok, err := s.maybeProxy(false)
	if err != nil {
		return nil, err
	}
	if ok {
		return p.NextSeq(ctx, req)
	}
	seq, err := s.db.NextSeq(ctx, req.Key)
	if err != nil {
		return nil, rpcerrs.ToStatus(err)
	}
	return &v1alpha1.NextSeqResponse{Seq: seq}, nil
}

func (s *server) Tx(stream v1alpha1.ProtoDB_TxServer) error {
	ctx := stream.Context()
	readOnly := false
	var opts []protodb.TxOption
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if readOnly = len(md.Get(v1alpha1.ReadOnlyTxKey)) > 0; readOnly {
			opts = append(opts, protodb.WithReadOnly())
		}
	}
	p, ok, err := s.maybeProxy(readOnly)
	if err != nil {
		return err
	}
	if ok {
		return p.Tx(stream)
	}
	// send dummy headers for clients that cannot create streams before receiving first from the server
	if err := grpc.SendHeader(ctx, metadata.Pairs(v1alpha1.ReadOnlyTxKey, strconv.FormatBool(readOnly))); err != nil {
		return gerrs.Internalf("failed to send headers: %v", err)
	}
	tx, err := s.db.Tx(stream.Context(), opts...)
	if err != nil {
		return rpcerrs.ToStatus(err)
	}
	defer tx.Close()
	for {
		req, err := stream.Recv()
		if err != nil && ctx.Err() == nil {
			return rpcerrs.ToStatus(err)
		}
		if err := ctx.Err(); err != nil {
			return rpcerrs.ToStatus(err)
		}
		var res *v1alpha1.TxResponse
		switch r := req.Request.(type) {
		case *v1alpha1.TxRequest_Get:
			as, i, err := s.get(ctx, tx, r.Get)
			if err != nil {
				return rpcerrs.ToStatus(err)
			}
			res = &v1alpha1.TxResponse{Response: &v1alpha1.TxResponse_Get{Get: &v1alpha1.GetResponse{
				Results: as,
				Paging:  i,
			}}}
		case *v1alpha1.TxRequest_Set:
			a, err := s.set(ctx, tx, r.Set)
			if err != nil {
				return rpcerrs.ToStatus(err)
			}
			res = &v1alpha1.TxResponse{Response: &v1alpha1.TxResponse_Set{Set: &v1alpha1.SetResponse{
				Result: a,
			}}}
		case *v1alpha1.TxRequest_Delete:
			if err := s.delete(ctx, tx, r.Delete); err != nil {
				return rpcerrs.ToStatus(err)
			}
			res = &v1alpha1.TxResponse{Response: &v1alpha1.TxResponse_Delete{Delete: &v1alpha1.DeleteResponse{}}}
		case *v1alpha1.TxRequest_Commit:
			if !r.Commit.GetValue() {
				continue
			}
			err := tx.Commit(ctx)
			cr := &v1alpha1.CommitResponse{}
			if err != nil {
				cr.Error = wrapperspb.String(err.Error())
				return rpcerrs.ToStatus(err)
			}
			res = &v1alpha1.TxResponse{Response: &v1alpha1.TxResponse_Commit{Commit: cr}}
		}
		if err := stream.Send(res); err != nil {
			return rpcerrs.ToStatus(err)
		}
	}
}

func (s *server) Watch(req *v1alpha1.WatchRequest, stream v1alpha1.ProtoDB_WatchServer) error {
	p, ok, err := s.maybeProxy(true)
	if err != nil {
		return err
	}
	if ok {
		return p.Watch(req, stream)
	}
	d, err := s.unmarshal(req.Search)
	if err != nil {
		return err
	}
	ch, err := s.db.Watch(stream.Context(), d, watchOpts(req)...)
	if err != nil {
		return rpcerrs.ToStatus(err)
	}
	// we send an empty event for clients that cannot create streams before receiving first from the server
	if err := stream.Send(&v1alpha1.WatchEvent{}); err != nil {
		return err
	}
	for e := range ch {
		if err := e.Err(); err != nil {
			return rpcerrs.ToStatus(err)
		}
		we := &v1alpha1.WatchEvent{Type: e.Type()}
		if n := e.New(); n != nil {
			a, err := anypb.New(n)
			if err != nil {
				return err
			}
			we.New = a
		}
		if o := e.Old(); o != nil {
			a, err := anypb.New(o)
			if err != nil {
				return err
			}
			we.Old = a
		}
		if err := stream.Send(we); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) get(ctx context.Context, r protodb.Reader, get *v1alpha1.GetRequest) ([]*anypb.Any, *v1alpha1.PagingInfo, error) {
	d, err := s.unmarshal(get.Search)
	if err != nil {
		return nil, nil, err
	}
	res, i, err := r.Get(ctx, d, getOpts(get)...)
	if err != nil {
		return nil, nil, err
	}
	as, err := toAnySlice(res...)
	if err != nil {
		return nil, nil, err
	}
	return as, i, nil
}

func (s *server) set(ctx context.Context, w protodb.Writer, set *v1alpha1.SetRequest) (*anypb.Any, error) {
	d, err := s.unmarshal(set.Payload)
	if err != nil {
		return nil, err
	}
	m, err := w.Set(ctx, d, setOpts(set)...)
	if err != nil {
		return nil, err
	}
	a, err := anypb.New(m)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (s *server) delete(ctx context.Context, w protodb.Writer, del *v1alpha1.DeleteRequest) error {
	d, err := s.unmarshal(del.Payload)
	if err != nil {
		return err
	}
	return w.Delete(ctx, d)
}

func (s *server) maybeProxy(read bool) (v1alpha1.ProtoDBServer, bool, error) {
	p, ok := s.db.(protodb.LeaderProxy)
	if !ok {
		return nil, false, nil
	}
	c, ok, err := p.MaybeProxy(read)
	if err != nil {
		return nil, false, rpcerrs.ToStatus(err)
	}
	if !ok {
		return nil, false, nil
	}
	return v1alpha1.NewProtoDBProxy(v1alpha1.NewProtoDBClient(c)), true, nil
}

func toAnySlice(m ...proto.Message) ([]*anypb.Any, error) {
	out := make([]*anypb.Any, 0, len(m))
	for _, v := range m {
		a, err := anypb.New(v)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func (s *server) unmarshal(a *anypb.Any) (proto.Message, error) {
	if m, err := anypb.UnmarshalNew(a); err == nil {
		return m, nil
	}
	desc, err := s.db.Resolver().FindDescriptorByName(a.MessageName())
	if err != nil {
		return nil, err
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unexpected descriptor type: %T", md)
	}
	d := dynamicpb.NewMessage(md)
	if err := anypb.UnmarshalTo(a, d); err != nil {
		return nil, err
	}
	return d, nil
}

func getOpts(r *v1alpha1.GetRequest) (opts []protodb.GetOption) {
	if r.One {
		opts = append(opts, protodb.WithOne())
	}
	if r.Reverse {
		opts = append(opts, protodb.WithReverse())
	}
	if r.GetOrderBy() != nil {
		opts = append(opts, protodb.WithOrderBy(r.GetOrderBy().GetField(), r.GetOrderBy().GetDirection()))
	}
	return append(opts, protodb.WithFilter(r.Filter), protodb.WithPaging(r.Paging), protodb.WithReadFieldMask(r.FieldMask))
}

func setOpts(r *v1alpha1.SetRequest) (opts []protodb.SetOption) {
	return append(opts, protodb.WithTTL(r.TTL.AsDuration()), protodb.WithWriteFieldMask(r.FieldMask))
}

func watchOpts(r *v1alpha1.WatchRequest) (opts []protodb.GetOption) {
	return append(opts, protodb.WithFilter(r.Filter))
}
