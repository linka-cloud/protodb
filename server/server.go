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

package server

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/pb"
)

func New(db protodb.DB) (pb.ProtoDBServer, error) {
	if db == nil {
		return nil, errors.New("db cannot be nil")
	}
	return &server{db: db}, nil
}

type server struct {
	db protodb.DB
	pb.UnimplementedProtoDBServer
}

func (s *server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if err := s.db.RegisterProto(ctx, req.File); err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	a, i, err := s.get(ctx, s.db, req)
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Results: a, Paging: i}, nil
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	a, err := s.put(ctx, s.db, req)
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Result: a}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.delete(ctx, s.db, req); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func (s *server) Tx(stream pb.ProtoDB_TxServer) error {
	ctx := stream.Context()
	tx, err := s.db.Tx(stream.Context())
	if err != nil {
		return err
	}
	defer tx.Close()
	for {
		req, err := stream.Recv()
		if err != nil && ctx.Err() == nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var res *pb.TxResponse
		switch r := req.Request.(type) {
		case *pb.TxRequest_Get:
			as, i, err := s.get(ctx, tx, r.Get)
			if err != nil {
				return err
			}
			res = &pb.TxResponse{Response: &pb.TxResponse_Get{Get: &pb.GetResponse{
				Results: as,
				Paging:  i,
			}}}
		case *pb.TxRequest_Put:
			a, err := s.put(ctx, tx, r.Put)
			if err != nil {
				return err
			}
			res = &pb.TxResponse{Response: &pb.TxResponse_Put{Put: &pb.PutResponse{
				Result: a,
			}}}
		case *pb.TxRequest_Delete:
			if err := s.delete(ctx, tx, r.Delete); err != nil {
				return err
			}
			res = &pb.TxResponse{Response: &pb.TxResponse_Delete{Delete: &pb.DeleteResponse{}}}
		case *pb.TxRequest_Commit:
			if !r.Commit.GetValue() {
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *server) Watch(req *pb.WatchRequest, stream pb.ProtoDB_WatchServer) error {
	d, err := s.unmarshalToDynamic(req.Search)
	if err != nil {
		return err
	}
	ch, err := s.db.Watch(stream.Context(), d, watchOpts(req)...)
	if err != nil {
		return err
	}
	for e := range ch {
		if err := e.Err(); err != nil {
			return err
		}
		we := &pb.WatchEvent{Type: e.Type()}
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

func (s *server) get(ctx context.Context, r protodb.Reader, get *pb.GetRequest) ([]*anypb.Any, *pb.PagingInfo, error) {
	d, err := s.unmarshalToDynamic(get.Search)
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

func (s *server) put(ctx context.Context, w protodb.Writer, put *pb.PutRequest) (*anypb.Any, error) {
	d, err := s.unmarshalToDynamic(put.Payload)
	if err != nil {
		return nil, err
	}
	m, err := w.Put(ctx, d, protodb.WithTTL(put.GetTTL().AsDuration()))
	if err != nil {
		return nil, err
	}
	a, err := anypb.New(m)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (s *server) delete(ctx context.Context, w protodb.Writer, del *pb.DeleteRequest) error {
	d, err := s.unmarshalToDynamic(del.Payload)
	if err != nil {
		return err
	}
	return w.Delete(ctx, d)
}

func toAnySlice(m ...proto.Message) (out []*anypb.Any, err error) {
	for _, v := range m {
		a, err := anypb.New(v)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return
}

func (s *server) unmarshalToDynamic(a *anypb.Any) (*dynamicpb.Message, error) {
	desc, err := s.db.Resolver().FindDescriptorByName(a.MessageName())
	if err != nil {
		return nil, err
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unexpected descriptor type: %T", md)
	}
	d := dynamicpb.NewMessage(md)
	if err := anypb.UnmarshalTo(a, d, proto.UnmarshalOptions{}); err != nil {
		return nil, err
	}
	return d, nil
}

func getOpts(r *pb.GetRequest) (opts []protodb.QueryOption) {
	return append(opts, protodb.WithFilters(r.Filters...), protodb.WithPaging(r.Paging))
}

func watchOpts(r *pb.WatchRequest) (opts []protodb.QueryOption) {
	return append(opts, protodb.WithFilters(r.Filters...))
}
