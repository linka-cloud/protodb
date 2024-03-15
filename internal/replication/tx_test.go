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

package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"go.linka.cloud/protodb/internal/replication/pb"
)

var _ pb.ReplicationService_ReplicateClient = (*mockStream)(nil)

type mockStream struct {
	fail bool
	o    sync.Once
}

func (m *mockStream) Send(op *pb.Op) error {
	if m.fail {
		return fmt.Errorf("failed")
	}
	return nil
}

func (m *mockStream) Recv() (*pb.Ack, error) {
	if m.fail {
		return nil, fmt.Errorf("failed")
	}
	return &pb.Ack{}, nil
}

func (m *mockStream) Header() (metadata.MD, error) {
	// TODO implement me
	panic("implement me")
}

func (m *mockStream) Trailer() metadata.MD {
	// TODO implement me
	panic("implement me")
}

func (m *mockStream) CloseSend() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockStream) Context() context.Context {
	// TODO implement me
	panic("implement me")
}

func (m *mockStream) SendMsg(v any) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockStream) RecvMsg(v any) error {
	// TODO implement me
	panic("implement me")
}

func TestTxAsync(t *testing.T) {
	tx := &Tx{
		mode: ModeAsync,
		cs:   []*stream{{c: &mockStream{fail: true}}},
	}
	require.NoError(t, tx.New(context.Background(), 0))
	require.NoError(t, tx.Set(context.Background(), []byte("key"), []byte("value-%d"), 0))

	s := &mockStream{}
	tx = &Tx{
		mode: ModeSync,
		cs:   []*stream{{c: s}},
	}
	require.NoError(t, tx.New(context.Background(), 0))
	s.fail = true
	require.Error(t, tx.Set(context.Background(), []byte("key"), []byte("value-%d"), 0))
}
