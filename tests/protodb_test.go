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

package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.linka.cloud/protodb"
	_ "go.linka.cloud/protodb/tests/pb"
)

const data = "testdata"

func TestServerReplicated(t *testing.T) {
	for _, v := range Tests {
		t.Run(v.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			path := filepath.Join(data, v.Name)
			defer os.RemoveAll(path)
			c := NewCluster(path, 3, protodb.WithApplyDefaults(true))
			require.NoError(t, c.StartAll(ctx))
			defer func() {
				require.NoError(t, c.StopAll())
			}()

			srv, err := protodb.NewServer(c.Get(1))
			require.NoError(t, err)

			tr := &inprocgrpc.Channel{}
			srv.RegisterService(tr)

			db, err := protodb.NewClient(tr)
			require.NoError(t, err)
			v.Run(t, db)
		})
	}
}

func TestServer(t *testing.T) {
	for _, v := range Tests {
		t.Run(v.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			path := filepath.Join(data, v.Name)
			defer os.RemoveAll(path)
			sdb, err := protodb.Open(ctx, protodb.WithPath(path), protodb.WithApplyDefaults(true))
			require.NoError(t, err)
			assert.NotNil(t, sdb)
			defer sdb.Close()

			srv, err := protodb.NewServer(sdb)
			require.NoError(t, err)

			tr := &inprocgrpc.Channel{}
			srv.RegisterService(tr)

			db, err := protodb.NewClient(tr)
			require.NoError(t, err)
			v.Run(t, db)
		})
	}
}

func TestEmbed(t *testing.T) {
	for _, v := range Tests {
		t.Run(v.Name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			path := filepath.Join(data, v.Name)
			defer os.RemoveAll(path)

			db, err := protodb.Open(ctx, protodb.WithPath(path), protodb.WithApplyDefaults(true))
			require.NoError(t, err)
			assert.NotNil(t, db)
			defer db.Close()
			v.Run(t, db)
		})
	}
}
