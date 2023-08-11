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
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.linka.cloud/grpc-toolkit/logger"

	"go.linka.cloud/protodb"
	testpb "go.linka.cloud/protodb/tests/pb"
)

func TestReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	count := 2
	path := filepath.Join(data, "TestReplication")
	defer os.RemoveAll(path)
	c := NewCluster(path, count)
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
	for i := 0; i < insert; i++ {
		name := randString(10)
		if _, err := tx.Set(ctx, &testpb.Interface{Name: name}); err != nil {
			t.Fatal(err)
		}
	}
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
	if _, _, err := c.Get(1).Get(ctx, &testpb.Interface{}, protodb.WithPaging(&protodb.Paging{Limit: 1})); err != nil {
		t.Fatal(err)
	}
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

func randString(len int) string {
	l := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	b := make([]byte, len)
	for i := range b {
		b[i] = byte(l[rand.Intn(len)])
	}
	return string(b)
}
