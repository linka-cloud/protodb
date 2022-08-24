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

package generics

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"go.linka.cloud/protodb"
	testpb "go.linka.cloud/protodb/tests/pb"
	"go.linka.cloud/protodb/typed"
)

func Test(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := protodb.Open(ctx, protodb.WithPath(":memory:"))
	if err != nil {
		logrus.Fatal(err)
	}
	s := typed.NewStore[testpb.Interface](db)
	require.NoError(t, s.Register(ctx))
	if _, err := s.Set(ctx, &testpb.Interface{Name: "eth0"}); err != nil {
		logrus.Fatal(err)
	}
	is, _, err := s.Get(ctx, &testpb.Interface{})
	if err != nil {
		logrus.Fatal(err)
	}
	for _, v := range is {
		logrus.Infof("%+v", v)
	}
}
