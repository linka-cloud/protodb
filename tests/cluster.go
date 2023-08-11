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
	"fmt"
	"io"
	"os"
	"sync"

	"go.linka.cloud/grpc-toolkit/logger"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"go.linka.cloud/protodb"
)

var offset = 0

type Cluster struct {
	path  string
	addrs []string
	ports []int
	dbs   []protodb.DB
	mu    sync.Mutex
	opts  []protodb.Option
}

func NewCluster(path string, count int, opts ...protodb.Option) *Cluster {
	var addrs []string
	var ports []int
	for i := 0; i < count; i++ {
		offset++
		ports = append(ports, 18800+offset)
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", ports[i]))
	}
	return &Cluster{path: path, addrs: addrs, ports: ports, dbs: make([]protodb.DB, count), opts: opts}
}

func (c *Cluster) StartAll(ctx context.Context) error {
	g := errgroup.Group{}
	for i := range c.dbs {
		i := i
		g.Go(func() error {
			return c.Start(ctx, i)
		})
	}
	return g.Wait()
}

func (c *Cluster) Start(ctx context.Context, i int) error {
	if i < 0 || i >= len(c.dbs) {
		return fmt.Errorf("index out of range")
	}
	p := fmt.Sprintf("%s/protodb-repl-%d", c.path, i)
	if err := os.MkdirAll(p, os.ModePerm); err != nil {
		return err
	}
	if i == 2 {
		log := logger.C(ctx)
		log = log.Clone()
		log.SetOutput(io.Discard)
		ctx = logger.Set(ctx, log)
	}
	opts := append(
		c.opts,
		protodb.WithLogger(logger.C(ctx).WithField("name", fmt.Sprintf("db-%d", i))),
		protodb.WithPath(p),
		// protodb.WithInMemory(true),
		protodb.WithReplication(
			protodb.WithMode(protodb.ReplicationModeSync),
			protodb.WithName(fmt.Sprintf("db-%d", i)),
			protodb.WithAddrs(c.addrs...),
			protodb.WithGossipPort(c.ports[i]),
			protodb.WithGRPCPort(c.ports[i]+10000),
		),
	)
	db, err := protodb.Open(ctx, opts...)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.dbs[i] = db
	c.mu.Unlock()
	return nil
}

func (c *Cluster) Get(i int) protodb.DB {
	if i < 0 || i >= len(c.dbs) {
		panic("index out of range")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dbs[i]
}

func (c *Cluster) Stop(i int) error {
	if i < 0 || i >= len(c.dbs) {
		return nil
	}
	return c.dbs[i].Close()
}

func (c *Cluster) StopAll() error {
	var err error
	g := errgroup.Group{}
	for i := range c.dbs {
		if db := c.Get(i); db != nil {
			g.Go(func() error {
				err = multierr.Append(err, c.Stop(i))
				return nil
			})
		}
	}
	g.Wait()
	return err
}
