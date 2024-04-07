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

package db

import (
	"strings"

	"github.com/dgraph-io/badger/v3"

	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/protodb"
)

var (
	DefaultPath = "./data"
)

type (
	Logger = protodb.Logger
)

type Option func(o *options)

func WithPath(path string) Option {
	return func(o *options) {
		switch strings.TrimSpace(path) {
		case ":memory:":
			o.inMemory = true
			o.path = ""
		default:
			o.path = path
		}
	}
}

func WithInMemory(b bool) Option {
	return func(o *options) {
		if b {
			o.path = ""
		}
		o.inMemory = b
	}
}

func WithBadgerOptionsFunc(fn func(opts badger.Options) badger.Options) Option {
	return func(o *options) {
		o.badgerOptionsFunc = fn
	}
}

func WithLogger(l Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

func WithApplyDefaults(b bool) Option {
	return func(o *options) {
		o.applyDefaults = b
	}
}

func WithIgnoreProtoRegisterErrors() Option {
	return func(o *options) {
		o.ignoreProtoRegisterErrors = true
	}
}

func WithProtoRegisterErrHandler(fn func(err error) error) Option {
	return func(o *options) {
		o.registerErrHandler = fn
	}
}

func WithOnClose(fn func()) Option {
	return func(o *options) {
		o.onClose = fn
	}
}

func WithReplication(opts ...replication.Option) Option {
	return func(o *options) {
		o.repl = append(o.repl, opts...)
	}
}

type options struct {
	path                      string
	inMemory                  bool
	applyDefaults             bool
	logger                    Logger
	ignoreProtoRegisterErrors bool
	registerErrHandler        func(err error) error
	badgerOptionsFunc         func(opts badger.Options) badger.Options
	// lowMemory                 bool
	onClose func()

	repl []replication.Option
}

var defaultOptions = options{
	path: DefaultPath,
}

func makeGetOpts(opts ...protodb.GetOption) protodb.GetOpts {
	o := protodb.GetOpts{}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

func makeSetOpts(opts ...protodb.SetOption) protodb.SetOpts {
	o := protodb.SetOpts{}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}
