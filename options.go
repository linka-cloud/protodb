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

package protodb

import (
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var (
	DefaultPath = "./data"
)

type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

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

func WithNumVersionsToKeep(n int) Option {
	return func(o *options) {
		o.numVersions = n
	}
}

func WithLowMemory(b bool) Option {
	return func(o *options) {
		o.lowMemory = b
	}
}

func WithApplyDefaults(b bool) Option {
	return func(o *options) {
		o.applyDefaults = b
	}
}

func WithIgnoreProtoRegisterErrors(b bool) Option {
	return func(o *options) {
		o.ignoreProtoRegisterErrors = true
	}
}

func WithProtoRegisterErrHandler(fn func(err error) error) Option {
	return func(o *options) {
		o.registerErrHandler = fn
	}
}

type options struct {
	path                      string
	inMemory                  bool
	applyDefaults             bool
	logger                    Logger
	numVersions               int
	ignoreProtoRegisterErrors bool
	registerErrHandler        func(err error) error
	badgerOptionsFunc         func(opts badger.Options) badger.Options
	lowMemory                 bool
}

func (o options) build() badger.Options {
	opts := badger.DefaultOptions(o.path).
		WithInMemory(o.inMemory).
		WithNumVersionsToKeep(o.numVersions).
		WithLogger(o.logger)
	// https://github.com/dgraph-io/badger/issues/1304#issuecomment-630078745
	if o.lowMemory {
		// opts.TableLoadingMode = boptions.FileIO
		// opts.ValueLogLoadingMode = boptions.FileIO

		// Memtable size. This is the actively written in-memory data structure.
		// This value also affects the size of a transaction. The size of a transaction is 15% of the table size.
		// opts.MaxTableSize = 1 << 20
		// To allow writes at a faster speed, we create a new memtable as soon as
		// an existing memtable is filled up. This option determines how many
		// memtables should be kept in memory.
		opts.NumMemtables = 1

		// This option will have a significant effect the memory. If the level is kept
		// in-memory, read are faster but the tables will be kept in memory.
		// opts.KeepL0InMemory = false

		// The NumLevelZeroTables and NumLevelZeroTableStall will not have any
		// effect on the memory if `KeepL0InMemory` is set to false.
		opts.NumLevelZeroTables = 1
		opts.NumLevelZeroTablesStall = 2

		// SyncWrites=false has significant effect on write performance. When sync
		// writes is set to true, badger ensures the data is flushed to the disk after a
		// write call. For normal usage, such a high level of consistency is not required.
		// opts.SyncWrites = false

		// LoadBloomsOnOpen=false will improve the db startup speed
		// opts.LoadBloomsOnOpen = false

		// Bloom filters will be kept in memory if the following option is not set. Each
		// bloom filter takes up 5 MB of memory. A smaller bf cache would mean that
		// bloom filters will be evicted quickly from the cache and they will be read from
		// the disk (which is slow) and inserted into the cache.
		// opts.MaxBfCacheSize = 100 << 20
	}
	if o.badgerOptionsFunc != nil {
		opts = o.badgerOptionsFunc(opts)
	}
	return opts
}

var defaultOptions = options{
	path:        DefaultPath,
	numVersions: 2,
}

type GetOption func(o *GetOpts)

func WithPaging(paging *Paging) GetOption {
	return func(o *GetOpts) {
		o.Paging = paging
	}
}

func WithFilter(filter Filter) GetOption {
	return func(o *GetOpts) {
		o.Filter = filter
	}
}

func WithReadFieldMaskPaths(paths ...string) GetOption {
	return func(o *GetOpts) {
		o.FieldMask = &fieldmaskpb.FieldMask{Paths: paths}
	}
}

func WithReadFieldMask(fieldMask *fieldmaskpb.FieldMask) GetOption {
	return func(o *GetOpts) {
		o.FieldMask = fieldMask
	}
}

type GetOpts struct {
	Paging    *Paging
	Filter    Filter
	FieldMask *fieldmaskpb.FieldMask
}

func makeGetOpts(opts ...GetOption) GetOpts {
	o := GetOpts{}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

type SetOption func(o *SetOpts)

func WithTTL(d time.Duration) SetOption {
	return func(o *SetOpts) {
		o.TTL = d
	}
}

func WithWriteFieldMaskPaths(paths ...string) SetOption {
	return func(o *SetOpts) {
		o.FieldMask = &fieldmaskpb.FieldMask{Paths: paths}
	}
}

func WithWriteFieldMask(fieldMask *fieldmaskpb.FieldMask) SetOption {
	return func(o *SetOpts) {
		o.FieldMask = fieldMask
	}
}

type SetOpts struct {
	TTL       time.Duration
	FieldMask *fieldmaskpb.FieldMask
}

func makeSetOpts(opts ...SetOption) SetOpts {
	o := SetOpts{}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}
