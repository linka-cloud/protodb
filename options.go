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

	"github.com/dgraph-io/badger/v2"
)

var (
	DefaultPath = "./data"
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

func WithApplyDefaults(b bool) Option {
	return func(o *options) {
		o.applyDefaults = b
	}
}

type options struct {
	path          string
	inMemory      bool
	applyDefaults bool
}

func (o options) build() badger.Options {
	return badger.DefaultOptions(o.path).WithInMemory(o.inMemory)
}

var defaultOptions = options{
	path: DefaultPath,
}
