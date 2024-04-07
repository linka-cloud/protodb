// Copyright 2024 Linka Cloud  All rights reserved.
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

package pending

import (
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
)

var _ Item = (*item)(nil)

type item struct {
	e      *badger.Entry
	readTs uint64
}

func (i *item) Key() []byte {
	return i.e.Key
}

func (i *item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, i.e.Key)
}

func (i *item) Value(fn func(val []byte) error) error {
	return fn(i.e.Value)
}

func (i *item) ValueCopy(dst []byte) ([]byte, error) {
	return y.SafeCopy(dst, i.e.Value), nil
}

func (i *item) IsDeletedOrExpired() bool {
	if i.e.UserMeta&BitDelete > 0 {
		return true
	}
	if i.e.ExpiresAt == 0 {
		return false
	}
	return i.e.ExpiresAt <= uint64(time.Now().Unix())
}

func (i *item) ExpiresAt() uint64 {
	return i.e.ExpiresAt
}

func (i *item) Version() uint64 {
	return i.readTs
}
