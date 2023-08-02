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

package registry

import (
	"sync"
)

type SyncMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{
		m: map[K]V{},
	}
}

func (m *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	m.mu.RLock()
	value, ok = m.m[key]
	m.mu.RUnlock()
	return
}

func (m *SyncMap[K, V]) Store(key K, value V) {
	m.mu.Lock()
	m.m[key] = value
	m.mu.Unlock()
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.mu.Lock()
	delete(m.m, key)
	m.mu.Unlock()
}

func (m *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.mu.RLock()
	for k, v := range m.m {
		if !f(k, v) {
			break
		}
	}
	m.mu.RUnlock()
}

func (m *SyncMap[K, V]) Len() int {
	m.mu.RLock()
	l := len(m.m)
	m.mu.RUnlock()
	return l
}

func (m *SyncMap[K, V]) Merge(o *SyncMap[K, V]) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range o.m {
		m.m[k] = v
	}
}
