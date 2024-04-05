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

package gossip

import (
	"sync"
	"sync/atomic"
)

func NewAtomic[T any](value T) *Atomic[T] {
	v := atomic.Value{}
	v.Store(value)
	return &Atomic[T]{value: v}
}

type Atomic[T any] struct {
	value atomic.Value
}

func (a *Atomic[T]) Load() T {
	return a.value.Load().(T)
}

func (a *Atomic[T]) Store(value T) {
	a.value.Store(value)
}

func (a *Atomic[T]) CompareAndSwap(old, new T) bool {
	return a.value.CompareAndSwap(old, new)
}

func (a *Atomic[T]) Swap(value T) T {
	return a.value.Swap(value).(T)
}

type Map[T any] struct {
	m sync.Map
}

func (m *Map[T]) Load(key string) (value T, ok bool) {
	v, ok := m.m.Load(key)
	if !ok {
		return
	}
	value, ok = v.(T)
	return
}

func (m *Map[T]) Store(key string, value T) {
	m.m.Store(key, value)
}

func (m *Map[T]) Delete(key string) {
	m.m.Delete(key)
}

func (m *Map[T]) Range(f func(key string, value T) bool) {
	m.m.Range(func(key, value interface{}) bool {
		return f(key.(string), value.(T))
	})
}

func (m *Map[T]) Len() int {
	var l int
	m.m.Range(func(key, value interface{}) bool {
		l++
		return true
	})
	return l
}

func (m *Map[T]) Clear() {
	m.m.Range(func(key, value interface{}) bool {
		m.m.Delete(key)
		return true
	})
}

func (m *Map[T]) Values() []T {
	var values []T
	m.m.Range(func(key, value interface{}) bool {
		values = append(values, value.(T))
		return true
	})
	return values
}

func (m *Map[T]) Keys() []string {
	var keys []string
	m.m.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}
