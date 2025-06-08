// Copyright 2025 Linka Cloud  All rights reserved.
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

package mutex

import (
	"context"
	"errors"
	"sync"
)

func NewContextKV() *ContextKV {
	return &ContextKV{
		store: make(map[string]*ContextMutex),
	}
}

type ContextKV struct {
	lock  sync.Mutex
	store map[string]*ContextMutex
}

func (m *ContextKV) Lock(ctx context.Context, key string) error {
	return m.get(key).Lock(ctx)
}

func (m *ContextKV) Unlock(key string) error {
	return m.get(key).Unlock()
}

// Returns a mutex for the given key, no guarantee of its lock status
func (m *ContextKV) get(key string) *ContextMutex {
	m.lock.Lock()
	defer m.lock.Unlock()
	mutex, ok := m.store[key]
	if !ok {
		mutex = &ContextMutex{}
		m.store[key] = mutex
	}
	return mutex
}

type ContextMutex struct {
	o    sync.Once
	lock chan struct{}
}

func (c *ContextMutex) Lock(ctx context.Context) error {
	c.o.Do(func() {
		c.lock = make(chan struct{}, 1)
	})
	select {
	case c.lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ContextMutex) Unlock() error {
	c.o.Do(func() {
		c.lock = make(chan struct{}, 1)
	})
	select {
	case <-c.lock:
		return nil
	default:
		return errors.New("unlock called on an unlocked mutex")
	}
}
