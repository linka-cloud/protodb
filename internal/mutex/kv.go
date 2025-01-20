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

package mutex

import (
	"sync"
)

// NewKV returns a properly initialized KV
func NewKV() *KV {
	return &KV{
		store: make(map[string]*sync.Mutex),
	}
}

// KV is a simple key/value store for arbitrary mutexes. It can be used to
// serialize changes across arbitrary collaborators that share knowledge of the
// keys they must serialize on.
//
// The initial use case is to let aws_security_group_rule resources serialize
// their access to individual security groups based on SG ID.
type KV struct {
	lock  sync.Mutex
	store map[string]*sync.Mutex
}

// Lock locks the mutex for the given key. Caller is responsible for calling Unlock
// for the same key
func (m *KV) Lock(key string) {
	m.get(key).Lock()
}

// Unlock the mutex for the given key. Caller must have called Lock for the same key first
func (m *KV) Unlock(key string) {
	m.get(key).Unlock()
}

// Returns a mutex for the given key, no guarantee of its lock status
func (m *KV) get(key string) *sync.Mutex {
	m.lock.Lock()
	defer m.lock.Unlock()
	mutex, ok := m.store[key]
	if !ok {
		mutex = &sync.Mutex{}
		m.store[key] = mutex
	}
	return mutex
}
