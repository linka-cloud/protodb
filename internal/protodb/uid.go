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

package protodb

import (
	"bytes"

	"github.com/dgraph-io/badger/v3/y"
)

func UIDKey(uid uint64) []byte {
	return bytes.Join([][]byte{[]byte(UID), y.U64ToBytes(uid)}, []byte("/"))
}

func UIDRevKey(key []byte) []byte {
	return bytes.Join([][]byte{[]byte(UIDRev), bytes.TrimPrefix(key, []byte(Data+"/"))}, []byte("/"))
}

func UIDLastKey() []byte {
	return []byte(UIDLast)
}

func StorageVersionKey() []byte {
	return bytes.Join([][]byte{[]byte(Internal), []byte(Storage)}, []byte("/"))
}
