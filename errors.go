// Copyright 2022 Linka Cloud  All rights reserved.
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
	"errors"

	"github.com/dgraph-io/badger/v3"
)

var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = badger.ErrKeyNotFound

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig = badger.ErrTxnTooBig

	// ErrConflict is returned when a transaction conflicts with another transaction. This can
	// happen if the read rows had been updated concurrently by another transaction.
	ErrConflict = badger.ErrConflict

	// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
	ErrReadOnlyTxn = badger.ErrReadOnlyTxn

	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = badger.ErrEmptyKey

	// ErrInvalidKey is returned if the key has a special !badger! prefix,
	// reserved for internal usage.
	ErrInvalidKey = badger.ErrInvalidKey

	// ErrBannedKey is returned if the read/write key belongs to any banned namespace.
	ErrBannedKey = badger.ErrBannedKey

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = badger.ErrInvalidRequest

	// ErrInvalidDump if a data dump made previously cannot be loaded into the database.
	ErrInvalidDump = badger.ErrInvalidDump

	// ErrWindowsNotSupported is returned when opt.ReadOnly is used on Windows
	ErrWindowsNotSupported = badger.ErrWindowsNotSupported

	// ErrPlan9NotSupported is returned when opt.ReadOnly is used on Plan 9
	ErrPlan9NotSupported = badger.ErrPlan9NotSupported

	// ErrTruncateNeeded is returned when the value log gets corrupt, and requires truncation of
	// corrupt data to allow Badger to run properly.
	ErrTruncateNeeded = badger.ErrTruncateNeeded

	// ErrBlockedWrites is returned if the user called DropAll. During the process of dropping all
	// data from Badger, we stop accepting new writes, by returning this error.
	ErrBlockedWrites = badger.ErrBlockedWrites

	// ErrEncryptionKeyMismatch is returned when the storage key is not
	// matched with the key previously given.
	ErrEncryptionKeyMismatch = badger.ErrEncryptionKeyMismatch

	// ErrInvalidDataKeyID is returned if the datakey id is invalid.
	ErrInvalidDataKeyID = badger.ErrInvalidDataKeyID

	// ErrInvalidEncryptionKey is returned if length of encryption keys is invalid.
	ErrInvalidEncryptionKey = badger.ErrInvalidEncryptionKey

	// ErrDBClosed is returned when a get operation is performed after closing the DB.
	ErrDBClosed = badger.ErrDBClosed

	ErrNotLeader = errors.New("current node is not leader")
)
