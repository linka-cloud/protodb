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

package badgerd

import (
	"context"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/ristretto/z"
	"go.linka.cloud/grpc-toolkit/logger"

	"go.linka.cloud/protodb/internal/badgerd/pending"
	"go.linka.cloud/protodb/internal/badgerd/replication"
)

var _ Tx = (*tx)(nil)

func (db *db) newTransaction(ctx context.Context, update bool) (*tx, error) {
	readTs := db.orc.readTs()
	if db.Replicated() {
		logger.C(ctx).Tracef("starting transaction with readTs %d", readTs)
	}
	txn := db.bdb.NewTransactionAt(readTs, false)
	tx := &tx{
		db:           db,
		update:       update,
		readTs:       readTs,
		repl:         &replication.Maybe{DB: db},
		conflictKeys: make(map[uint64]struct{}),
	}
	var err error
	if update && db.Replicated() {
		if !db.repl.IsLeader() {
			return nil, ErrNotLeader
		}
		if tx.repl.Tx, err = db.repl.NewTx(ctx); err != nil {
			return nil, err
		}
	}
	if err = tx.repl.New(ctx, txn, tx.addReadKey); err != nil {
		return nil, err
	}
	return tx, nil
}

type tx struct {
	db       *db
	update   bool
	repl     *replication.Maybe
	readTs   uint64
	doneRead bool

	reads []uint64 // contains fingerprints of keys read.
	// contains fingerprints of keys written. This is used for conflict detection.
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	m    sync.RWMutex
	done bool
}

func (tx *tx) ReadTs() uint64 {
	return tx.readTs
}

func (tx *tx) Iterator(opt badger.IteratorOptions) pending.Iterator {
	return tx.repl.Iterator(opt)
}

func (tx *tx) Set(ctx context.Context, key, value []byte, expiresAt uint64) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if !tx.update {
		return badger.ErrReadOnlyTxn
	}
	if err := tx.repl.Set(ctx, key, value, expiresAt); err != nil {
		return err
	}
	tx.addConflictKey(key)
	return nil
}

func (tx *tx) Get(ctx context.Context, key []byte) (pending.Item, error) {
	if tx.closed() {
		return nil, badger.ErrDBClosed
	}
	return tx.repl.Get(ctx, key)
}

func (tx *tx) Delete(ctx context.Context, key []byte) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	if err := tx.repl.Delete(ctx, key); err != nil {
		return err
	}
	tx.addConflictKey(key)
	return nil
}

func (tx *tx) Commit(ctx context.Context) error {
	if tx.closed() {
		return badger.ErrDBClosed
	}
	defer tx.close()
	if !tx.update {
		return nil
	}
	tx.db.orc.writeChLock.Lock()
	defer tx.db.orc.writeChLock.Unlock()

	ts, conflict := tx.db.orc.newCommitTs(tx)
	if conflict {
		return badger.ErrConflict
	}
	defer tx.db.orc.doneCommit(ts)

	return tx.repl.CommitAt(ctx, ts)
}

func (tx *tx) Close(_ context.Context) error {
	return tx.close()
}

func (tx *tx) addReadKey(key []byte) {
	if tx.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		tx.readsLock.Lock()
		tx.reads = append(tx.reads, fp)
		tx.readsLock.Unlock()
	}
}

func (tx *tx) addConflictKey(key []byte) {
	if !tx.update {
		return
	}
	fp := z.MemHash(key)
	tx.conflictKeys[fp] = struct{}{}
}

func (tx *tx) close() error {
	if tx.closed() {
		return nil
	}
	tx.m.Lock()
	tx.done = true
	err := tx.repl.Close(context.Background())
	tx.m.Unlock()
	return err
}

func (tx *tx) closed() bool {
	tx.m.RLock()
	defer tx.m.RUnlock()
	return tx.done || tx.db.closed()
}
