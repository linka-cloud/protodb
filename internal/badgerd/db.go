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
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/badger/v3/y"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	"go.linka.cloud/protodb/internal/badgerd/pending"
	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/badgerd/replication/gossip"
)

var ErrNotLeader = errors.New("current node is not leader")

var _ DB = (*db)(nil)

var (
	internalPrefix = []byte("!badgerd!")
	commitTsKey    = []byte("!badgerd!commitTs")
)

func Open(ctx context.Context, opts ...Option) (DB, error) {
	o := defaultOptions
	for _, v := range opts {
		v(&o)
	}
	if o.logger == nil {
		o.logger = logger.C(ctx).WithField("service", "badgerd")
	}
	bopts := o.build()
	// we do our own conflict checks, so we don't need badger to do it
	bopts.DetectConflicts = false
	bdb, err := badger.OpenManaged(bopts)
	if err != nil {
		return nil, err
	}

	db, err := newDB(ctx, bdb, o, bopts)
	if err != nil {
		return nil, multierr.Combine(err, bdb.Close())
	}
	y.Check(db.replayWal())
	return db, nil
}

func newDB(ctx context.Context, bdb *badger.DB, o options, bopts badger.Options) (*db, error) {
	orc := newOracle()

	var max uint64
	if err := bdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(commitTsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		max = y.BytesToU64(v)
		return nil
	}); err != nil {
		return nil, err
	}
	if orc.nextTxnTs == 0 {
		max = bdb.MaxVersion()
	}

	// We do increment nextTxnTs below. So, no need to do it here.
	orc.nextTxnTs = max
	// Let's advance nextTxnTs to one more than whatever we observed via
	// replaying the logs.
	orc.txnMark.Done(orc.nextTxnTs)
	// In normal mode, we must update readMark so older versions of keys can be removed during
	// compaction when run in offline mode via the flatten tool.
	orc.readMark.Done(orc.nextTxnTs)
	orc.incrementNextTs()
	db := &db{bdb: bdb, bopts: bopts, orc: orc}
	var ro replication.Options
	for _, v := range o.repl {
		v(&ro)
	}
	var err error
	switch ro.Mode {
	case replication.ModeAsync, replication.ModeSync:
		db.repl, err = gossip.New(ctx, db, o.repl...)
		if err != nil {
			return nil, err
		}
	default:
	}
	return db, nil
}

type db struct {
	bdb   *badger.DB
	bopts badger.Options

	orc        *oracle
	mu         sync.Mutex
	maxVersion uint64
	mmu        sync.RWMutex

	repl replication.Replication

	cmu   sync.RWMutex
	close bool
}

func (db *db) View(fn func(tx *badger.Txn) error) error {
	return db.bdb.View(fn)
}

func (db *db) Subscribe(ctx context.Context, cb func(kv *badger.KVList) error, matches []pb.Match) error {
	return db.bdb.Subscribe(ctx, cb, matches)
}

func (db *db) NewTransaction(ctx context.Context, update bool) (Tx, error) {
	return db.newTransaction(ctx, update)
}

func (db *db) MaxVersion() uint64 {
	db.mmu.RLock()
	defer db.mmu.RUnlock()
	return db.maxVersion
}

func (db *db) SetVersion(v uint64) {
	db.mmu.Lock()
	defer db.mmu.Unlock()
	db.maxVersion = v
	db.orc.Lock()
	db.orc.txnMark.Done(v)
	// we do not set the read mark here
	db.orc.nextTxnTs = v + 1
	db.orc.Unlock()
}

func (db *db) Drop() error {
	return db.bdb.DropAll()
}

func (db *db) Load(_ context.Context, reader io.Reader) (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.bdb.Load(reader, 1024); err != nil {
		return 0, err
	}
	v := db.bdb.MaxVersion()
	db.orc.txnMark.Done(v)
	db.orc.readMark.Done(v)
	db.orc.nextTxnTs = v + 1
	return v, nil
}

func (db *db) Stream(_ context.Context, at, since uint64, w io.Writer) error {
	s := db.bdb.NewStreamAt(at)
	s.LogPrefix = "Init replication"
	s.SinceTs = since
	_, err := s.Backup(w, since)
	return err
}

func (db *db) NewWriteBatchAt(readTs uint64) replication.WriteBatch {
	return db.bdb.NewWriteBatchAt(readTs)
}

func (db *db) Path() string {
	return db.bdb.Opts().Dir
}

func (db *db) InMemory() bool {
	return db.bdb.Opts().InMemory
}

func (db *db) ValueThreshold() int64 {
	return db.bdb.Opts().ValueThreshold
}

func (db *db) MaxBatchCount() int64 {
	return db.bdb.MaxBatchCount()
}

func (db *db) MaxBatchSize() int64 {
	return db.bdb.MaxBatchSize()
}

func (db *db) Replicated() bool {
	return db.repl != nil
}

func (db *db) IsLeader() bool {
	y.AssertTruef(db.repl != nil, "call IsLeader on non replicated instance")
	return db.repl.IsLeader()
}

func (db *db) Leader() string {
	y.AssertTruef(db.repl != nil, "call IsLeader on non replicated instance")
	if _, ok := db.repl.LeaderConn(); ok || db.repl.IsLeader() {
		return db.repl.CurrentLeader()
	}
	return ""
}

func (db *db) LeaderChanges() <-chan string {
	y.AssertTruef(db.repl != nil, "call IsLeader on non replicated instance")
	return db.repl.Subscribe()
}

func (db *db) LeaderConn() (grpc.ClientConnInterface, bool) {
	y.AssertTruef(db.repl != nil, "call LeaderConn on non replicated instance")
	return db.repl.LeaderConn()
}

func (db *db) LinearizableReads() bool {
	y.AssertTruef(db.repl != nil, "call LinearizableReads on non replicated instance")
	return db.repl.LinearizableReads()
}

func (db *db) Close() (err error) {
	if db.closed() {
		return nil
	}
	db.cmu.Lock()
	db.close = true
	if db.repl != nil {
		err = multierr.Append(err, db.repl.Close())
	}
	db.cmu.Unlock()
	return multierr.Append(err, db.bdb.Close())
}

func (db *db) closed() bool {
	db.cmu.RLock()
	defer db.cmu.RUnlock()
	return db.close
}

func (db *db) replayWal() error {
	files, err := filepath.Glob(filepath.Join(db.bdb.Opts().Dir, "tx-*.wal"))
	if err != nil {
		return err
	}
	for _, v := range files {
		if err := db.replayWalFile(v); err != nil {
			return err
		}
	}
	return nil
}

func (db *db) replayWalFile(path string) error {
	w, err := pending.OpenWAL(path)
	if err != nil {
		return nil
	}
	if w.CommitTs() <= db.orc.nextTxnTs-1 {
		return w.Close()
	}
	b := db.bdb.NewWriteBatchAt(w.CommitTs())
	if err := w.Replay(func(e *badger.Entry) error {
		if e.UserMeta&pending.BitDelete > 0 {
			return b.Delete(e.Key)
		}
		return b.SetEntry(e)
	}); err != nil {
		return err
	}
	return w.Close()
}
