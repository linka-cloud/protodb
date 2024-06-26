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

package db

import (
	"context"
	"io"

	"github.com/dgraph-io/badger/v3"
)

func (db *db) MaxVersion() uint64 {
	return db.bdb.MaxVersion()
}

func (db *db) SetVersion(v uint64) {
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

func (db *db) LoadDescriptors(ctx context.Context) error {
	return db.loadDescriptors(ctx)
}

func (db *db) NewWriteBatchAt(commitTs uint64) *badger.WriteBatch {
	return db.bdb.NewWriteBatchAt(commitTs)
}

func (db *db) Path() string {
	return db.opts.path
}

func (db *db) InMemory() bool {
	return db.opts.inMemory
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
