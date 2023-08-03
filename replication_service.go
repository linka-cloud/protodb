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

package protodb

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	gerrs "go.linka.cloud/grpc/errors"
	"go.linka.cloud/grpc/logger"

	repl2 "go.linka.cloud/protodb/internal/replication"
)

func (r *repl) Init(req *repl2.InitRequest, ss repl2.ReplicationService_InitServer) error {
	if !r.isLeader() {
		return gerrs.FailedPreconditionf("cannot initialize from non-le")
	}
	m := r.db.bdb.MaxVersion()
	if req.Since > m {
		return fmt.Errorf("invalid replication version %d, max is %d", req.Since, m)
	}
	if req.Since == m {
		return nil
	}
	s := r.db.bdb.NewStreamAt(m)
	s.LogPrefix = "Init replication"
	s.SinceTs = req.Since
	_, err := s.Backup(&replWriter{ss: ss}, req.Since)
	return err
}

func (r *repl) Replicate(ss repl2.ReplicationService_ReplicateServer) error {
	if r.isLeader() {
		return gerrs.FailedPreconditionf("cannot replicate to le")
	}
	log := logger.C(ss.Context())
	var tx *badger.Txn
	for {
		op, err := ss.Recv()
		if err != nil {
			return err
		}
		switch a := op.Action.(type) {
		case *repl2.Op_New:
			log.Debugf("new transaction at %d", a.New.At)
			if tx != nil {
				return gerrs.InvalidArgumentf("transaction already started")
			}
			tx = r.db.bdb.NewTransactionAt(a.New.At, true)
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
		case *repl2.Op_Set:
			log.WithField("key", string(a.Set.Key)).Debugf("set key")
			if tx == nil {
				return gerrs.InvalidArgumentf("transaction not started")
			}
			if err := tx.SetEntry(&badger.Entry{
				Key:       a.Set.Key,
				Value:     a.Set.Value,
				ExpiresAt: a.Set.ExpiresAt,
			}); err != nil {
				return gerrs.Internalf("failed to set key %s: %v", a.Set.Key, err)
			}
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
		case *repl2.Op_Delete:
			log.WithField("key", string(a.Delete.Key)).Debugf("delete key")
			if tx == nil {
				return gerrs.InvalidArgumentf("transaction not started")
			}
			if err := tx.Delete(a.Delete.Key); err != nil {
				return gerrs.Internalf("failed to delete key %s: %v", a.Delete.Key, err)
			}
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send ack: %v", err)
			}
		case *repl2.Op_Commit:
			log.Debugf("commit transaction at %d", a.Commit.At)
			if tx == nil {
				return gerrs.InvalidArgumentf("transaction not started")
			}
			errs := make(chan error, 1)
			if err := tx.CommitAt(a.Commit.At, func(err error) {
				errs <- err
			}); err != nil {
				return gerrs.Internalf("failed to commit transaction: %v", err)
			}
			if err := <-errs; err != nil {
				return gerrs.Internalf("failed to commit transaction: %v", err)
			}
			if err := ss.Send(&repl2.Ack{}); err != nil {
				return gerrs.Internalf("failed to send response: %v", err)
			}
			r.db.orc.Lock()
			r.db.orc.txnMark.Done(a.Commit.At)
			r.db.orc.readMark.Done(a.Commit.At)
			r.db.orc.nextTxnTs = a.Commit.At + 1
			r.db.orc.Unlock()
			return nil
		}
	}
}
