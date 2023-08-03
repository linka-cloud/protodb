// Copyright 2021 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	bpb "github.com/dgraph-io/badger/v3/pb"
	"go.linka.cloud/grpc/logger"
	"go.linka.cloud/protoc-gen-defaults/defaults"
	pf "go.linka.cloud/protofilters"
	"google.golang.org/protobuf/proto"
	pdesc "google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	preg "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.linka.cloud/protodb/pb"
)

const (
	protobufRegistrationConflictEnv    = "GOLANG_PROTOBUF_REGISTRATION_CONFLICT"
	protobufRegistrationConflictPolicy = "ignore"
)

func Open(ctx context.Context, opts ...Option) (DB, error) {
	o := defaultOptions
	for _, v := range opts {
		v(&o)
	}
	if o.logger == nil {
		o.logger = logger.C(ctx).WithField("service", "protodb")
	}
	bopts := o.build()
	// we check for conflicts internally, so we don't need badger to do it
	bopts.DetectConflicts = false
	bdb, err := badger.OpenManaged(bopts)
	if err != nil {
		return nil, err
	}

	orc := newOracle()

	// We do increment nextTxnTs below. So, no need to do it here.
	orc.nextTxnTs = bdb.MaxVersion()

	// Let's advance nextTxnTs to one more than whatever we observed via
	// replaying the logs.
	orc.txnMark.Done(orc.nextTxnTs)
	// In normal mode, we must update readMark so older versions of keys can be removed during
	// compaction when run in offline mode via the flatten tool.
	orc.readMark.Done(orc.nextTxnTs)
	orc.incrementNextTs()
	// TODO(adphi): Use custom registry to avoid this hack
	if err := os.Setenv(protobufRegistrationConflictEnv, protobufRegistrationConflictPolicy); err != nil {
		return nil, err
	}
	reg := preg.GlobalFiles

	db := &db{bdb: bdb, orc: orc, opts: o, reg: reg, bopts: bopts}
	if o.repl.mode != ReplicationModeNone {
		var err error
		db.repl, err = newRepl(ctx, db, o.repl)
		if err != nil {
			return nil, err
		}
	}
	if db.repl != nil && db.repl.isLeader() {
		if err := db.load(ctx); err != nil {
			return nil, err
		}
	}
	return db, nil
}

type db struct {
	bdb   *badger.DB
	opts  options
	bopts badger.Options
	orc   *oracle

	mu  sync.RWMutex
	reg *preg.Files

	repl *repl

	cmu   sync.RWMutex
	close bool
}

func (db *db) Watch(ctx context.Context, m proto.Message, opts ...GetOption) (<-chan Event, error) {
	end := metrics.Watch.Start(string(m.ProtoReflect().Descriptor().FullName()))
	o := makeGetOpts(opts...)

	k, _ := dataPrefix(m)
	log := logger.StandardLogger().WithFields("service", "protodb", "action", "watch", "key", string(k))
	log.Debugf("start watching for key %s", string(k))
	ch := make(chan Event, 1)
	wait := make(chan struct{})
	go func() {
		defer end.End()
		defer close(ch)
		log.Debugf("subscribing to changes")
		close(wait)
		err := db.bdb.Subscribe(ctx, func(kv *badger.KVList) error {
			log.Debugf("received event containing %d kv", len(kv.Kv))
			for _, v := range kv.Kv {
				if err := ctx.Err(); err != nil {
					return err
				}
				var err error
				var typ EventType
				var new proto.Message
				var old proto.Message
				if err := db.bdb.View(func(txn *badger.Txn) error {
					it := txn.NewKeyIterator(v.Key, badger.IteratorOptions{AllVersions: true, PrefetchValues: false})
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						if item.Version() == v.Version {
							continue
						}
						if item.IsDeletedOrExpired() {
							return nil
						}
						return item.Value(func(val []byte) error {
							o := m.ProtoReflect().New().Interface()
							if err := db.unmarshal(val, o); err != nil {
								return err
							}
							old = o
							return nil
						})
					}
					return nil
				}); err != nil {
					return err
				}
				if len(v.Value) != 0 {
					new = m.ProtoReflect().New().Interface()
					if err := db.unmarshal(v.Value, new); err != nil {
						return err
					}
				}
				if o.Filter == nil {
					if new == nil {
						typ = pb.WatchEventLeave
					} else if old != nil {
						typ = pb.WatchEventUpdate
					} else {
						typ = pb.WatchEventEnter
					}

					log.Debugf("sending event with key %s and type %v", string(v.Key), typ)
					select {
					case ch <- event{typ: typ, old: old, new: new}:
						continue
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				var was bool
				if old != nil {
					was, err = pf.Match(old, o.Filter)
					if err != nil {
						return err
					}
				}
				var is bool
				if new != nil {
					is, err = pf.Match(new, o.Filter)
					if err != nil {
						return err
					}
				}
				if was {
					if !is {
						typ = pb.WatchEventLeave
					} else {
						typ = pb.WatchEventUpdate
					}
				} else {
					if is {
						typ = pb.WatchEventEnter
					} else {
						log.Debugf("skipping non matching event with key %s", string(v.Key))
						continue
					}
				}
				log.Debugf("sending event with key %s and type %v", string(v.Key), typ)
				select {
				case ch <- event{new: new, old: old, typ: typ}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}, []bpb.Match{{Prefix: k}})
		if err != nil {
			metrics.Watch.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
			log.Errorf("failed to watch prefix %s: %v", string(k), err)
		}
		select {
		case ch <- event{err: err}:
		case <-ctx.Done():
		}
		log.Debugf("stopped watching")
	}()
	<-wait
	return ch, nil
}

func (db *db) Get(ctx context.Context, m proto.Message, opts ...GetOption) ([]proto.Message, *PagingInfo, error) {
	defer metrics.Get.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	tx, err := db.Tx(ctx, WithReadOnly())
	if err != nil {
		metrics.Get.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return nil, nil, err
	}
	defer tx.Close()
	msgs, paging, err := tx.Get(ctx, m, opts...)
	if err != nil {
		metrics.Get.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
	}
	return msgs, paging, err
}

func (db *db) Set(ctx context.Context, m proto.Message, opts ...SetOption) (proto.Message, error) {
	defer metrics.Set.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	tx, err := db.Tx(ctx)
	if err != nil {
		metrics.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return nil, err
	}
	defer tx.Close()
	m, err = tx.Set(ctx, m, opts...)
	if err != nil {
		metrics.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		metrics.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		metrics.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return nil, err
	}
	return m, nil
}

func (db *db) Delete(ctx context.Context, m proto.Message) error {
	defer metrics.Delete.Start(string(m.ProtoReflect().Descriptor().FullName())).End()
	tx, err := db.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := tx.Delete(ctx, m); err != nil {
		metrics.Delete.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return err
	}
	if err := ctx.Err(); err != nil {
		metrics.Delete.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		metrics.Delete.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return err
	}
	return nil
}

func (db *db) Tx(ctx context.Context, opts ...TxOption) (Tx, error) {
	return newTx(ctx, db, opts...)
}

func (db *db) Close() error {
	db.cmu.Lock()
	db.close = true
	if db.repl != nil {
		db.repl.close()
	}
	db.cmu.Unlock()
	return db.bdb.Close()
}

func (db *db) closed() bool {
	db.cmu.RLock()
	defer db.cmu.RUnlock()
	return db.close
}

func (db *db) unmarshal(b []byte, m proto.Message) error {
	switch v := interface{}(m).(type) {
	case interface{ UnmarshalVT(b []byte) error }:
		return v.UnmarshalVT(b)
	case interface{ Unmarshal(b []byte) error }:
		return v.Unmarshal(b)
	default:
		return proto.UnmarshalOptions{}.Unmarshal(b, m)
	}
}

func (db *db) marshal(m proto.Message) ([]byte, error) {
	switch v := interface{}(m).(type) {
	case interface{ MarshalVT() ([]byte, error) }:
		return v.MarshalVT()
	case interface{ Marshal() ([]byte, error) }:
		return v.Marshal()
	default:
		return proto.MarshalOptions{}.Marshal(m)
	}
}

func (db *db) RegisterProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) error {
	if file == nil || file.GetName() == "" {
		return errors.New("invalid file")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.registerFileDescriptorProto(ctx, file); err != nil {
		return err
	}
	txn, err := newTx(ctx, db)
	if err != nil {
		return err
	}
	defer txn.Close()
	b, err := proto.Marshal(file)
	if err != nil {
		return err
	}
	if err := txn.setRaw(ctx, descriptorPrefix(file), b); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (db *db) Register(ctx context.Context, fd protoreflect.FileDescriptor) error {
	return db.RegisterProto(ctx, pdesc.ToFileDescriptorProto(fd))
}

func (db *db) Resolver() pdesc.Resolver {
	return db.reg
}

func (db *db) Descriptors(_ context.Context) ([]*descriptorpb.DescriptorProto, error) {
	m := make(map[string]*descriptorpb.FileDescriptorProto)
	if err := db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(descriptors)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				fdp := &descriptorpb.FileDescriptorProto{}
				if err := db.unmarshal(val, fdp); err != nil {
					return err
				}
				m[fdp.GetName()] = fdp
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	var out []*descriptorpb.DescriptorProto
	for _, v := range m {
		out = append(out, v.MessageType...)
	}
	return out, nil
}

func (db *db) FileDescriptors(_ context.Context) ([]*descriptorpb.FileDescriptorProto, error) {
	var out []*descriptorpb.FileDescriptorProto
	if err := db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(descriptors)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				fdp := &descriptorpb.FileDescriptorProto{}
				if err := db.unmarshal(val, fdp); err != nil {
					return err
				}
				out = append(out, fdp)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *db) Backup(ctx context.Context, w io.Writer) error {
	_, err := db.bdb.NewStream().Backup(w, 0)
	return err
}

func (db *db) registerFileDescriptorProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) (err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			ok, err = isAlreadyRegistered(e)
			if ok {
				err = db.recoverRegister(ctx, file)
			}
			if err != nil {
				err = db.handleRegisterErr(err)
			}
		}
	}()
	fd, err := pdesc.NewFile(file, db.reg)
	if err != nil {
		return db.handleRegisterErr(err)
	}
	if err := db.reg.RegisterFile(fd); err != nil {
		return db.handleRegisterErr(err)
	}
	return nil
}

func (db *db) handleRegisterErr(err error) error {
	if errors.Is(err, ErrNotLeader) {
		return err
	}
	if db.opts.ignoreProtoRegisterErrors {
		return nil
	}
	if db.opts.registerErrHandler != nil {
		return db.opts.registerErrHandler(err)
	}
	return err
}

func (db *db) load(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveDescriptors(ctx); err != nil && !errors.Is(err, ErrNotLeader) {
		return err
	}

	if err := db.loadDescriptors(ctx); err != nil && !errors.Is(err, ErrNotLeader) {
		return err
	}
	return nil
}

func (db *db) saveDescriptors(ctx context.Context) error {
	var fdps []*descriptorpb.FileDescriptorProto
	db.reg.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		// we can't register it now as the protogistry.globalMutex would dead lock
		fdps = append(fdps, pdesc.ToFileDescriptorProto(fd))
		return true
	})
	tx, err := newTx(context.Background(), db)
	if err != nil {
		return err
	}
	defer tx.Close()
	for _, v := range fdps {
		if err := db.registerFileDescriptorProto(ctx, v); err != nil {
			return err
		}
		b, err := db.marshal(v)
		if err != nil {
			return err
		}
		k := descriptorPrefix(v)
		g, err := tx.getRaw(k)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if bytes.Equal(g, b) {
			continue
		}
		if err := tx.setRaw(ctx, k, b); err != nil {
			return err
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		return err
	}
	return nil
}

func (db *db) loadDescriptors(ctx context.Context) error {
	tx, err := newTx(ctx, db, WithReadOnly())
	if err != nil {
		return err
	}
	defer tx.Close()
	txn := tx.txn
	it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(descriptors)})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := it.Item().Value(func(val []byte) error {
			fdp := &descriptorpb.FileDescriptorProto{}
			if err := db.unmarshal(val, fdp); err != nil {
				return err
			}
			return db.registerFileDescriptorProto(ctx, fdp)

		}); err != nil {
			return err
		}
	}
	return nil
}

func (db *db) recoverRegister(ctx context.Context, file *descriptorpb.FileDescriptorProto) error {
	tx, err := newTx(ctx, db)
	if err != nil {
		return err
	}
	txn := tx.txn
	key := descriptorPrefix(file)
	it := txn.NewIterator(badger.IteratorOptions{Prefix: key})
	defer it.Close()
	found := false
	equals := false
	for it.Rewind(); it.Valid(); it.Next() {
		if err := it.Item().Value(func(val []byte) error {
			got := &descriptorpb.FileDescriptorProto{}
			if err := proto.Unmarshal(val, got); err != nil {
				// TODO(adphi): should we delete the malformed FileDescriptorProto and silently continue ?
				return err
			}
			found = true
			if proto.Equal(got, file) {
				equals = true
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if equals {
		return nil
	}
	if found {
		return errors.New("updating definitions not yet supported")
	}
	b, err := proto.Marshal(file)
	if err != nil {
		return err
	}
	if err := tx.setRaw(ctx, key, b); err != nil {
		return err
	}
	it.Close()
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func applyDefaults(m proto.Message) {
	if v, ok := m.(interface{ Default() }); ok {
		v.Default()
	} else {
		defaults.Apply(m)
	}
}

func isAlreadyRegistered(r interface{}) (bool, error) {
	switch v := r.(type) {
	case string:
		return strings.Contains(v, "is already registered") || strings.Contains(v, "duplicate registration"), errors.New(strings.Split(v, "\n")[0])
	case error:
		return false, v
	default:
		return false, fmt.Errorf("%v", r)
	}
}
