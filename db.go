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
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v2"
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
	bopts := o.build().WithNumVersionsToKeep(2).WithLogger(logger.C(ctx).WithField("service", "protodb"))
	bdb, err := badger.Open(bopts)
	if err != nil {
		return nil, err
	}
	// TODO(adphi): Use custom registry to avoid this hack
	if err := os.Setenv(protobufRegistrationConflictEnv, protobufRegistrationConflictPolicy); err != nil {
		return nil, err
	}
	reg := preg.GlobalFiles

	db := &db{bdb: bdb, opts: o, reg: reg, bopts: bopts}
	if err := db.load(); err != nil {
		return nil, err
	}
	return db, nil
}

type db struct {
	bdb   *badger.DB
	opts  options
	bopts badger.Options

	mu  sync.RWMutex
	reg *preg.Files
}

func (db *db) Watch(ctx context.Context, m proto.Message, opts ...GetOption) (<-chan Event, error) {
	o := makeGetOpts(opts...)

	k, _ := dataPrefix(m)
	ch := make(chan Event)
	go func() {
		defer close(ch)
		err := db.bdb.Subscribe(ctx, func(kv *badger.KVList) error {
			for _, v := range kv.Kv {
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
				if o.Filter == nil || o.Filter.Condition == nil {
					if new == nil {
						typ = pb.WatchEventLeave
					} else if old != nil {
						typ = pb.WatchEventUpdate
					} else {
						typ = pb.WatchEventEnter
					}
					ch <- event{typ: typ, old: old, new: new}
					return nil
				}
				var was bool
				if old != nil {
					was, err = pf.MatchExpression(old, o.Filter)
					if err != nil {
						return err
					}
				}
				var is bool
				if new != nil {
					is, err = pf.MatchExpression(new, o.Filter)
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
						continue
					}
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				ch <- event{new: new, old: old, typ: typ}
			}
			return nil
		}, k)
		ch <- event{err: err}
	}()
	return ch, nil
}

func (db *db) Get(ctx context.Context, m proto.Message, opts ...GetOption) ([]proto.Message, *PagingInfo, error) {
	end := metrics.Get.Start()
	defer end()
	tx, err := db.Tx(ctx)
	if err != nil {
		metrics.Get.ErrorsCounter.Inc()
		return nil, nil, err
	}
	defer tx.Close()
	msgs, paging, err := tx.Get(ctx, m, opts...)
	if err != nil {
		metrics.Get.ErrorsCounter.Inc()
	}
	return msgs, paging, err
}

func (db *db) Set(ctx context.Context, m proto.Message, opts ...SetOption) (proto.Message, error) {
	end := metrics.Set.Start()
	defer end()
	tx, err := db.Tx(ctx)
	if err != nil {
		metrics.Set.ErrorsCounter.Inc()
		return nil, err
	}
	defer tx.Close()
	m, err = tx.Set(ctx, m, opts...)
	if err != nil {
		metrics.Set.ErrorsCounter.Inc()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		metrics.Set.ErrorsCounter.Inc()
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		metrics.Set.ErrorsCounter.Inc()
		return nil, err
	}
	return m, nil
}

func (db *db) Delete(ctx context.Context, m proto.Message) error {
	end := metrics.Delete.Start()
	defer end()
	tx, err := db.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := tx.Delete(ctx, m); err != nil {
		metrics.Delete.ErrorsCounter.Inc()
		return err
	}
	if err := ctx.Err(); err != nil {
		metrics.Delete.ErrorsCounter.Inc()
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		metrics.Delete.ErrorsCounter.Inc()
		return err
	}
	return nil
}

func (db *db) Tx(ctx context.Context) (Tx, error) {
	return newTx(ctx, db)
}

func (db *db) Close() error {
	return db.bdb.Close()
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
	if err := db.registerFileDescriptorProto(file); err != nil {
		return err
	}
	txn := db.bdb.NewTransaction(true)
	defer txn.Discard()
	b, err := proto.Marshal(file)
	if err != nil {
		return err
	}
	if err := txn.Set(descriptorPrefix(file), b); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := txn.Commit(); err != nil {
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

func (db *db) registerFileDescriptorProto(file *descriptorpb.FileDescriptorProto) (err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			ok, err = isAlreadyRegistered(e)
			if ok {
				err = db.recoverRegister(file)
			}
		}
	}()
	fd, err := pdesc.NewFile(file, db.reg)
	if err != nil {
		return err
	}
	if err := db.reg.RegisterFile(fd); err != nil {
		return err
	}
	return nil
}

func (db *db) load() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	var fdps []*descriptorpb.FileDescriptorProto
	db.reg.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		// we can't register it now as the protogistry.globalMutex would dead lock
		fdps = append(fdps, pdesc.ToFileDescriptorProto(fd))
		return true
	})
	for _, v := range fdps {
		if err := db.registerFileDescriptorProto(v); err != nil {
			return err
		}
	}
	return db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(descriptors)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				fdp := &descriptorpb.FileDescriptorProto{}
				if err := db.unmarshal(val, fdp); err != nil {
					return err
				}
				return db.registerFileDescriptorProto(fdp)

			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *db) recoverRegister(file *descriptorpb.FileDescriptorProto) error {
	return db.bdb.Update(func(txn *badger.Txn) error {
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
		return txn.Set(key, b)
	})
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
