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
	pf "go.linka.cloud/protofilters"
	"go.linka.cloud/protofilters/filters"
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
	bdb, err := badger.Open(o.build().WithNumVersionsToKeep(2))
	if err != nil {
		return nil, err
	}
	// TODO(adphi): Use custom registry to avoid this hack
	if err := os.Setenv(protobufRegistrationConflictEnv, protobufRegistrationConflictPolicy); err != nil {
		return nil, err
	}
	reg := preg.GlobalFiles

	db := &db{bdb: bdb, opts: o, reg: reg}
	if err := db.load(); err != nil {
		return nil, err
	}
	return db, nil
}

type db struct {
	bdb  *badger.DB
	opts options

	mu  sync.RWMutex
	reg *preg.Files
}

func (d *db) Watch(ctx context.Context, m proto.Message, filters ...*filters.FieldFilter) (<-chan Event, error) {
	k := dataPrefix(m)
	ch := make(chan Event)
	go func() {
		defer close(ch)
		err := d.bdb.Subscribe(ctx, func(kv *badger.KVList) error {
			for _, v := range kv.Kv {
				var err error
				var typ EventType
				var new proto.Message
				var old proto.Message
				if err := d.bdb.View(func(txn *badger.Txn) error {
					it := txn.NewKeyIterator(v.Key, badger.IteratorOptions{AllVersions: true, PrefetchValues: false})
					defer it.Close()
					for it.Rewind(); it.Valid(); it.Next() {
						item := it.Item()
						if item.Version() == v.Version {
							continue
						}
						return item.Value(func(val []byte) error {
							o := m.ProtoReflect().New().Interface()
							if err := d.unmarshal(val, o); err != nil {
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
					if err := d.unmarshal(v.Value, new); err != nil {
						return err
					}
				}
				if len(filters) == 0 {
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
					was, err = pf.MatchFilters(old, filters...)
					if err != nil {
						return err
					}
				}
				var is bool
				if new != nil {
					is, err = pf.MatchFilters(new, filters...)
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

func (d *db) Get(ctx context.Context, m proto.Message, paging *Paging, filters ...*filters.FieldFilter) ([]proto.Message, *PagingInfo, error) {
	tx, err := d.Tx(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Close()
	return tx.Get(ctx, m, paging, filters...)
}

func (d *db) Put(ctx context.Context, m proto.Message) (proto.Message, error) {
	opts, _ := optsFromCtx(ctx)
	if opts.applyDefaults {
		defaults(m)
	}
	tx, err := d.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	m, err = tx.Put(ctx, m)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return m, tx.Commit(ctx)
}

func (d *db) Delete(ctx context.Context, m proto.Message) error {
	tx, err := d.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	if err := tx.Delete(ctx, m); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *db) Tx(ctx context.Context) (Tx, error) {
	ctx = ctxWithOpts(ctx, d.opts)
	return newTx(ctx, d)
}

func (d *db) Close() error {
	return d.bdb.Close()
}

func (d *db) unmarshal(b []byte, m proto.Message) error {
	switch v := interface{}(m).(type) {
	case interface{ UnmarshalVT(b []byte) error }:
		return v.UnmarshalVT(b)
	case interface{ Unmarshal(b []byte) error }:
		return v.Unmarshal(b)
	default:
		return proto.UnmarshalOptions{}.Unmarshal(b, m)
	}
}

func (d *db) marshal(m proto.Message) ([]byte, error) {
	switch v := interface{}(m).(type) {
	case interface{ MarshalVT() ([]byte, error) }:
		return v.MarshalVT()
	case interface{ Marshal() ([]byte, error) }:
		return v.Marshal()
	default:
		return proto.MarshalOptions{}.Marshal(m)
	}
}

func (d *db) RegisterProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) error {
	if file == nil || file.GetName() == "" {
		return errors.New("invalid file")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.registerFileDescriptorProto(file); err != nil {
		return err
	}
	txn := d.bdb.NewTransaction(true)
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

func (d *db) Register(ctx context.Context, fd protoreflect.FileDescriptor) error {
	return d.RegisterProto(ctx, pdesc.ToFileDescriptorProto(fd))
}

func (d *db) registerFileDescriptorProto(file *descriptorpb.FileDescriptorProto) (err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			ok, err = isAlreadyRegistered(e)
			if ok {
				err = d.recoverRegister(file)
			}
		}
	}()
	fd, err := pdesc.NewFile(file, d.reg)
	if err != nil {
		return err
	}
	if err := d.reg.RegisterFile(fd); err != nil {
		return err
	}
	return nil
}

func (d *db) load() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var fdps []*descriptorpb.FileDescriptorProto
	d.reg.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		// we can't register it now as the protogistry.globalMutex would dead lock
		fdps = append(fdps, pdesc.ToFileDescriptorProto(fd))
		return true
	})
	for _, v := range fdps {
		if err := d.registerFileDescriptorProto(v); err != nil {
			return err
		}
	}
	return d.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(descriptors)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				fdp := &descriptorpb.FileDescriptorProto{}
				if err := d.unmarshal(val, fdp); err != nil {
					return err
				}
				return d.registerFileDescriptorProto(fdp)

			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *db) recoverRegister(file *descriptorpb.FileDescriptorProto) error {
	return d.bdb.Update(func(txn *badger.Txn) error {
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

func defaults(m proto.Message) {
	if v, ok := m.(interface{ Default() }); ok {
		v.Default()
	}
}

func isAlreadyRegistered(r interface{}) (bool, error) {
	switch v := r.(type) {
	case string:
		return strings.Contains(v, "is already registered"), errors.New(strings.Split(v, "\n")[0])
	case error:
		return false, v
	default:
		return false, fmt.Errorf("%v", r)
	}
}
