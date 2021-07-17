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

	"github.com/dgraph-io/badger/v2"
	"go.linka.cloud/protofilters"
	"go.linka.cloud/protofilters/matcher"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"go.linka.cloud/protodb/pb"
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
	// TODO(adphi): load registered file descriptors
	return &db{bdb: bdb, opts: o}, nil
}

type db struct {
	bdb  *badger.DB
	opts options
}

func (d *db) Watch(ctx context.Context, m proto.Message, filters ...*protofilters.FieldFilter) (<-chan Event, error) {
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
							if err := proto.Unmarshal(val, o); err != nil {
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
					if err := proto.Unmarshal(v.Value, new); err != nil {
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
					was, err = matcher.MatchFilters(old, filters...)
					if err != nil {
						return err
					}
				}
				var is bool
				if new != nil {
					is, err = matcher.MatchFilters(new, filters...)
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

func (d *db) Register(ctx context.Context, file *descriptorpb.FileDescriptorProto) error {
	if file == nil || file.GetName() == "" {
		return errors.New("invalid file")
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
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (d *db) Get(ctx context.Context, m proto.Message, paging *Paging, filters ...*protofilters.FieldFilter) ([]proto.Message, *PagingInfo, error) {
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
	return newTx(ctx, d.bdb)
}

func (d *db) Close() error {
	return d.bdb.Close()
}

func defaults(m proto.Message) {
	if v, ok := m.(interface{ Default() }); ok {
		v.Default()
	}
}
