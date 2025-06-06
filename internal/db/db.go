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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v3"
	bpb "github.com/dgraph-io/badger/v3/pb"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/protoc-gen-defaults/defaults"
	pf "go.linka.cloud/protofilters"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pdesc "google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	preg "google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"go.linka.cloud/protodb/internal/badgerd"
	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/client"
	"go.linka.cloud/protodb/internal/mutex"
	"go.linka.cloud/protodb/internal/protodb"
	"go.linka.cloud/protodb/internal/server"
	"go.linka.cloud/protodb/pb"
)

var tracer = otel.Tracer("protodb")

const (
	protobufRegistrationConflictEnv    = "GOLANG_PROTOBUF_REGISTRATION_CONFLICT"
	protobufRegistrationConflictPolicy = "ignore"
)

func Open(ctx context.Context, opts ...Option) (protodb.DB, error) {
	ctx, span := tracer.Start(ctx, "Open")
	defer span.End()
	o := defaultOptions
	for _, v := range opts {
		v(&o)
	}
	if o.logger == nil {
		o.logger = logger.C(ctx).WithField("service", "protodb")
	}
	// TODO(adphi): Use custom registry to avoid this hack
	if err := os.Setenv(protobufRegistrationConflictEnv, protobufRegistrationConflictPolicy); err != nil {
		return nil, err
	}
	freg := preg.GlobalFiles
	treg := preg.GlobalTypes

	db := &db{opts: o, freg: freg, treg: treg, smu: mutex.NewKV()}
	if o.repl != nil {
		h, err := server.NewServer(db)
		if err != nil {
			return nil, err
		}
		o.repl = append(o.repl, replication.WithExtraServices(func(r grpc.ServiceRegistrar) {
			pb.RegisterProtoDBServer(r, h)
		}))
	}
	var err error
	db.bdb, err = badgerd.Open(
		ctx,
		badgerd.WithLogger(o.logger),
		badgerd.WithInMemory(o.inMemory),
		badgerd.WithPath(o.path),
		badgerd.WithBadgerOptionsFunc(o.badgerOptionsFunc),
		badgerd.WithReplication(o.repl...),
	)
	if err != nil {
		return nil, err
	}

	ctx, db.scancel = context.WithCancel(ctx)
	go func() {
		if err := db.bdb.Subscribe(ctx, func(_ *badger.KVList) error {
			if err := db.loadDescriptors(ctx); err != nil {
				logger.C(ctx).WithError(err).Errorf("failed to reload descriptors")
			}
			return nil
		}, []bpb.Match{{Prefix: []byte(protodb.Descriptors + "/")}}); err != nil && !errors.Is(err, context.Canceled) {
			logger.C(ctx).WithError(err).Errorf("failed to watch schema changes")
		}
	}()

	if db.bdb.Replicated() {
		if err = db.loadDescriptors(ctx); err != nil {
			return nil, multierr.Combine(err, db.bdb.Close())
		}
		return db, nil
	}
	if err = db.load(ctx); err != nil {
		return nil, multierr.Combine(err, db.bdb.Close())
	}
	return db, nil
}

type db struct {
	bdb  badgerd.DB
	opts options

	mu   sync.RWMutex
	freg *preg.Files
	treg *preg.Types
	smu  *mutex.KV
	// repl replication.Replication

	cmu     sync.RWMutex
	scancel context.CancelFunc
	close   bool
}

func (db *db) IsLeader() bool {
	return db.bdb.IsLeader()
}

func (db *db) Leader() string {
	return db.bdb.Leader()
}

func (db *db) LeaderChanges() <-chan string {
	return db.bdb.LeaderChanges()
}

func (db *db) Watch(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (<-chan protodb.Event, error) {
	ctx, span := tracer.Start(ctx, "Watch")
	defer span.End()
	end := metrics.Watch.Start(string(m.ProtoReflect().Descriptor().FullName()))
	c, ok, err := db.maybeProxy(true)
	if err != nil {
		end.End()
		return nil, err
	}
	if ok {
		defer end.End()
		return c.Watch(ctx, m, opts...)
	}

	o := makeGetOpts(opts...)

	k, _, _, _ := protodb.DataPrefix(m)
	log := logger.C(ctx).WithFields("service", "protodb", "action", "watch", "key", string(k))
	log.Debugf("start watching for key %s", string(k))
	ch := make(chan protodb.Event, 1)
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
				var typ protodb.EventType
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
		if err != nil && !errors.Is(err, context.Canceled) {
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

func (db *db) Get(ctx context.Context, m proto.Message, opts ...protodb.GetOption) ([]proto.Message, *protodb.PagingInfo, error) {
	ctx, span := tracer.Start(ctx, "Get", trace.WithAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName()))))
	defer span.End()
	defer metrics.Get.Start(string(m.ProtoReflect().Descriptor().FullName())).End()

	c, ok, err := db.maybeProxy(true)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return c.Get(ctx, m, opts...)
	}

	tx, err := db.Tx(ctx, protodb.WithReadOnly())
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

func (db *db) GetOne(ctx context.Context, m proto.Message, opts ...protodb.GetOption) (proto.Message, bool, error) {
	return protodb.GetOne(ctx, db, m, opts...)
}

func (db *db) Set(ctx context.Context, m proto.Message, opts ...protodb.SetOption) (proto.Message, error) {
	ctx, span := tracer.Start(ctx, "Set", trace.WithAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName()))))
	defer span.End()
	defer metrics.Set.Start(string(m.ProtoReflect().Descriptor().FullName())).End()

	c, ok, err := db.maybeProxy(false)
	if err != nil {
		return nil, err
	}
	if ok {
		return c.Set(ctx, m, opts...)
	}

	tx, err := db.Tx(ctx)
	if err != nil {
		metrics.Set.ErrorsCounter.WithLabelValues(string(m.ProtoReflect().Descriptor().FullName())).Inc()
		return nil, err
	}
	defer tx.Close()
	out, err := tx.Set(ctx, m, opts...)
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
	return out, nil
}

func (db *db) Delete(ctx context.Context, m proto.Message) error {
	ctx, span := tracer.Start(ctx, "Delete", trace.WithAttributes(attribute.String("message", string(m.ProtoReflect().Descriptor().FullName()))))
	defer span.End()
	defer metrics.Delete.Start(string(m.ProtoReflect().Descriptor().FullName())).End()

	c, ok, err := db.maybeProxy(false)
	if err != nil {
		return err
	}
	if ok {
		return c.Delete(ctx, m)
	}

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

func (db *db) Tx(ctx context.Context, opts ...protodb.TxOption) (protodb.Tx, error) {
	ctx, span := tracer.Start(ctx, "Tx")
	o := protodb.TxOpts{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.ReadOnly {
		span.SetAttributes(attribute.Bool("read_only", true))
	}
	c, ok, err := db.maybeProxy(o.ReadOnly)
	if err != nil {
		return nil, err
	}
	if ok {
		return c.Tx(ctx, opts...)
	}
	return newTx(ctx, db, opts...)
}

func (db *db) NextSeq(ctx context.Context, name string) (uint64, error) {
	ctx, span := tracer.Start(ctx, "NextSeq", trace.WithAttributes(attribute.String("name", name)))
	defer span.End()
	if name == "" {
		return 0, badger.ErrEmptyKey
	}
	c, ok, err := db.maybeProxy(false)
	if err != nil {
		return 0, err
	}
	if ok {
		return c.NextSeq(ctx, name)
	}
	db.smu.Lock(name)
	defer db.smu.Unlock(name)
	var seq uint64
	txn, err := db.bdb.NewTransaction(ctx, true)
	if err != nil {
		return 0, err
	}
	defer txn.Close(ctx)
	k := []byte(protodb.SeqKey(name))
	i, err := txn.Get(ctx, k)
	found := !errors.Is(err, badger.ErrKeyNotFound)
	if err != nil && found {
		return 0, err
	}
	b := make([]byte, 8)
	if found {
		b, err = i.ValueCopy(b)
		if err != nil {
			return 0, err
		}
		if len(b) != 8 {
			return 0, fmt.Errorf("invalid seq value: %v", b)
		}
		seq = binary.BigEndian.Uint64(b)
	}
	seq++
	binary.BigEndian.PutUint64(b, seq)
	if err := txn.Set(ctx, k, b, 0); err != nil {
		return 0, err
	}
	if err := txn.Commit(ctx); err != nil {
		return 0, err
	}
	return seq, nil
}

func (db *db) Close() (err error) {
	if db.closed() {
		return nil
	}
	db.cmu.Lock()
	db.close = true
	db.cmu.Unlock()
	db.scancel()
	err = multierr.Append(err, db.bdb.Close())
	if db.opts.onClose != nil {
		db.opts.onClose()
	}
	return err
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
	ctx, span := tracer.Start(ctx, "RegisterProto", trace.WithAttributes(attribute.String("file", file.GetName())))
	defer span.End()
	if file == nil || file.GetName() == "" {
		return errors.New("invalid file")
	}

	c, ok, err := db.maybeProxy(false)
	if err != nil {
		return err
	}
	if ok {
		if err := c.RegisterProto(ctx, file); err != nil {
			return err
		}
		return db.loadDescriptors(ctx)
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
	if err := txn.setRaw(ctx, protodb.DescriptorPrefix(file), b); err != nil {
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
	return db.freg
}

func (db *db) Descriptors(ctx context.Context) ([]*descriptorpb.DescriptorProto, error) {
	ctx, span := tracer.Start(ctx, "Descriptors")
	defer span.End()
	c, ok, err := db.maybeProxy(true)
	if err != nil {
		return nil, err
	}
	if ok {
		return c.Descriptors(ctx)
	}

	m := make(map[string]*descriptorpb.FileDescriptorProto)
	if err := db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(protodb.Descriptors)})
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

func (db *db) FileDescriptors(ctx context.Context) ([]*descriptorpb.FileDescriptorProto, error) {
	ctx, span := tracer.Start(ctx, "FileDescriptors")
	defer span.End()
	c, ok, err := db.maybeProxy(true)
	if err != nil {
		return nil, err
	}
	if ok {
		return c.FileDescriptors(ctx)
	}

	var out []*descriptorpb.FileDescriptorProto
	if err := db.bdb.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(protodb.Descriptors)})
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

// func (db *db) Backup(ctx context.Context, w io.Writer) error {
// 	// TODO(adphi): add to service and proxy
// 	_, err := db.bdb.NewStream().Backup(w, 0)
// 	return err
// }

func (db *db) registerFileDescriptorProto(ctx context.Context, file *descriptorpb.FileDescriptorProto) (err error) {
	defer func() {
		if e := recover(); e != nil {
			// var ok bool
			// ok, err = isAlreadyRegistered(e)
			// if ok {
			// 	err = db.recoverRegister(ctx, file)
			// }
			if err != nil {
				err = db.handleRegisterErr(err)
			}
		}
	}()
	fd, err := pdesc.NewFile(file, db.freg)
	if err != nil {
		return db.handleRegisterErr(err)
	}
	if d, err := db.freg.FindFileByPath(fd.Path()); err == nil {
		if proto.Equal(pdesc.ToFileDescriptorProto(d), file) {
			return nil
		}
	}
	if err := db.freg.RegisterFile(fd); err != nil {
		return db.handleRegisterErr(err)
	}
	for i := 0; i < fd.Messages().Len(); i++ {
		m := fd.Messages().Get(i)
		if err := db.treg.RegisterMessage(dynamicpb.NewMessageType(m)); err != nil {
			return db.handleRegisterErr(err)
		}
	}
	return nil
}

func (db *db) handleRegisterErr(err error) error {
	if errors.Is(err, protodb.ErrNotLeader) {
		return err
	}
	// ignore gogoproto errors
	if strings.Contains(err.Error(), `"gogoproto/gogo.proto"`) {
		return nil
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

	if err := db.saveDescriptors(ctx); err != nil && !errors.Is(err, protodb.ErrNotLeader) {
		return err
	}

	if err := db.loadDescriptors(ctx); err != nil && !errors.Is(err, protodb.ErrNotLeader) {
		return err
	}
	return nil
}

func (db *db) saveDescriptors(ctx context.Context) error {
	var fdps []*descriptorpb.FileDescriptorProto
	db.freg.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
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
		k := protodb.DescriptorPrefix(v)
		g, err := tx.getRaw(ctx, k)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
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
	tx, err := newTx(ctx, db, protodb.WithReadOnly())
	if err != nil {
		return err
	}
	defer tx.Close()
	txn := tx.txn
	it := txn.Iterator(badger.IteratorOptions{Prefix: []byte(protodb.Descriptors)})
	defer it.Close()
	var errs error
	for it.Rewind(); it.Valid(); it.Next() {
		if err := it.Item().Value(func(val []byte) error {
			fdp := &descriptorpb.FileDescriptorProto{}
			if err := db.unmarshal(val, fdp); err != nil {
				return err
			}
			return db.registerFileDescriptorProto(ctx, fdp)

		}); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

func (db *db) recoverRegister(ctx context.Context, file *descriptorpb.FileDescriptorProto) error {
	tx, err := newTx(ctx, db)
	if err != nil {
		return err
	}
	txn := tx.txn
	key := protodb.DescriptorPrefix(file)
	it := txn.Iterator(badger.IteratorOptions{Prefix: key})
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

func (db *db) maybeProxy(read bool) (client.Client, bool, error) {
	if !db.bdb.Replicated() {
		return nil, false, nil
	}
	if db.bdb.IsLeader() || (db.bdb.LinearizableReads() && read) {
		return nil, false, nil
	}
	cc, ok := db.bdb.LeaderConn()
	if !ok {
		return nil, false, protodb.ErrNoLeaderConn
	}
	c, err := client.NewClient(cc)
	if err != nil {
		return nil, false, err
	}
	return c, true, nil
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
