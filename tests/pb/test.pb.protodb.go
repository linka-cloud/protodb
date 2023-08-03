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

// Code generated by protoc-gen-defaults. DO NOT EDIT.

package testpb

import (
	"context"
	"fmt"

	"go.linka.cloud/protodb"
	"go.uber.org/multierr"
)

var (
	_ MessageWithKeyOptionStore = (*_MessageWithKeyOptionDB)(nil)
	_ MessageWithKeyOptionTx    = (*_MessageWithKeyOptionTx)(nil)
)

type MessageWithKeyOptionStore interface {
	MessageWithKeyOptionReader
	MessageWithKeyOptionWriter
	MessageWithKeyOptionWatcher
	MessageWithKeyOptionTxProvider

	Register(ctx context.Context) error

	Raw() protodb.Client
}

type MessageWithKeyOptionTx interface {
	protodb.Committer
	protodb.Sizer
	MessageWithKeyOptionReader
	MessageWithKeyOptionWriter
	Raw() protodb.Tx
}

type MessageWithKeyOptionReader interface {
	Get(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.GetOption) ([]*MessageWithKeyOption, *protodb.PagingInfo, error)
}

type MessageWithKeyOptionWatcher interface {
	Watch(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.GetOption) (<-chan MessageWithKeyOptionEvent, error)
}

type MessageWithKeyOptionWriter interface {
	Set(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.SetOption) (*MessageWithKeyOption, error)
	Delete(ctx context.Context, m *MessageWithKeyOption) error
}

type MessageWithKeyOptionTxProvider interface {
	Tx(ctx context.Context) (MessageWithKeyOptionTx, error)
}

type MessageWithKeyOptionEvent interface {
	Type() protodb.EventType
	Old() *MessageWithKeyOption
	New() *MessageWithKeyOption
	Err() error
}

func NewMessageWithKeyOptionStore(db protodb.Client) MessageWithKeyOptionStore {
	return &_MessageWithKeyOptionDB{db: db}
}

type _MessageWithKeyOptionDB struct {
	db protodb.Client
}

func (s *_MessageWithKeyOptionDB) Raw() protodb.Client {
	return s.db
}

func (s *_MessageWithKeyOptionDB) Register(ctx context.Context) error {
	return s.db.Register(ctx, (&MessageWithKeyOption{}).ProtoReflect().Descriptor().ParentFile())
}

func (s *_MessageWithKeyOptionDB) Get(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.GetOption) ([]*MessageWithKeyOption, *protodb.PagingInfo, error) {
	return getMessageWithKeyOption(ctx, s.db, m, opts...)
}

func (s *_MessageWithKeyOptionDB) Set(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.SetOption) (*MessageWithKeyOption, error) {
	return setMessageWithKeyOption(ctx, s.db, m, opts...)
}

func (s *_MessageWithKeyOptionDB) Delete(ctx context.Context, m *MessageWithKeyOption) error {
	return deleteMessageWithKeyOption(ctx, s.db, m)
}

func (s *_MessageWithKeyOptionDB) Watch(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.GetOption) (<-chan MessageWithKeyOptionEvent, error) {
	out := make(chan MessageWithKeyOptionEvent)
	ch, err := s.db.Watch(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for e := range ch {
			ev := &_MessageWithKeyOptionEvent{typ: e.Type(), err: e.Err()}
			if n := e.New(); n != nil {
				v, ok := n.(*MessageWithKeyOption)
				if !ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for new MessageWithKeyOption: %T", n))
				} else {
					ev.new = v
				}
			}
			if o := e.Old(); o != nil {
				v, ok := o.(*MessageWithKeyOption)
				if !ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for old MessageWithKeyOption: %T", o))
				} else {
					ev.old = v
				}
			}
			out <- ev
		}
	}()
	return out, nil
}

func (s *_MessageWithKeyOptionDB) Tx(ctx context.Context) (MessageWithKeyOptionTx, error) {
	txn, err := s.db.Tx(ctx)
	return &_MessageWithKeyOptionTx{txn: txn}, err
}

func getMessageWithKeyOption(ctx context.Context, r protodb.Reader, m *MessageWithKeyOption, opts ...protodb.GetOption) ([]*MessageWithKeyOption, *protodb.PagingInfo, error) {
	ms, i, err := r.Get(ctx, m, opts...)
	if err != nil {
		return nil, nil, err
	}
	var out []*MessageWithKeyOption
	for _, v := range ms {
		vv, ok := v.(*MessageWithKeyOption)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected type for MessageWithKeyOption: %T", v)
		}
		out = append(out, vv)
	}
	return out, i, nil
}

func setMessageWithKeyOption(ctx context.Context, w protodb.Writer, m *MessageWithKeyOption, opts ...protodb.SetOption) (*MessageWithKeyOption, error) {
	v, err := w.Set(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	vv, ok := v.(*MessageWithKeyOption)
	if !ok {
		return nil, fmt.Errorf("unexpected type for MessageWithKeyOption: %T", v)
	}
	return vv, nil
}

func deleteMessageWithKeyOption(ctx context.Context, w protodb.Writer, m *MessageWithKeyOption) error {
	return w.Delete(ctx, m)
}

func NewMessageWithKeyOptionTx(tx protodb.Tx) MessageWithKeyOptionTx {
	return &_MessageWithKeyOptionTx{txn: tx}
}

type _MessageWithKeyOptionTx struct {
	txn protodb.Tx
}

func (s *_MessageWithKeyOptionTx) Raw() protodb.Tx {
	return s.txn
}

func (s *_MessageWithKeyOptionTx) Get(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.GetOption) ([]*MessageWithKeyOption, *protodb.PagingInfo, error) {
	return getMessageWithKeyOption(ctx, s.txn, m, opts...)
}

func (s *_MessageWithKeyOptionTx) Set(ctx context.Context, m *MessageWithKeyOption, opts ...protodb.SetOption) (*MessageWithKeyOption, error) {
	return setMessageWithKeyOption(ctx, s.txn, m, opts...)
}

func (s *_MessageWithKeyOptionTx) Delete(ctx context.Context, m *MessageWithKeyOption) error {
	return deleteMessageWithKeyOption(ctx, s.txn, m)
}

func (s *_MessageWithKeyOptionTx) Commit(ctx context.Context) error {
	return s.txn.Commit(ctx)
}

func (s *_MessageWithKeyOptionTx) Close() {
	s.txn.Close()
}

func (s *_MessageWithKeyOptionTx) Count() (int64, error) {
	return s.txn.Count()
}

func (s *_MessageWithKeyOptionTx) Size() (int64, error) {
	return s.txn.Size()
}

type _MessageWithKeyOptionEvent struct {
	typ protodb.EventType
	old *MessageWithKeyOption
	new *MessageWithKeyOption
	err error
}

func (e *_MessageWithKeyOptionEvent) Type() protodb.EventType {
	return e.typ
}

func (e *_MessageWithKeyOptionEvent) Old() *MessageWithKeyOption {
	return e.old
}

func (e *_MessageWithKeyOptionEvent) New() *MessageWithKeyOption {
	return e.new
}

func (e *_MessageWithKeyOptionEvent) Err() error {
	return e.err
}

var (
	_ InterfaceStore = (*_InterfaceDB)(nil)
	_ InterfaceTx    = (*_InterfaceTx)(nil)
)

type InterfaceStore interface {
	InterfaceReader
	InterfaceWriter
	InterfaceWatcher
	InterfaceTxProvider

	Register(ctx context.Context) error

	Raw() protodb.Client
}

type InterfaceTx interface {
	protodb.Committer
	protodb.Sizer
	InterfaceReader
	InterfaceWriter
	Raw() protodb.Tx
}

type InterfaceReader interface {
	Get(ctx context.Context, m *Interface, opts ...protodb.GetOption) ([]*Interface, *protodb.PagingInfo, error)
}

type InterfaceWatcher interface {
	Watch(ctx context.Context, m *Interface, opts ...protodb.GetOption) (<-chan InterfaceEvent, error)
}

type InterfaceWriter interface {
	Set(ctx context.Context, m *Interface, opts ...protodb.SetOption) (*Interface, error)
	Delete(ctx context.Context, m *Interface) error
}

type InterfaceTxProvider interface {
	Tx(ctx context.Context) (InterfaceTx, error)
}

type InterfaceEvent interface {
	Type() protodb.EventType
	Old() *Interface
	New() *Interface
	Err() error
}

func NewInterfaceStore(db protodb.Client) InterfaceStore {
	return &_InterfaceDB{db: db}
}

type _InterfaceDB struct {
	db protodb.Client
}

func (s *_InterfaceDB) Raw() protodb.Client {
	return s.db
}

func (s *_InterfaceDB) Register(ctx context.Context) error {
	return s.db.Register(ctx, (&Interface{}).ProtoReflect().Descriptor().ParentFile())
}

func (s *_InterfaceDB) Get(ctx context.Context, m *Interface, opts ...protodb.GetOption) ([]*Interface, *protodb.PagingInfo, error) {
	return getInterface(ctx, s.db, m, opts...)
}

func (s *_InterfaceDB) Set(ctx context.Context, m *Interface, opts ...protodb.SetOption) (*Interface, error) {
	return setInterface(ctx, s.db, m, opts...)
}

func (s *_InterfaceDB) Delete(ctx context.Context, m *Interface) error {
	return deleteInterface(ctx, s.db, m)
}

func (s *_InterfaceDB) Watch(ctx context.Context, m *Interface, opts ...protodb.GetOption) (<-chan InterfaceEvent, error) {
	out := make(chan InterfaceEvent)
	ch, err := s.db.Watch(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for e := range ch {
			ev := &_InterfaceEvent{typ: e.Type(), err: e.Err()}
			if n := e.New(); n != nil {
				v, ok := n.(*Interface)
				if !ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for new Interface: %T", n))
				} else {
					ev.new = v
				}
			}
			if o := e.Old(); o != nil {
				v, ok := o.(*Interface)
				if !ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for old Interface: %T", o))
				} else {
					ev.old = v
				}
			}
			out <- ev
		}
	}()
	return out, nil
}

func (s *_InterfaceDB) Tx(ctx context.Context) (InterfaceTx, error) {
	txn, err := s.db.Tx(ctx)
	return &_InterfaceTx{txn: txn}, err
}

func getInterface(ctx context.Context, r protodb.Reader, m *Interface, opts ...protodb.GetOption) ([]*Interface, *protodb.PagingInfo, error) {
	ms, i, err := r.Get(ctx, m, opts...)
	if err != nil {
		return nil, nil, err
	}
	var out []*Interface
	for _, v := range ms {
		vv, ok := v.(*Interface)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected type for Interface: %T", v)
		}
		out = append(out, vv)
	}
	return out, i, nil
}

func setInterface(ctx context.Context, w protodb.Writer, m *Interface, opts ...protodb.SetOption) (*Interface, error) {
	v, err := w.Set(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	vv, ok := v.(*Interface)
	if !ok {
		return nil, fmt.Errorf("unexpected type for Interface: %T", v)
	}
	return vv, nil
}

func deleteInterface(ctx context.Context, w protodb.Writer, m *Interface) error {
	return w.Delete(ctx, m)
}

func NewInterfaceTx(tx protodb.Tx) InterfaceTx {
	return &_InterfaceTx{txn: tx}
}

type _InterfaceTx struct {
	txn protodb.Tx
}

func (s *_InterfaceTx) Raw() protodb.Tx {
	return s.txn
}

func (s *_InterfaceTx) Get(ctx context.Context, m *Interface, opts ...protodb.GetOption) ([]*Interface, *protodb.PagingInfo, error) {
	return getInterface(ctx, s.txn, m, opts...)
}

func (s *_InterfaceTx) Set(ctx context.Context, m *Interface, opts ...protodb.SetOption) (*Interface, error) {
	return setInterface(ctx, s.txn, m, opts...)
}

func (s *_InterfaceTx) Delete(ctx context.Context, m *Interface) error {
	return deleteInterface(ctx, s.txn, m)
}

func (s *_InterfaceTx) Commit(ctx context.Context) error {
	return s.txn.Commit(ctx)
}

func (s *_InterfaceTx) Close() {
	s.txn.Close()
}

func (s *_InterfaceTx) Count() (int64, error) {
	return s.txn.Count()
}

func (s *_InterfaceTx) Size() (int64, error) {
	return s.txn.Size()
}

type _InterfaceEvent struct {
	typ protodb.EventType
	old *Interface
	new *Interface
	err error
}

func (e *_InterfaceEvent) Type() protodb.EventType {
	return e.typ
}

func (e *_InterfaceEvent) Old() *Interface {
	return e.old
}

func (e *_InterfaceEvent) New() *Interface {
	return e.new
}

func (e *_InterfaceEvent) Err() error {
	return e.err
}
