package testpb

import (
	"context"
	"fmt"

	"go.uber.org/multierr"

	"go.linka.cloud/protodb"
	pdbc "go.linka.cloud/protodb/client"
)

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
}

type InterfaceTx interface {
	InterfaceReader
	InterfaceWriter
	protodb.Committer
	protodb.Sizer
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

func NewInterfaceStore(db pdbc.Client) InterfaceStore {
	return &_InterfaceDB{db: db}
}

type _InterfaceDB struct {
	db pdbc.Client
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
