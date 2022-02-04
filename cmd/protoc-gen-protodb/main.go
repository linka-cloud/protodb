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

package main

import (
	"text/template"

	pgs "github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
	"google.golang.org/protobuf/types/pluginpb"

	"go.linka.cloud/protodb/protodb"
)

func module() *mod {
	return &mod{
		ModuleBase: &pgs.ModuleBase{},
		imports:    make(map[string]struct{}),
		oneOfs:     make(map[string]struct{}),
	}
}

type mod struct {
	*pgs.ModuleBase
	ctx     pgsgo.Context
	tpl     *template.Template
	imports map[string]struct{}
	oneOfs  map[string]struct{}
}

func (p *mod) Name() string {
	return "protodb"
}

func (p *mod) InitContext(c pgs.BuildContext) {
	p.ModuleBase.InitContext(c)
	p.ctx = pgsgo.InitContext(c.Parameters())

	tpl := template.New("protodb").Funcs(map[string]interface{}{
		"package": p.ctx.PackageName,
		"name":    p.ctx.Name,
		"enabled": func(m pgs.Message) bool {
			var enabled bool
			m.Extension(protodb.E_Enabled, &enabled)
			return enabled
		},
	})
	p.tpl = template.Must(tpl.Parse(protodbTpl))
}

func (p *mod) Execute(targets map[string]pgs.File, _ map[string]pgs.Package) []pgs.Artifact {
	for _, f := range targets {
		var msgs []pgs.Message
		for _, m := range f.AllMessages() {
			var enabled bool
			ok, err := m.Extension(protodb.E_Enabled, &enabled)
			if err != nil {
				p.Fail(err)
			}
			if !ok || !enabled {
				continue
			}
			msgs = append(msgs, m)
		}
		p.generate(f, msgs)
	}
	return p.Artifacts()
}

func (p *mod) generate(f pgs.File, msgs []pgs.Message) {
	if len(msgs) == 0 {
		return
	}
	name := p.ctx.OutputPath(f).SetExt(".protodb.go")
	p.AddGeneratorTemplateFile(name.String(), p.tpl, f)
}

const protodbTpl = `package {{ package . }}

import (
	"context"
	"fmt"

	"go.linka.cloud/protodb"
	pdbc "go.linka.cloud/protodb/client"
	"go.uber.org/multierr"
)

{{ range .AllMessages }}

{{ if enabled . }}

var (
	_ {{ name . }}Store = (*_{{ name .}}DB)(nil)
	_ {{ name . }}Tx = (*_{{ name .}}Tx)(nil)
)

type {{ name . }}Store interface {
	{{ name . }}Reader
	{{ name . }}Writer
	{{ name . }}Watcher
	{{ name . }}TxProvider

	Register(ctx context.Context) error
}

type {{ name . }}Tx interface {
	{{ name . }}Reader
	{{ name . }}Writer
	protodb.Committer
	protodb.Sizer
}

type {{ name . }}Reader interface {
	Get(ctx context.Context, m *{{ name . }}, opts ...protodb.GetOption) ([]*{{ name . }}, *protodb.PagingInfo, error)
}

type {{ name . }}Watcher interface {
	Watch(ctx context.Context, m *{{ name . }}, opts ...protodb.GetOption) (<-chan {{ name . }}Event, error)
}

type {{ name . }}Writer interface {
	Set(ctx context.Context, m *{{ name . }}, opts ...protodb.SetOption) (*{{ name . }}, error)
	Delete(ctx context.Context, m *{{ name . }}) error
}

type {{ name . }}TxProvider interface {
	Tx(ctx context.Context) ({{ name . }}Tx, error)
}

type {{ name . }}Event interface {
	Type() protodb.EventType
	Old() *{{ name . }}
	New() *{{ name . }}
	Err() error
}

func New{{ name . }}Store(db pdbc.Client) {{ name . }}Store {
	return &_{{ name . }}DB{db: db}
}

type _{{ name . }}DB struct {
	db pdbc.Client
}

func (s *_{{ name . }}DB) Register(ctx context.Context) error {
	return s.db.Register(ctx, (&{{ name . }}{}).ProtoReflect().Descriptor().ParentFile())
}

func (s *_{{ name . }}DB) Get(ctx context.Context, m *{{ name . }}, opts ...protodb.GetOption) ([]*{{ name . }}, *protodb.PagingInfo, error) {
	return get{{ name . }}(ctx, s.db, m, opts...)
}

func (s *_{{ name . }}DB) Set(ctx context.Context, m *{{ name . }}, opts ...protodb.SetOption) (*{{ name . }}, error) {
	return set{{ name . }}(ctx, s.db, m, opts...)
}

func (s *_{{ name . }}DB) Delete(ctx context.Context, m *{{ name . }}) error {
	return delete{{ name . }}(ctx, s.db, m)
}

func (s *_{{ name . }}DB) Watch(ctx context.Context, m *{{ name . }}, opts ...protodb.GetOption) (<-chan {{ name . }}Event, error) {
	out := make(chan {{ name . }}Event)
	ch, err := s.db.Watch(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(out)
		for e := range ch {
			ev := &_{{ name . }}Event{typ: e.Type(), err: e.Err()}
			if n := e.New(); n != nil {
				v, ok := n.(*{{ name . }})
				if ! ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for new {{ name . }}: %T", n))
				} else {
					ev.new = v
				}
			}
			if o := e.Old(); o != nil {
				v, ok := o.(*{{ name . }})
				if ! ok {
					ev.err = multierr.Append(ev.err, fmt.Errorf("unexpected type for old {{ name . }}: %T", o))
				} else {
					ev.old = v
				}
			}
			out <- ev
		}
	}()
	return out, nil
}

func (s *_{{ name . }}DB) Tx(ctx context.Context) ({{ name . }}Tx, error) {
	txn, err := s.db.Tx(ctx)
	return &_{{ name . }}Tx{txn: txn}, err
}

func get{{ name . }}(ctx context.Context, r protodb.Reader, m *{{ name . }}, opts ...protodb.GetOption) ([]*{{ name . }}, *protodb.PagingInfo, error) {
	ms, i, err := r.Get(ctx, m, opts...)
	if err != nil {
		return nil, nil, err
	}
	var out []*{{ name . }}
	for _, v := range ms {
		vv, ok := v.(*{{ name . }})
		if !ok {
			return  nil, nil, fmt.Errorf("unexpected type for {{ name .}}: %T", v)
		}
		out = append(out, vv)
	}
	return out, i, nil
}

func set{{ name . }}(ctx context.Context, w protodb.Writer, m *{{ name . }}, opts ...protodb.SetOption) (*{{ name . }}, error) {
	v, err := w.Set(ctx, m, opts...)
	if err != nil {
		return nil, err
	}
	vv, ok := v.(*{{ name . }})
	if !ok {
		return  nil, fmt.Errorf("unexpected type for {{ name .}}: %T", v)
	}
	return vv, nil
}

func delete{{ name . }}(ctx context.Context, w protodb.Writer, m *{{ name . }}) error {
	return w.Delete(ctx, m)
}

func New{{ name . }}Tx(tx protodb.Tx) {{ name . }}Tx {
	return &_{{ name . }}Tx{txn: tx}
}

type _{{ name . }}Tx struct {
	txn protodb.Tx
}

func (s *_{{ name . }}Tx) Get(ctx context.Context, m *{{ name . }}, opts ...protodb.GetOption) ([]*{{ name . }}, *protodb.PagingInfo, error) {
	return get{{ name . }}(ctx, s.txn, m, opts...)
}

func (s *_{{ name . }}Tx) Set(ctx context.Context, m *{{ name . }}, opts ...protodb.SetOption) (*{{ name . }}, error) {
	return set{{ name . }}(ctx, s.txn, m, opts...)
}

func (s *_{{ name . }}Tx) Delete(ctx context.Context, m *{{ name . }}) error {
	return delete{{ name . }}(ctx, s.txn, m)
}

func (s *_{{ name . }}Tx) Commit(ctx context.Context) error {
	return s.txn.Commit(ctx)
}

func (s *_{{ name . }}Tx) Close() {
	s.txn.Close()
}

func (s *_{{ name . }}Tx) Count() (int64, error) {
	return s.txn.Count()
}

func (s *_{{ name . }}Tx) Size() (int64, error) {
	return s.txn.Size()
}

type _{{ name . }}Event struct {
	typ protodb.EventType
	old *{{ name . }}
	new *{{ name . }}
	err error
}

func (e *_{{ name . }}Event) Type() protodb.EventType {
	return e.typ
}

func (e *_{{ name . }}Event) Old() *{{ name . }} {
	return e.old
}

func (e *_{{ name . }}Event) New() *{{ name . }} {
	return e.new
}

func (e *_{{ name . }}Event) Err() error {
	return e.err
}

{{ end }}

{{ end }}
`

func main() {
	feat := uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
	pgs.Init(
		pgs.DebugEnv("DEBUG"),
		pgs.SupportedFeatures(&feat),
	).RegisterModule(
		module(),
	).RegisterPostProcessor(
		pgsgo.GoFmt(),
	).Render()
}
