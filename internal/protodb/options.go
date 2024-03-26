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
	"time"

	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"go.linka.cloud/protodb/pb"
)

type (
	Paging     = pb.Paging
	PagingInfo = pb.PagingInfo
	FilterExpr = filters.Expression
	Filter     = filters.FieldFilterer
)

type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

type GetOption func(o *GetOpts)

func WithPaging(paging *Paging) GetOption {
	return func(o *GetOpts) {
		o.Paging = paging
	}
}

func WithFilter(filter Filter) GetOption {
	return func(o *GetOpts) {
		o.Filter = filter
	}
}

func WithReverse() GetOption {
	return func(o *GetOpts) {
		o.Reverse = true
	}
}

func WithReadFieldMaskPaths(paths ...string) GetOption {
	return func(o *GetOpts) {
		o.FieldMask = &fieldmaskpb.FieldMask{Paths: paths}
	}
}

func WithReadFieldMask(fieldMask *fieldmaskpb.FieldMask) GetOption {
	return func(o *GetOpts) {
		o.FieldMask = fieldMask
	}
}

type TxOpts struct {
	ReadOnly bool
}

type TxOption func(o *TxOpts)

func WithReadOnly() TxOption {
	return func(o *TxOpts) {
		o.ReadOnly = true
	}
}

type GetOpts struct {
	Paging    *Paging
	Filter    Filter
	FieldMask *fieldmaskpb.FieldMask
	Reverse   bool
}

type SetOption func(o *SetOpts)

func WithTTL(d time.Duration) SetOption {
	return func(o *SetOpts) {
		o.TTL = d
	}
}

func WithWriteFieldMaskPaths(paths ...string) SetOption {
	return func(o *SetOpts) {
		o.FieldMask = &fieldmaskpb.FieldMask{Paths: paths}
	}
}

func WithWriteFieldMask(fieldMask *fieldmaskpb.FieldMask) SetOption {
	return func(o *SetOpts) {
		o.FieldMask = fieldMask
	}
}

type SetOpts struct {
	TTL       time.Duration
	FieldMask *fieldmaskpb.FieldMask
}
