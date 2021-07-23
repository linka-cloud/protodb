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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type QueryOption func(o *queryOpts)

func WithPaging(paging *Paging) QueryOption {
	return func(o *queryOpts) {
		o.paging = paging
	}
}

func WithFilters(filters ...*FieldFilter) QueryOption {
	return func(o *queryOpts) {
		o.filters = filters
	}
}

func WithFieldMask(fieldMask *fieldmaskpb.FieldMask) QueryOption {
	return func(o *queryOpts) {
		o.fieldMask = fieldMask
	}
}

type queryOpts struct {
	paging    *Paging
	filters   []*FieldFilter
	fieldMask *fieldmaskpb.FieldMask
}

func makeOpts(opts ...QueryOption) queryOpts {
	o := queryOpts{}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}
