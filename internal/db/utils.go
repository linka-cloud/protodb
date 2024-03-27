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
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mennanov/fmutils"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/protofilters/filters"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"go.linka.cloud/protodb/internal/protodb"
)

func FilterFieldMask(m proto.Message, fm *fieldmaskpb.FieldMask) error {
	return FilterFieldMaskPaths(m, fm.GetPaths()...)
}

func FilterFieldMaskPaths(m proto.Message, path ...string) error {
	if _, err := fieldmaskpb.New(m, path...); err != nil {
		return err
	}
	fmutils.Filter(m, path)
	return nil
}

func ApplyFieldMask(src proto.Message, dst proto.Message, fm *fieldmaskpb.FieldMask) error {
	return ApplyFieldMaskPaths(src, dst, fm.GetPaths()...)
}

func ApplyFieldMaskPaths(src, dst proto.Message, path ...string) error {
	if src == nil {
		return errors.New("src is nil")
	}
	if dst == nil {
		return errors.New("dst is nil")
	}
	fm, err := fieldmaskpb.New(dst, path...)
	if err != nil {
		return err
	}
	fmutils.Filter(src, fm.Paths)
	fmutils.Prune(dst, fm.Paths)
	proto.Merge(dst, src)
	return nil
}

type lock struct {
	m sync.RWMutex
	n atomic.Int64
}

func (l *lock) Lock() {
	logger.C(context.Background()).Warnf("lock (%d)", l.n.Add(1))
	n := time.Now()
	l.m.Lock()
	logger.C(context.Background()).Warnf("lock took %s (%d)", time.Since(n), l.n.Load())
}

func (l *lock) Unlock() {
	logger.C(context.Background()).Warnf("unlock (%d)", l.n.Load())
	l.m.Unlock()
}

func (l *lock) RLock() {
	logger.C(context.Background()).Warnf("rlock (%d)", l.n.Add(1))
	n := time.Now()
	l.m.RLock()
	logger.C(context.Background()).Warnf("rlock took %s (%d)", time.Since(n), l.n.Load())
}

func (l *lock) RUnlock() {
	logger.C(context.Background()).Warnf("runlock")
	l.m.RUnlock()
}

func IsKeyOnlyFilter(f protodb.Filter, field string) bool {
	if f == nil {
		return false
	}
	if f.Expr().GetCondition().GetField() != field {
		return false
	}
	for _, v := range f.Expr().GetAndExprs() {
		if !IsKeyOnlyFilter(v, field) {
			return false
		}
	}
	for _, v := range f.Expr().GetOrExprs() {
		if !IsKeyOnlyFilter(v, field) {
			return false
		}
	}
	return true
}

func MatchPrefixOnly(f protodb.Filter) (string, bool) {
	if f == nil {
		return "", false
	}
	if f.Expr().GetAndExprs() != nil {
		return "", false
	}
	if f.Expr().GetOrExprs() != nil {
		return "", false
	}
	p := f.Expr().GetCondition().GetFilter().GetString_().GetHasPrefix()
	return p, p != ""
}

func MatchKey(filter filters.FieldFilterer, value string) (bool, error) {
	ok, err := doMatchKey(filter.Expr().GetCondition().GetFilter(), value)
	if err != nil {
		return false, err
	}
	andOk := true
	for _, v := range filter.Expr().GetAndExprs() {
		andOk, err = MatchKey(v, value)
		if err != nil {
			return false, err
		}
		if !andOk {
			break
		}
	}
	orOk := false
	for _, v := range filter.Expr().GetOrExprs() {
		orOk, err = MatchKey(v, value)
		if err != nil {
			return false, err
		}
		if orOk {
			break
		}
	}
	return ok && andOk || orOk, nil
}

func doMatchKey(filter *filters.Filter, value string) (bool, error) {
	if filter == nil {
		return true, nil
	}
	switch f := filter.Match.(type) {
	case *filters.Filter_String_:
		return f.String_.Match(&value)
	case *filters.Filter_Number:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return false, err
		}
		return f.Number.Match(&v)
	default:
		return false, fmt.Errorf("unsupported filter type for key: %T", f)
	}
}
