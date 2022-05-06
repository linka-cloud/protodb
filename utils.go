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
	"errors"

	"github.com/mennanov/fmutils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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
