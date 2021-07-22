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

package token

import (
	"encoding/base64"
	"errors"
	"fmt"
)

var ErrInvalid = errors.New("invalid continuation token")

func (x *Token) Encode() (string, error) {
	if x == nil {
		return "", nil
	}
	b, err := x.MarshalVT()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (x *Token) Decode(s string) error {
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return x.UnmarshalVT(b)
}

func (x *Token) ValidateFor(prev *Token) error {
	if x == nil && prev == nil {
		return nil
	}
	if x != nil && prev == nil {
		return nil
	}
	if typ, tkType := prev.GetType(), x.GetType(); typ != "" && typ != tkType {
		return fmt.Errorf("%w: token valid for %s, not %s", ErrInvalid, typ, tkType)
	}
	if prev.GetFiltersHash() != "" && x.GetFiltersHash() != prev.GetFiltersHash() {
		return fmt.Errorf("%w: filters mismatch", ErrInvalid)
	}
	return nil
}
