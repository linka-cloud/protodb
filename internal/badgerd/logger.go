// Copyright 2024 Linka Cloud  All rights reserved.
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

package badgerd

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

var _ badger.Logger = (*logWrapper)(nil)

type logWrapper struct {
	badger.Logger
}

func (l *logWrapper) Errorf(s string, i ...interface{}) {
	l.log(l.Logger.Errorf, s, i...)
}

func (l *logWrapper) Warningf(s string, i ...interface{}) {
	l.log(l.Logger.Warningf, s, i...)
}

func (l *logWrapper) Infof(s string, i ...interface{}) {
	l.log(l.Logger.Infof, s, i...)
}

func (l *logWrapper) Debugf(s string, i ...interface{}) {
	l.log(l.Logger.Debugf, s, i...)
}

func (l *logWrapper) log(fn func(string, ...interface{}), s string, i ...interface{}) {
	msg := fmt.Sprintf(s, i...)
	for _, v := range strings.Split(msg, "\n") {
		if v == "" {
			continue
		}
		fn(v)
	}
}
