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

package raft

import (
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/raft/raftlog"
)

var _ raftlog.Logger = (*logWrapper)(nil)

type logWrapper struct {
	logger.Logger
}

func (l *logWrapper) V(lvl int) raftlog.Verbose {
	return &verboseWrapper{lvl, l.Logger}
}

type verboseWrapper struct {
	lvl int
	logger.Logger
}

func (v *verboseWrapper) Enabled() bool {
	// logrus level are 0-6 (PanicLevel-TraceLevel)
	// raft level are 0-4 (Info-Panic)
	return int(v.Logger.Logger().Level)*-1+4 >= v.lvl
}
