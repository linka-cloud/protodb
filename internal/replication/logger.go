// Copyright 2023 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replication

import (
	"bytes"
	"context"
	"log"

	"github.com/bombsimon/logrusr/v4"
	"github.com/sirupsen/logrus"
	"go.linka.cloud/grpc-toolkit/logger"
	"k8s.io/klog/v2"
)

func newLogger(ctx context.Context) *log.Logger {
	return log.New(&logw{logger: logger.C(ctx)}, "", 0)
}

type logw struct {
	logger logger.Logger
}

func (l *logw) Write(b []byte) (int, error) {
	b = bytes.TrimRight(b, "\n")
	switch {
	case bytes.HasPrefix(b, []byte("[DEBUG]")):
		l.logger.Debug(string(bytes.TrimPrefix(b, []byte("[DEBUG] "))))
	case bytes.HasPrefix(b, []byte("[INFO]")):
		l.logger.Info(string(bytes.TrimPrefix(b, []byte("[INFO] "))))
	case bytes.HasPrefix(b, []byte("[WARN]")):
		l.logger.Warn(string(bytes.TrimPrefix(b, []byte("[WARN] "))))
	case bytes.HasPrefix(b, []byte("[ERROR]")):
		l.logger.Error(string(bytes.TrimPrefix(b, []byte("[ERROR] "))))
	default:
		l.logger.Info(string(b))
	}
	return len(b), nil
}

func init() {
	klog.SetLogger(logrusr.New(logrus.StandardLogger()))
}
