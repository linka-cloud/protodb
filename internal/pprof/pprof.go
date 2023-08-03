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

package pprof

import (
	"os"
	"runtime/pprof"
	"runtime/trace"

	"github.com/sirupsen/logrus"
)

type Ender interface {
	End()
}

type enderFunc func()

func (f enderFunc) End() {
	f()
}

func Start(p string) Ender {
	cpuf, err := os.Create(p + ".cpu.pprof")
	if err != nil {
		logrus.Fatal("could not create CPU profile: ", err)
	}
	pprof.Profiles()
	if err := pprof.StartCPUProfile(cpuf); err != nil {
		logrus.Fatal("could not start CPU profile: ", err)
	}
	memf, err := os.Create(p + ".mem.pprof")
	if err := pprof.WriteHeapProfile(memf); err != nil {
		logrus.Fatal("could not start memory profile: ", err)
	}
	blockf, err := os.Create(p + ".block.pprof")
	if err != nil {
		logrus.Fatal("could not create block profile: ", err)
	}
	if err := pprof.Lookup("block").WriteTo(blockf, 0); err != nil {
		logrus.Fatal("could not start block profile: ", err)
	}
	tracef, err := os.Create(p + ".trace.pprof")
	if err != nil {
		logrus.Fatal("could not create trace profile: ", err)
	}
	if err := trace.Start(tracef); err != nil {
		logrus.Fatal("could not start trace profile: ", err)
	}
	return enderFunc(func() {
		trace.Stop()
		pprof.StopCPUProfile()
		if err := cpuf.Close(); err != nil {
			logrus.Error("could not close CPU profile: ", err)
		}
		if err := memf.Close(); err != nil {
			logrus.Error("could not close memory profile: ", err)
		}
		if err := blockf.Close(); err != nil {
			logrus.Error("could not close block profile: ", err)
		}
		if err := tracef.Close(); err != nil {
			logrus.Error("could not close trace profile: ", err)
		}
	})
}
