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

package tests

import (
	"testing"

	"go.linka.cloud/protodb"
)

func TestReplicationModes(t *testing.T) {
	for _, v := range []protodb.ReplicationMode{protodb.ReplicationModeAsync, protodb.ReplicationModeSync} {
		t.Run(v.String(), func(t *testing.T) {
			TestReplication(t, data, v)
		})
	}
}
