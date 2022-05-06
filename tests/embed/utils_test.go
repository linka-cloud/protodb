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

package embed

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.linka.cloud/protodb"
	testpb "go.linka.cloud/protodb/tests/pb"
)

// TestApplyFieldMaskPaths cannot be in the protodb package due to import cycle not allowed
func TestApplyFieldMaskPaths(t *testing.T) {
	o := &testpb.Interface{
		Name:   "eth0",
		Status: testpb.StatusUp,
		Addresses: []*testpb.IPAddress{
			{
				Address: &testpb.IPAddress_IPV4{
					IPV4: "10.0.0.42/24",
				},
			},
		},
	}

	n := &testpb.Interface{
		Name:   "lan0",
		Status: testpb.StatusDown,
		Addresses: []*testpb.IPAddress{
			{
				Address: &testpb.IPAddress_IPV4{
					IPV4: "10.0.10.42/24",
				},
			},
		},
	}

	type test struct {
		name   string
		fields []string
		want   func(o *testpb.Interface)
	}

	tests := []test{
		{
			name:   "string field",
			fields: []string{testpb.InterfaceFields.Name},
			want: func(o *testpb.Interface) {
				o.Name = n.Name
			},
		},
		{
			name:   "enum field",
			fields: []string{testpb.InterfaceFields.Status},
			want: func(o *testpb.Interface) {
				o.Status = n.Status
			},
		},
		{
			name:   "repeated field",
			fields: []string{testpb.InterfaceFields.Addresses},
			want: func(o *testpb.Interface) {
				o.Addresses = n.Addresses
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := proto.Clone(o).(*testpb.Interface)
			n := proto.Clone(n).(*testpb.Interface)
			w := proto.Clone(o).(*testpb.Interface)
			tt.want(w)
			require.NoError(t, protodb.ApplyFieldMaskPaths(n, o, tt.fields...))
			assert.Equal(t, w.String(), o.String())
		})
	}
}
