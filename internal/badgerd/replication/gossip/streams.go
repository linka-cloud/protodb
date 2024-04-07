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

package gossip

import (
	"go.linka.cloud/protodb/internal/badgerd/replication"
	pb2 "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

type writer struct {
	ss pb2.ReplicationService_InitServer
}

func (w *writer) Write(p []byte) (n int, err error) {
	b := make([]byte, len(p))
	copy(b, p)
	for len(b) > 0 {
		d := &pb2.InitResponse{}
		if len(b) > replication.MaxMsgSize {
			d.Data = b[:replication.MaxMsgSize]
			b = b[replication.MaxMsgSize:]
		} else {
			d.Data = b
			b = nil
		}
		if err := w.ss.Send(d); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

type reader struct {
	ss   pb2.ReplicationService_InitClient
	buff []byte
}

func (r *reader) Read(p []byte) (n int, err error) {
	if len(r.buff) > 0 && len(p) > 0 {
		n := copy(p, r.buff)
		r.buff = r.buff[n:]
		return n, nil
	}
	msg, err := r.ss.Recv()
	if err != nil {
		return 0, err
	}
	if len(msg.Data) > len(p) {
		r.buff = msg.Data[len(p):]
		msg.Data = msg.Data[:len(p)]
	}
	n = copy(p, msg.Data)
	return n, nil
}
