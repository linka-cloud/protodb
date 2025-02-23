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

package async

import (
	"errors"
	"sync"
)

var ErrClosed = errors.New("queue closed")

type Stream[Req any, Res any] interface {
	Send(req Req) error
	Recv() (Res, error)
	CloseSend() error
}

type Async[Res any] interface {
	WaitSent() error
	Sent() bool
	Wait() (Res, error)
}

type async[Res any] struct {
	ch    chan *Result[Res]
	mu    sync.RWMutex
	sent  bool
	err   error
	value Res
}

func (a *async[Res]) WaitSent() error {
	a.mu.RLock()
	if a.sent || a.err != nil {
		defer a.mu.RUnlock()
		return a.err
	}
	a.mu.RUnlock()
	r := <-a.ch
	a.mu.Lock()
	a.sent = true
	a.err = r.err
	a.mu.Unlock()
	return r.err
}

func (a *async[Res]) Sent() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.sent
}

func (a *async[Res]) Wait() (Res, error) {
	var z Res
	if err := a.WaitSent(); err != nil {
		return z, err
	}
	select {
	case r, ok := <-a.ch:
		if !ok {
			a.mu.RLock()
			defer a.mu.RUnlock()
			return a.value, a.err
		}
		a.mu.Lock()
		a.value = r.value
		a.err = r.err
		a.mu.Unlock()
		return r.value, r.err
	}
}

func NewResult[T any](v T, err error) *Result[T] {
	return &Result[T]{value: v, err: err}
}

type Result[T any] struct {
	value T
	err   error
}

func (r *Result[T]) Value() T {
	return r.value
}

func (r *Result[T]) Err() error {
	return r.err
}

type message[Req any, Res any] struct {
	req  Req
	done bool
	ch   chan *Result[Res]
}

func NewQueue[Req any, Res any](s Stream[Req, Res], size ...int) *Queue[Req, Res] {
	var sz int
	if len(size) > 0 {
		sz = size[0]
	}
	q := &Queue[Req, Res]{s: s, ch: make(chan *message[Req, Res], sz), close: make(chan struct{})}
	go q.run()
	return q
}

type Queue[Req any, Res any] struct {
	ch    chan *message[Req, Res]
	s     Stream[Req, Res]
	close chan struct{}
}

func (q *Queue[Req, Res]) Send(r Req) Async[Res] {
	if q.closed() {
		return &async[Res]{err: ErrClosed}
	}
	ch := make(chan *Result[Res], 2)
	q.ch <- &message[Req, Res]{req: r, ch: ch}
	return &async[Res]{ch: ch}

}

func (q *Queue[Req, Res]) Close() error {
	select {
	case <-q.close:
		return nil
	default:
		ch := make(chan *Result[Res])
		q.ch <- &message[Req, Res]{done: true, ch: ch}
		r := <-ch
		closeSafe(q.close)
		return r.err
	}
}

func (q *Queue[Req, Res]) run() {
	do := func(msg *message[Req, Res]) bool {
		defer close(msg.ch)
		if err := q.s.Send(msg.req); err != nil {
			msg.ch <- &Result[Res]{err: err}
			return false
		}
		msg.ch <- &Result[Res]{}
		res, err := q.s.Recv()
		msg.ch <- &Result[Res]{value: res, err: err}
		if err != nil {
			return false
		}
		return true
	}
	for {
		select {
		case msg, ok := <-q.ch:
			if !ok {
				return
			}
			if !msg.done {
				if !do(msg) {
					closeSafe(q.close)
					return
				}
				continue
			}
			if err := q.s.CloseSend(); err != nil {
				msg.ch <- &Result[Res]{err: err}
				return
			}
			msg.ch <- &Result[Res]{}
		case <-q.close:
			return
		}
	}
}

func (q *Queue[Req, Res]) closed() bool {
	select {
	case <-q.close:
		return true
	default:
		return false
	}
}

func closeSafe[T any](ch chan T) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
