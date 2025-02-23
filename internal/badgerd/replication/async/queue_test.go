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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var _ Stream[any, any] = (*mockStream)(nil)

func newMockStream() *mockStream {
	return &mockStream{
		s: make(chan error, 1),
		a: make(chan error, 1),
		c: make(chan error, 1),
	}
}

type mockStream struct {
	s      chan error
	a      chan error
	closed bool
	c      chan error
}

func (m *mockStream) Send(_ any) error {
	return <-m.s
}

func (m *mockStream) Recv() (any, error) {
	return nil, <-m.a
}

func (m *mockStream) sent(err error) {
	m.s <- err
}

func (m *mockStream) ack(err error) {
	m.a <- err
}

func (m *mockStream) close(err error) {
	m.c <- err
}

func (m *mockStream) CloseSend() error {
	m.closed = true
	return <-m.c
}

func TestQueue(t *testing.T) {
	t.Run("basic flow", func(t *testing.T) {
		s := newMockStream()
		q := NewQueue[any, any](s)
		var a Async[any]
		t.Run("send", func(t *testing.T) {
			a = q.Send(nil)
			assert.False(t, a.Sent())
			s.sent(nil)
			assert.Nil(t, a.WaitSent())
			assert.True(t, a.Sent())
			// double wait sent should return the same result
			assert.Nil(t, a.WaitSent())
		})
		t.Run("ack", func(t *testing.T) {
			if a == nil {
				t.Skip()
			}
			ch := make(chan *Result[any])
			go func() {
				ch <- NewResult(a.Wait())
			}()
			assert.Nil(t, a.WaitSent())
			s.ack(nil)
			r := <-ch
			assert.Nil(t, r.err)
			assert.Nil(t, r.value)
			assert.False(t, q.closed())
		})
		t.Run("close", func(t *testing.T) {
			ech := make(chan error)
			go func() {
				ech <- q.Close()
			}()
			assert.False(t, q.closed())
			s.close(nil)
			err := <-ech
			assert.Nil(t, err)
			assert.True(t, q.closed())
		})
	})
	t.Run("send error", func(t *testing.T) {
		s := newMockStream()
		q := NewQueue[any, any](s)
		a := q.Send(nil)
		assert.False(t, a.Sent())
		s.sent(os.ErrNotExist)
		assert.ErrorIs(t, a.WaitSent(), os.ErrNotExist)
		assert.True(t, a.Sent())
		// double wait sent should return the same result
		assert.ErrorIs(t, a.WaitSent(), os.ErrNotExist)
		_, err := a.Wait()
		assert.ErrorIs(t, err, os.ErrNotExist)
		assert.True(t, q.closed())
	})
	t.Run("ack error", func(t *testing.T) {
		s := newMockStream()
		q := NewQueue[any, any](s)
		a := q.Send(nil)
		assert.False(t, a.Sent())
		s.sent(nil)
		assert.Nil(t, a.WaitSent())
		assert.True(t, a.Sent())
		ch := make(chan *Result[any])
		go func() {
			ch <- NewResult(a.Wait())
		}()
		assert.Nil(t, a.WaitSent())
		s.ack(os.ErrNotExist)
		r := <-ch
		assert.ErrorIs(t, r.err, os.ErrNotExist)
		assert.Nil(t, r.value)
		// double wait sent should return the same result
		_, err := a.Wait()
		assert.ErrorIs(t, err, os.ErrNotExist)
		assert.True(t, q.closed())
		assert.Nil(t, q.Close())
	})
	t.Run("close wait for all messages", func(t *testing.T) {
		s := newMockStream()
		q := NewQueue[any, any](s)
		a := q.Send(nil)
		assert.False(t, a.Sent())
		s.sent(nil)
		assert.Nil(t, a.WaitSent())
		assert.True(t, a.Sent())
		ch := make(chan *Result[any])
		go func() {
			ch <- NewResult(a.Wait())
		}()
		assert.Nil(t, a.WaitSent())
		ech := make(chan error)
		go func() {
			ech <- q.Close()
		}()
		s.ack(nil)
		r := <-ch
		assert.Nil(t, r.err)
		assert.Nil(t, r.value)
		assert.False(t, q.closed())
		s.close(nil)
		err := <-ech
		assert.Nil(t, err)
		assert.True(t, q.closed())
	})
	t.Run("send after close", func(t *testing.T) {
		s := newMockStream()
		q := NewQueue[any, any](s)
		ch := make(chan error)
		go func() {
			ch <- q.Close()
		}()
		s.close(nil)
		assert.Nil(t, <-ch)
		assert.True(t, q.closed())
		a := q.Send(nil)
		assert.ErrorIs(t, a.WaitSent(), ErrClosed)
		_, err := a.Wait()
		assert.ErrorIs(t, err, ErrClosed)
	})
}
