package pending

import (
	"bytes"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

func TestIncrementPrefixTable(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{name: "empty", prefix: nil, want: []byte{}},
		{name: "simple", prefix: []byte{0x01, 0x02}, want: []byte{0x01, 0x03}},
		{name: "carry", prefix: []byte{0x01, 0xFF}, want: []byte{0x02}},
		{name: "all_ff", prefix: []byte{0xFF, 0xFF}, want: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, incrementPrefix(tt.prefix))
		})
	}
}

func TestSeekLastTable(t *testing.T) {
	t.Run("forward_seeks_to_next_prefix_boundary", func(t *testing.T) {
		it := newMockIterator([][]byte{
			[]byte("a/1"),
			[]byte("a/2"),
			[]byte("a/3"),
			[]byte("b/1"),
		}, false)

		seekLast(it, []byte("a/"))
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("b/1"), it.Key())
	})

	t.Run("reverse_seeks_to_last_key_with_prefix", func(t *testing.T) {
		it := newMockIterator([][]byte{
			[]byte("a/1"),
			[]byte("a/2"),
			[]byte("a/3"),
			[]byte("b/1"),
		}, true)

		seekLast(it, []byte("a/"))
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("a/3"), it.Key())
	})

	t.Run("forward_prefix_after_last_is_invalid", func(t *testing.T) {
		it := newMockIterator([][]byte{[]byte("a/1")}, false)
		seekLast(it, []byte("z/"))
		assert.False(t, it.Valid())
	})
}

type mockIterator struct {
	keys     [][]byte
	idx      int
	reversed bool
}

func newMockIterator(keys [][]byte, reversed bool) *mockIterator {
	sorted := make([][]byte, len(keys))
	copy(sorted, keys)
	sort.Slice(sorted, func(i, j int) bool {
		if reversed {
			return bytes.Compare(sorted[i], sorted[j]) > 0
		}
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})
	return &mockIterator{keys: sorted, idx: 0, reversed: reversed}
}

func (m *mockIterator) Rewind() { m.idx = 0 }
func (m *mockIterator) Next()   { m.idx++ }
func (m *mockIterator) Valid() bool {
	return m.idx >= 0 && m.idx < len(m.keys)
}
func (m *mockIterator) Seek(key []byte) {
	m.seek(key)
}
func (m *mockIterator) seek(key []byte) {
	m.idx = sort.Search(len(m.keys), func(i int) bool {
		cmp := bytes.Compare(m.keys[i], key)
		if m.reversed {
			return cmp <= 0
		}
		return cmp >= 0
	})
}
func (m *mockIterator) SeekLast()   {}
func (m *mockIterator) Key() []byte { return m.keys[m.idx] }
func (m *mockIterator) Item() Item {
	return &item{e: &badger.Entry{Key: m.keys[m.idx]}}
}
func (m *mockIterator) Close()     {}
func (m *mockIterator) skip() bool { return false }
