package pending

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

func TestMemIteratorPrefixSeekReverseTable(t *testing.T) {
	m := newMem(nil)
	m.Set(&badger.Entry{Key: []byte("a/1"), Value: []byte("v1")})
	m.Set(&badger.Entry{Key: []byte("a/2"), Value: []byte("v2")})
	m.Set(&badger.Entry{Key: []byte("b/1"), Value: []byte("v3")})
	m.Delete([]byte("a/2"))

	t.Run("forward_prefix_skips_deleted", func(t *testing.T) {
		it := m.newIterator([]byte("a/"), 0, false)
		keys := visibleKeys(it)
		assert.Equal(t, [][]byte{[]byte("a/1")}, keys)
	})

	t.Run("reverse_prefix_skips_deleted", func(t *testing.T) {
		it := m.newIterator([]byte("a/"), 0, true)
		keys := visibleKeys(it)
		assert.Equal(t, [][]byte{[]byte("a/1")}, keys)
	})

	t.Run("seek_bounds", func(t *testing.T) {
		it := m.newIterator(nil, 0, false)
		it.seek([]byte("b/"))
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("b/1"), it.Key())
	})

	t.Run("seek_last_prefix", func(t *testing.T) {
		it := m.newIterator([]byte("a/"), 0, true)
		it.SeekLast()
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("a/2"), it.Key())
		assert.True(t, it.skip())
		it.Next()
		assert.True(t, it.Valid())
		assert.Equal(t, []byte("a/1"), it.Key())
		assert.False(t, it.skip())
	})
}

func visibleKeys(it iterator) [][]byte {
	keys := make([][]byte, 0)
	for it.Rewind(); it.Valid(); it.Next() {
		if it.skip() {
			continue
		}
		k := append([]byte(nil), it.Key()...)
		keys = append(keys, k)
	}
	return keys
}
