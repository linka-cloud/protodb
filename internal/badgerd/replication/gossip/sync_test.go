package gossip

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicTable(t *testing.T) {
	a := NewAtomic(1)
	assert.Equal(t, 1, a.Load())

	assert.True(t, a.CompareAndSwap(1, 2))
	assert.False(t, a.CompareAndSwap(1, 3))
	assert.Equal(t, 2, a.Load())

	old := a.Swap(9)
	assert.Equal(t, 2, old)
	assert.Equal(t, 9, a.Load())
}

func TestMapTable(t *testing.T) {
	m := &Map[int]{}
	m.Store("a", 1)
	m.Store("b", 2)

	v, ok := m.Load("a")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	_, ok = m.Load("c")
	assert.False(t, ok)

	assert.Equal(t, 2, m.Len())

	keys := m.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"a", "b"}, keys)

	values := m.Values()
	sort.Ints(values)
	assert.Equal(t, []int{1, 2}, values)

	seen := map[string]int{}
	m.Range(func(key string, value int) bool {
		seen[key] = value
		return true
	})
	assert.Equal(t, map[string]int{"a": 1, "b": 2}, seen)

	m.Delete("a")
	assert.Equal(t, 1, m.Len())

	m.Clear()
	assert.Equal(t, 0, m.Len())
}
