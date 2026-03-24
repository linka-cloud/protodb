package pending

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWritesIteratorWhenWalBackedRespectsDeleteVisibility(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Update(func(tx *badger.Txn) error {
		require.NoError(t, tx.Set([]byte("k0"), []byte("b0")))
		require.NoError(t, tx.Set([]byte("k1"), []byte("b1")))
		require.NoError(t, tx.Set([]byte("k2"), []byte("b2")))
		require.NoError(t, tx.Set([]byte("k3"), []byte("b3")))
		return nil
	}))

	tx := db.NewTransaction(false)
	defer tx.Discard()

	w := newWrites(db.Opts().Dir, tx, 2, 1<<20, int(db.Opts().ValueThreshold))
	defer w.Close()

	w.Set(&badger.Entry{Key: []byte("k1"), Value: []byte("p1")})
	w.Set(&badger.Entry{Key: []byte("k4"), Value: []byte("p4")})
	w.Delete([]byte("k2"))

	require.NotNil(t, w.w)
	assert.Empty(t, w.m.m)

	item, err := w.Get([]byte("k1"))
	require.NoError(t, err)
	v, err := item.ValueCopy(nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("p1"), v)

	_, err = w.Get([]byte("k2"))
	require.ErrorIs(t, err, badger.ErrKeyNotFound)

	it := w.Iterator(nil, false)
	defer it.Close()

	got := map[string]string{}
	for it.Rewind(); it.Valid(); it.Next() {
		item = it.Item()
		if item.IsDeletedOrExpired() {
			continue
		}
		v, err = item.ValueCopy(nil)
		require.NoError(t, err)
		got[string(item.Key())] = string(v)
	}

	assert.Equal(t, map[string]string{
		"k0": "b0",
		"k1": "p1",
		"k3": "b3",
		"k4": "p4",
	}, got)
}
