package pending

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItemIsDeletedOrExpiredTable(t *testing.T) {
	now := uint64(time.Now().Unix())

	tests := []struct {
		name string
		e    *badger.Entry
		want bool
	}{
		{name: "deleted", e: &badger.Entry{UserMeta: BitDelete}, want: true},
		{name: "no_expiry", e: &badger.Entry{ExpiresAt: 0}, want: false},
		{name: "future_expiry", e: &badger.Entry{ExpiresAt: now + 2}, want: false},
		{name: "past_expiry", e: &badger.Entry{ExpiresAt: now - 2}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &item{e: tt.e}
			assert.Equal(t, tt.want, i.IsDeletedOrExpired())
		})
	}
}

func TestItemValueCopyIsSafeCopy(t *testing.T) {
	i := &item{e: &badger.Entry{Value: []byte("abc")}}
	b, err := i.ValueCopy(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), b)
	b[0] = 'z'
	assert.Equal(t, []byte("abc"), i.e.Value)
}
