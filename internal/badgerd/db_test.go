package badgerd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenInMemoryAndCloseIdempotent(t *testing.T) {
	dbi, err := Open(context.Background(), WithInMemory(true))
	require.NoError(t, err)
	db := dbi.(*db)

	assert.True(t, db.InMemory())
	assert.Equal(t, "", db.Path())
	assert.False(t, db.Replicated())

	require.NoError(t, db.Close())
	require.NoError(t, db.Close())
}
