package badgerd

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxCommitConflictReturnsErrConflict(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	k := []byte("k")

	tx1, err := db.newTransaction(ctx, true)
	require.NoError(t, err)
	tx2, err := db.newTransaction(ctx, true)
	require.NoError(t, err)

	tx2.AddReadKey(k)
	require.NoError(t, tx1.Set(ctx, k, []byte("v1"), 0))
	require.NoError(t, tx1.Commit(ctx))

	require.NoError(t, tx2.Set(ctx, k, []byte("v2"), 0))
	err = tx2.Commit(ctx)
	require.ErrorIs(t, err, badger.ErrConflict)
}

func TestTxGuardsTable(t *testing.T) {
	ctx := context.Background()

	t.Run("set_on_read_only_returns_err_read_only_txn", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		tx, err := db.newTransaction(ctx, false)
		require.NoError(t, err)
		defer tx.Close(ctx)

		err = tx.Set(ctx, []byte("k"), []byte("v"), 0)
		require.ErrorIs(t, err, badger.ErrReadOnlyTxn)
	})

	t.Run("delete_on_read_only_returns_err_read_only_txn", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		tx, err := db.newTransaction(ctx, false)
		require.NoError(t, err)
		defer tx.Close(ctx)

		err = tx.Delete(ctx, []byte("k"))
		require.ErrorIs(t, err, badger.ErrReadOnlyTxn)
	})

	t.Run("set_on_closed_tx_returns_err_db_closed", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		tx, err := db.newTransaction(ctx, true)
		require.NoError(t, err)
		require.NoError(t, tx.Close(ctx))

		err = tx.Set(ctx, []byte("k"), []byte("v"), 0)
		require.ErrorIs(t, err, badger.ErrDBClosed)
	})

	t.Run("delete_on_closed_tx_returns_err_db_closed", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		tx, err := db.newTransaction(ctx, true)
		require.NoError(t, err)
		require.NoError(t, tx.Close(ctx))

		err = tx.Delete(ctx, []byte("k"))
		require.ErrorIs(t, err, badger.ErrDBClosed)
	})

	t.Run("commit_after_close_returns_err_db_closed", func(t *testing.T) {
		db := openTestDB(t)
		defer db.Close()

		tx, err := db.newTransaction(ctx, true)
		require.NoError(t, err)
		require.NoError(t, tx.Close(ctx))

		err = tx.Commit(ctx)
		require.ErrorIs(t, err, badger.ErrDBClosed)
	})
}

func TestTxCloseIdempotent(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.newTransaction(ctx, true)
	require.NoError(t, err)

	assert.NoError(t, tx.Close(ctx))
	assert.NoError(t, tx.Close(ctx))
}

func openTestDB(t *testing.T) *db {
	t.Helper()
	v, err := Open(context.Background(), WithInMemory(true))
	require.NoError(t, err)

	db, ok := v.(*db)
	require.True(t, ok)
	return db
}
