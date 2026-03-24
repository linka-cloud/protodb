package replication

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.linka.cloud/protodb/internal/badgerd/pending"
)

func TestMaybeCommitAtReplayTable(t *testing.T) {
	t.Run("set_replayed_with_commit_ts", func(t *testing.T) {
		wb := &fakeWriteBatch{}
		db := &fakeDB{wb: wb}
		s := &Maybe{
			DB:     db,
			readTs: 7,
			w:      &fakeWrites{ops: []fakeOp{{key: []byte("k"), value: []byte("v"), expiresAt: 11}}},
		}

		require.NoError(t, s.CommitAt(context.Background(), 42))
		assert.Equal(t, uint64(7), db.readTs)
		require.Len(t, wb.sets, 1)
		assert.Equal(t, []byte("k"), wb.sets[0].entry.Key)
		assert.Equal(t, []byte("v"), wb.sets[0].entry.Value)
		assert.Equal(t, uint64(11), wb.sets[0].entry.ExpiresAt)
		assert.Equal(t, uint64(42), wb.sets[0].ts)
		assert.True(t, wb.cancelled)
		assert.True(t, wb.flushed)
	})

	t.Run("delete_replayed_with_commit_ts", func(t *testing.T) {
		wb := &fakeWriteBatch{}
		db := &fakeDB{wb: wb}
		s := &Maybe{
			DB:     db,
			readTs: 3,
			w:      &fakeWrites{ops: []fakeOp{{key: []byte("k"), userMeta: pending.BitDelete}}},
		}

		require.NoError(t, s.CommitAt(context.Background(), 19))
		assert.Equal(t, uint64(3), db.readTs)
		require.Len(t, wb.deletes, 1)
		assert.Equal(t, []byte("k"), wb.deletes[0].key)
		assert.Equal(t, uint64(19), wb.deletes[0].ts)
		assert.True(t, wb.cancelled)
		assert.True(t, wb.flushed)
	})

	t.Run("replay_error_propagates", func(t *testing.T) {
		want := errors.New("replay")
		wb := &fakeWriteBatch{}
		s := &Maybe{
			DB:     &fakeDB{wb: wb},
			readTs: 5,
			w:      &fakeWrites{err: want},
		}

		err := s.CommitAt(context.Background(), 8)
		require.ErrorIs(t, err, want)
		assert.True(t, wb.cancelled)
		assert.False(t, wb.flushed)
	})

	t.Run("flush_error_propagates", func(t *testing.T) {
		want := errors.New("flush")
		wb := &fakeWriteBatch{flushErr: want}
		s := &Maybe{
			DB:     &fakeDB{wb: wb},
			readTs: 5,
			w:      &fakeWrites{ops: []fakeOp{{key: []byte("k"), value: []byte("v")}}},
		}

		err := s.CommitAt(context.Background(), 8)
		require.ErrorIs(t, err, want)
		assert.True(t, wb.cancelled)
		assert.True(t, wb.flushed)
	})
}

type fakeDB struct {
	wb     *fakeWriteBatch
	readTs uint64
}

func (f *fakeDB) Path() string                                    { return "" }
func (f *fakeDB) InMemory() bool                                  { return true }
func (f *fakeDB) MaxVersion() uint64                              { return 0 }
func (f *fakeDB) SetVersion(_ uint64)                             {}
func (f *fakeDB) Drop() error                                     { return nil }
func (f *fakeDB) Load(context.Context, io.Reader) (uint64, error) { return 0, nil }
func (f *fakeDB) Stream(context.Context, uint64, uint64, io.Writer) error {
	return nil
}
func (f *fakeDB) NewWriteBatchAt(readTs uint64) WriteBatch {
	f.readTs = readTs
	return f.wb
}
func (f *fakeDB) ValueThreshold() int64 { return 0 }
func (f *fakeDB) MaxBatchCount() int64  { return 0 }
func (f *fakeDB) MaxBatchSize() int64   { return 0 }
func (f *fakeDB) Close() error          { return nil }

type setCall struct {
	entry *badger.Entry
	ts    uint64
}

type deleteCall struct {
	key []byte
	ts  uint64
}

type fakeWriteBatch struct {
	sets      []setCall
	deletes   []deleteCall
	flushed   bool
	cancelled bool
	flushErr  error
}

func (f *fakeWriteBatch) SetEntryAt(e *badger.Entry, ts uint64) error {
	f.sets = append(f.sets, setCall{entry: e, ts: ts})
	return nil
}

func (f *fakeWriteBatch) DeleteAt(key []byte, ts uint64) error {
	f.deletes = append(f.deletes, deleteCall{key: key, ts: ts})
	return nil
}

func (f *fakeWriteBatch) Flush() error {
	f.flushed = true
	return f.flushErr
}

func (f *fakeWriteBatch) Cancel() {
	f.cancelled = true
}

type fakeOp struct {
	key       []byte
	value     []byte
	userMeta  byte
	expiresAt uint64
}

type fakeWrites struct {
	ops []fakeOp
	err error
}

func (f *fakeWrites) Iterator([]byte, bool) pending.Iterator { return nil }
func (f *fakeWrites) Get([]byte) (pending.Item, error)       { return nil, nil }
func (f *fakeWrites) Set(*badger.Entry)                      {}
func (f *fakeWrites) Delete([]byte)                          {}
func (f *fakeWrites) Replay(func(*badger.Entry) error) error { return nil }
func (f *fakeWrites) Close() error                           { return nil }
func (f *fakeWrites) ReplayRaw(fn func(key, value []byte, userMeta byte, expiresAt uint64) error) error {
	if f.err != nil {
		return f.err
	}
	for _, op := range f.ops {
		if err := fn(op.key, op.value, op.userMeta, op.expiresAt); err != nil {
			return err
		}
	}
	return nil
}
