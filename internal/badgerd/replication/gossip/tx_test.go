package gossip

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.linka.cloud/protodb/internal/badgerd/replication"
	"go.linka.cloud/protodb/internal/badgerd/replication/async"
	pb "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func TestNewTxModesAndReplicateError(t *testing.T) {
	r := &Gossip{db: &fakeDB{}, mode: replication.ModeSync}
	r.nodes.Store("a", &node{name: "a", repl: &fakeReplicateClient{replicate: &fakeReplicateStream{}}})

	txv, err := r.NewTx(context.Background())
	require.NoError(t, err)
	txr := txv.(*tx)
	require.Len(t, txr.cs, 1)
	assert.NotNil(t, txr.cs[0].s)
	assert.Nil(t, txr.cs[0].q)

	r2 := &Gossip{db: &fakeDB{}, mode: replication.ModeAsync}
	r2.nodes.Store("a", &node{name: "a", repl: &fakeReplicateClient{replicate: &fakeReplicateStream{}}})

	txv, err = r2.NewTx(context.Background())
	require.NoError(t, err)
	txr = txv.(*tx)
	require.Len(t, txr.cs, 1)
	assert.NotNil(t, txr.cs[0].q)
	assert.Nil(t, txr.cs[0].s)

	r3 := &Gossip{db: &fakeDB{}, mode: replication.ModeSync}
	r3.nodes.Store("a", &node{name: "a", repl: &fakeReplicateClient{replicateErr: errors.New("boom")}})
	_, err = r3.NewTx(context.Background())
	require.EqualError(t, err, "boom")
}

func TestTxDoSizedNewFlushesBuffer(t *testing.T) {
	s := &fakeReplicateStream{acks: []*pb.Ack{{}, {}}}
	r := &tx{mode: replication.ModeSync, cs: []*stream{{n: "n1", s: s}}}

	err := r.doSized(context.Background(), &pb.Op{Action: &pb.Op_Set{Set: &pb.Set{Key: []byte("k"), Value: []byte("v")}}}, 8)
	require.NoError(t, err)
	require.NoError(t, r.do(context.Background(), &pb.Op{Action: &pb.Op_New{New: &pb.New{At: 1}}}))

	require.Len(t, s.sent, 2)
	require.Len(t, s.sent[0].Ops, 1)
	_, ok := s.sent[0].Ops[0].Action.(*pb.Op_Set)
	assert.True(t, ok)
	require.Len(t, s.sent[1].Ops, 1)
	_, ok = s.sent[1].Ops[0].Action.(*pb.Op_New)
	assert.True(t, ok)
}

func TestTxDoSizedCommitSyncSendsSingleBatch(t *testing.T) {
	s := &fakeReplicateStream{acks: []*pb.Ack{{}}}
	r := &tx{mode: replication.ModeSync, cs: []*stream{{n: "n1", s: s}}}

	require.NoError(t, r.do(context.Background(), &pb.Op{Action: &pb.Op_Set{Set: &pb.Set{Key: []byte("k"), Value: []byte("v")}}}))
	require.NoError(t, r.do(context.Background(), &pb.Op{Action: &pb.Op_Commit{Commit: &pb.Commit{At: 9}}}))

	require.Len(t, s.sent, 1)
	require.Len(t, s.sent[0].Ops, 2)
	_, ok := s.sent[0].Ops[0].Action.(*pb.Op_Set)
	assert.True(t, ok)
	_, ok = s.sent[0].Ops[1].Action.(*pb.Op_Commit)
	assert.True(t, ok)
}

func TestTxHandleErrTable(t *testing.T) {
	s := &stream{n: "n1"}
	r := &tx{cs: []*stream{s}}

	err := r.handleErr(context.Background(), async.NewResult(s, errors.New("send")))
	require.Error(t, err)
	assert.False(t, r.hasStreams(s))

	s2 := &stream{n: "n2"}
	r = &tx{cs: []*stream{s2}}
	err = r.handleErr(context.Background(), async.NewResult(s2, io.EOF))
	require.NoError(t, err)
	assert.True(t, r.hasStreams(s2))
}

func TestTxCommitReplaySetDeleteAndFlush(t *testing.T) {
	batch := &fakeWriteBatch{}
	db := &fakeDB{batch: batch}
	r := &tx{
		db:     db,
		mode:   replication.ModeSync,
		readTs: 5,
		cs:     []*stream{{n: "n1", s: &fakeReplicateStream{acks: []*pb.Ack{{}}}}},
		w: &fakeWrites{replayRawFn: func(fn func([]byte, []byte, byte, uint64) error) error {
			require.NoError(t, fn([]byte("k1"), []byte("v1"), 0, 11))
			require.NoError(t, fn([]byte("k2"), nil, 1, 0))
			return nil
		}},
	}

	require.NoError(t, r.Commit(context.Background(), 42))
	require.True(t, batch.flushed)
	require.Len(t, batch.sets, 1)
	assert.Equal(t, []byte("k1"), batch.sets[0].key)
	assert.Equal(t, []byte("v1"), batch.sets[0].value)
	assert.EqualValues(t, 42, batch.sets[0].ts)
	require.Len(t, batch.dels, 1)
	assert.Equal(t, []byte("k2"), batch.dels[0].key)
	assert.EqualValues(t, 42, batch.dels[0].ts)
}
