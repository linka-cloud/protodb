package gossip

import (
	"context"
	"errors"
	"testing"
	"time"

	pubsub "go.linka.cloud/pubsub/typed"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb2 "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func TestElectFoundLeaderPeerUsesIt(t *testing.T) {
	r := newElectionTestGossip("self", 10)
	r.elect(context.Background(), []electionPeer{
		{name: "n1", meta: &pb2.Meta{IsLeader: true, LocalVersion: 12}, repl: &fakeElectionClient{}},
	}, &pb2.Meta{LocalVersion: 10})

	assert.Equal(t, "n1", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	requireReady(t, r.ready)
}

func TestElectOkResponseStopsElection(t *testing.T) {
	r := newElectionTestGossip("b", 10)
	r.elect(context.Background(), []electionPeer{
		{name: "a", meta: &pb2.Meta{LocalVersion: 10}, repl: &fakeElectionClient{res: &pb2.Message{Name: "a", Type: pb2.ElectionTypeOk}}},
	}, &pb2.Meta{LocalVersion: 10})

	assert.Equal(t, "", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	notReady(t, r.ready)
}

func TestElectSkipThenLeaderUsesLeaderResponse(t *testing.T) {
	r := newElectionTestGossip("c", 10)
	leaderClient := &fakeElectionClient{res: &pb2.Message{Name: "a", Type: pb2.ElectionTypeLeader}}
	r.elect(context.Background(), []electionPeer{
		{name: "b", meta: &pb2.Meta{LocalVersion: 10}, repl: &fakeElectionClient{res: &pb2.Message{Name: "b", Type: pb2.ElectionTypeSkip}}},
		{name: "a", meta: &pb2.Meta{LocalVersion: 10}, repl: leaderClient},
	}, &pb2.Meta{LocalVersion: 10})

	assert.Equal(t, "a", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	assert.Equal(t, 1, leaderClient.calls)
	requireReady(t, r.ready)
}

func TestElectErrorStopsElection(t *testing.T) {
	r := newElectionTestGossip("b", 10)
	errClient := &fakeElectionClient{err: errors.New("boom")}
	second := &fakeElectionClient{res: &pb2.Message{Name: "a", Type: pb2.ElectionTypeLeader}}
	r.elect(context.Background(), []electionPeer{
		{name: "a", meta: &pb2.Meta{LocalVersion: 10}, repl: errClient},
		{name: "aa", meta: &pb2.Meta{LocalVersion: 10}, repl: second},
	}, &pb2.Meta{LocalVersion: 10})

	assert.Equal(t, 1, errClient.calls)
	assert.Equal(t, 0, second.calls)
	assert.Equal(t, "", r.CurrentLeader())
	notReady(t, r.ready)
}

func TestElectionWhenNotReadyReturnsSkip(t *testing.T) {
	r := newElectionTestGossip("self", 3)
	res, err := r.Election(context.Background(), &pb2.Message{Type: pb2.ElectionTypeElection, Name: "peer", Meta: &pb2.Meta{LocalVersion: 3}})
	require.NoError(t, err)
	assert.Equal(t, pb2.ElectionTypeSkip, res.Type)
	assert.Equal(t, "self", res.Name)
}

func TestElectionWhileShuttingDownReturnsSkip(t *testing.T) {
	r := newElectionTestGossip("self", 3)
	r.setReady()
	r.leading.Store(true)
	r.leaderName.Store("self")
	r.shuttingDown.Store(true)

	res, err := r.Election(context.Background(), &pb2.Message{Type: pb2.ElectionTypeElection, Name: "peer", Meta: &pb2.Meta{LocalVersion: 3}})
	require.NoError(t, err)
	assert.Equal(t, pb2.ElectionTypeSkip, res.Type)
	assert.Equal(t, "self", res.Name)
	assert.Equal(t, "self", r.CurrentLeader())
	assert.True(t, r.IsLeader())
}

func TestElectionWhenLeaderReturnsLeaderMessage(t *testing.T) {
	r := newElectionTestGossip("self", 7)
	r.setReady()
	r.leading.Store(true)
	r.leaderName.Store("self")

	res, err := r.Election(context.Background(), &pb2.Message{Type: pb2.ElectionTypeElection, Name: "peer", Meta: &pb2.Meta{LocalVersion: 7}})
	require.NoError(t, err)
	assert.Equal(t, pb2.ElectionTypeLeader, res.Type)
	assert.Equal(t, "self", res.Name)
	require.NotNil(t, res.Meta)
	assert.EqualValues(t, 7, res.Meta.LocalVersion)
}

func TestElectionLeaderMessageKeepsPreferredLeader(t *testing.T) {
	r := newElectionTestGossip("self", 7)
	r.setReady()
	r.leading.Store(true)
	r.leaderName.Store("self")

	res, err := r.Election(context.Background(), &pb2.Message{Type: pb2.ElectionTypeLeader, Name: "z-peer", Meta: &pb2.Meta{LocalVersion: 7}})
	require.NoError(t, err)
	assert.Equal(t, pb2.ElectionTypeLeader, res.Type)
	assert.Equal(t, "self", res.Name)
	assert.Equal(t, "self", r.CurrentLeader())
	assert.True(t, r.IsLeader())
}

func TestElectionLeaderMessageDemotesExistingLeader(t *testing.T) {
	r := newElectionTestGossip("self", 7)
	r.setReady()
	r.leading.Store(true)
	r.leaderName.Store("self")

	res, err := r.Election(context.Background(), &pb2.Message{Type: pb2.ElectionTypeLeader, Name: "peer", Meta: &pb2.Meta{LocalVersion: 7}})
	require.NoError(t, err)
	assert.Equal(t, "self", res.Name)
	assert.Equal(t, "peer", r.CurrentLeader())
	assert.False(t, r.IsLeader())
}

func TestElectionLeaderMessageSetsLeaderWhenNotReady(t *testing.T) {
	r := newElectionTestGossip("self", 1)

	res, err := r.Election(context.Background(), &pb2.Message{Type: pb2.ElectionTypeLeader, Name: "peer", Meta: &pb2.Meta{LocalVersion: 2}})
	require.NoError(t, err)
	assert.Equal(t, "self", res.Name)
	assert.Equal(t, "peer", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	requireReady(t, r.ready)
}

func TestElectWhileShuttingDownDoesNothing(t *testing.T) {
	r := newElectionTestGossip("self", 3)
	r.shuttingDown.Store(true)
	client := &fakeElectionClient{res: &pb2.Message{Name: "peer", Type: pb2.ElectionTypeLeader}}

	r.elect(context.Background(), []electionPeer{
		{name: "peer", meta: &pb2.Meta{LocalVersion: 3}, repl: client},
	}, &pb2.Meta{LocalVersion: 3})

	assert.Equal(t, 0, client.calls)
	assert.Equal(t, "", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	notReady(t, r.ready)
}

type fakeElectionClient struct {
	res   *pb2.Message
	err   error
	calls int
}

func (f *fakeElectionClient) Election(_ context.Context, _ *pb2.Message, _ ...any) (*pb2.Message, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.res == nil {
		return &pb2.Message{}, nil
	}
	return f.res.CloneVT(), nil
}

func newElectionTestGossip(name string, version uint64) *Gossip {
	r := &Gossip{
		ctx:          context.Background(),
		name:         name,
		shuttingDown: NewAtomic(false),
		leading:      NewAtomic(false),
		leaderName:   NewAtomic(""),
		ready:        make(chan struct{}),
		pub:          pubsub.NewPublisher[string](time.Second, 2),
	}
	r.meta.Store(&pb2.Meta{LocalVersion: version})
	return r
}

func requireReady(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatal("expected ready channel to be closed")
	}
}

func notReady(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("expected ready channel to stay open")
	default:
	}
}
