package gossip

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	pubsub "go.linka.cloud/pubsub/typed"
	"google.golang.org/grpc"

	"go.linka.cloud/protodb/internal/badgerd/replication"
	pb2 "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func TestGossipSimpleMethodsTable(t *testing.T) {
	r := &Gossip{
		mode:       replication.ModeSync,
		leading:    NewAtomic(false),
		leaderName: NewAtomic(""),
		nodes:      Map[*node]{},
		pub:        pubsub.NewPublisher[string](time.Second, 2),
	}

	ch := r.Subscribe()
	assert.NotNil(t, ch)

	assert.False(t, r.IsLeader())
	assert.False(t, r.HasLeader())
	assert.Equal(t, "", r.CurrentLeader())
	assert.Equal(t, replication.ModeSync, r.Mode())
	assert.True(t, r.LinearizableReads())

	r.mode = replication.ModeAsync
	assert.False(t, r.LinearizableReads())

	r.leaderName.Store("peer")
	repl := &fakeReplicateClient{}
	cc := &grpc.ClientConn{}
	n := &node{name: "peer", repl: repl, cc: cc}
	r.nodes.Store("peer", n)

	leader, ok := r.leaderClient()
	assert.True(t, ok)
	assert.NotNil(t, leader)
	assert.Same(t, repl, leader)

	conn, ok := r.LeaderConn()
	assert.True(t, ok)
	assert.NotNil(t, conn)
	assert.Same(t, cc, conn)
}

func TestRunSkipsElectionWhenLeaderAlreadyKnown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := &Gossip{
		ctx:          ctx,
		name:         "self",
		shuttingDown: NewAtomic(false),
		leading:      NewAtomic(false),
		leaderName:   NewAtomic("peer"),
		bootNodes:    []string{"peer"},
		ready:        make(chan struct{}),
		events:       make(chan memberlist.NodeEvent),
		converged:    make(chan struct{}),
		pub:          pubsub.NewPublisher[string](time.Second, 2),
	}
	r.meta.Store(&pb2.Meta{LocalVersion: 1})
	close(r.converged)

	r.run(ctx)
	t.Cleanup(func() { close(r.events) })

	time.Sleep(1500 * time.Millisecond)

	assert.Equal(t, "peer", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	assert.False(t, r.HasLeader() && r.CurrentLeader() == "self")
	select {
	case <-r.ready:
		t.Fatal("expected ready to stay open")
	default:
	}
}
