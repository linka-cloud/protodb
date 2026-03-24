package gossip

import (
	"testing"
	"time"

	pubsub "go.linka.cloud/pubsub/typed"
	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"

	"go.linka.cloud/protodb/internal/badgerd/replication"
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
