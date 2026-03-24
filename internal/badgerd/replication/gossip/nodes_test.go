package gossip

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pubsub "go.linka.cloud/pubsub/typed"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func TestLoadNodesTable(t *testing.T) {
	r := &Gossip{name: "self", db: &fakeDB{inMemory: true}}
	nodes, err := r.loadNodes()
	require.NoError(t, err)
	assert.Nil(t, nodes)

	d := t.TempDir()
	r = &Gossip{name: "self", db: &fakeDB{path: d}}
	nodes, err = r.loadNodes()
	require.NoError(t, err)
	assert.Nil(t, nodes)

	require.NoError(t, os.WriteFile(filepath.Join(d, "nodes"), []byte("self\nnode-a\n\nnode-b\n"), 0o644))
	nodes, err = r.loadNodes()
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a", "node-b"}, nodes)
}

func TestOnNewLeaderFollowerPath(t *testing.T) {
	r := &Gossip{
		ctx:        context.Background(),
		name:       "self",
		leading:    NewAtomic(false),
		leaderName: NewAtomic(""),
		ready:      make(chan struct{}),
		pub:        pubsub.NewPublisher[string](time.Second, 2),
	}
	r.meta.Store(&pb.Meta{LocalVersion: 3})

	r.onNewLeader(context.Background(), "peer")

	assert.Equal(t, "peer", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	assert.True(t, r.HasLeader())
	select {
	case <-r.ready:
	default:
		t.Fatal("expected ready channel to be closed")
	}
}

func TestOnNewLeaderEmptyIdentityIgnored(t *testing.T) {
	r := &Gossip{
		ctx:        context.Background(),
		name:       "self",
		leading:    NewAtomic(false),
		leaderName: NewAtomic("peer"),
		ready:      make(chan struct{}),
		pub:        pubsub.NewPublisher[string](time.Second, 2),
	}
	r.meta.Store(&pb.Meta{LocalVersion: 3})

	r.onNewLeader(context.Background(), "")

	assert.Equal(t, "peer", r.CurrentLeader())
	assert.False(t, r.IsLeader())
	assert.True(t, r.HasLeader())
	select {
	case <-r.ready:
		t.Fatal("ready must not close on empty leader")
	default:
	}
}
