package gossip

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"

	pb "go.linka.cloud/protodb/internal/badgerd/replication/gossip/pb"
)

func TestInitPreconditionsAndStreamPath(t *testing.T) {
	r := &Gossip{leading: NewAtomic(false), db: &fakeDB{maxVersion: 2}}
	err := r.Init(&pb.InitRequest{Since: 0}, &fakeInitSrv{ctx: context.Background()})
	require.ErrorContains(t, err, "cannot initialize from non-leader")

	r = &Gossip{leading: NewAtomic(true), db: &fakeDB{maxVersion: 2}}
	err = r.Init(&pb.InitRequest{Since: 0}, &fakeInitSrv{ctx: context.Background()})
	require.ErrorContains(t, err, "cannot get peer from context")

	r = &Gossip{leading: NewAtomic(true), db: &fakeDB{maxVersion: 2}, nodes: Map[*node]{}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.IPAddr{IP: net.ParseIP("127.0.0.1")}})
	err = r.Init(&pb.InitRequest{Since: 0}, &fakeInitSrv{ctx: ctx})
	require.ErrorContains(t, err, "failed to split host port")

	r = &Gossip{leading: NewAtomic(true), db: &fakeDB{maxVersion: 2}, nodes: Map[*node]{}}
	err = r.Init(&pb.InitRequest{Since: 0}, &fakeInitSrv{ctx: peerCtx("127.0.0.1", 7000)})
	require.ErrorContains(t, err, "is not in the cluster")

	r = &Gossip{leading: NewAtomic(true), db: &fakeDB{maxVersion: 2}, nodes: Map[*node]{}}
	r.nodes.Store("n1", &node{name: "n1", addr: net.ParseIP("127.0.0.1")})
	err = r.Init(&pb.InitRequest{Since: 3}, &fakeInitSrv{ctx: peerCtx("127.0.0.1", 7000)})
	require.ErrorContains(t, err, "invalid replication version")

	called := false
	db := &fakeDB{maxVersion: 5, streamFn: func(_ context.Context, at, since uint64, _ io.Writer) error {
		called = true
		assert.EqualValues(t, 5, at)
		assert.EqualValues(t, 2, since)
		return nil
	}}
	r = &Gossip{leading: NewAtomic(true), db: db, nodes: Map[*node]{}}
	r.nodes.Store("n1", &node{name: "n1", addr: net.ParseIP("127.0.0.1")})

	err = r.Init(&pb.InitRequest{Since: 2}, &fakeInitSrv{ctx: peerCtx("127.0.0.1", 7000)})
	require.NoError(t, err)
	assert.True(t, called)

	called = false
	err = r.Init(&pb.InitRequest{Since: 5}, &fakeInitSrv{ctx: peerCtx("127.0.0.1", 7000)})
	require.NoError(t, err)
	assert.False(t, called)
}

func TestReplicatePreconditions(t *testing.T) {
	r := &Gossip{leading: NewAtomic(true)}
	err := r.Replicate(&fakeReplicateSrv{ctx: context.Background()})
	require.ErrorContains(t, err, "cannot replicate to leader")

	r = &Gossip{leading: NewAtomic(false), name: "self", db: &fakeDB{path: t.TempDir(), maxBatchCount: 10, maxBatchSize: 1 << 20, valueThr: 1024}}
	err = r.Replicate(&fakeReplicateSrv{ctx: context.Background()})
	require.ErrorContains(t, err, "cannot get peer from context")

	r = &Gossip{leading: NewAtomic(false), name: "self", db: &fakeDB{path: t.TempDir(), maxBatchCount: 10, maxBatchSize: 1 << 20, valueThr: 1024}}
	err = r.Replicate(&fakeReplicateSrv{ctx: peerCtx("127.0.0.1", 7000), recvErr: errors.New("recv")})
	require.EqualError(t, err, "recv")
}

func TestAliveTable(t *testing.T) {
	r := &Gossip{}
	err := r.Alive(&fakeAliveSrv{ctx: context.Background(), recvErrs: []error{io.EOF}})
	require.NoError(t, err)

	err = r.Alive(&fakeAliveSrv{ctx: context.Background(), recvErrs: []error{errors.New("recv")}})
	require.EqualError(t, err, "recv")

	err = r.Alive(&fakeAliveSrv{ctx: context.Background(), recvErrs: []error{nil}, sendErrs: []error{io.EOF}})
	require.NoError(t, err)

	err = r.Alive(&fakeAliveSrv{ctx: context.Background(), recvErrs: []error{nil}, sendErrs: []error{errors.New("send")}})
	require.EqualError(t, err, "send")
}
