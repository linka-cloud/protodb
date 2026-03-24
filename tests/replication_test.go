package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/stretchr/testify/require"

	"go.linka.cloud/protodb"
	testpb "go.linka.cloud/protodb/tests/pb"
)

func TestReplicationModes(t *testing.T) {
	for _, mode := range []protodb.ReplicationMode{protodb.ReplicationModeAsync, protodb.ReplicationModeSync} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Run("leader_failover_write_continuity", func(t *testing.T) {
				withRealReplicationCluster(t, mode, func(ctx context.Context, c *Cluster) {
					cli, leader, err := leaderClient(ctx, c)
					require.NoError(t, err)
					require.NoError(t, insertRange(ctx, cli, "failover", 0, 40))

					require.NoError(t, c.Stop(leader))

					var failoverCli protodb.Client
					var failoverLeader int
					waitNoError(t, 30*time.Second, func() error {
						var e error
						failoverCli, failoverLeader, e = leaderClient(ctx, c)
						if e != nil {
							return e
						}
						if failoverLeader == leader {
							return fmt.Errorf("still on old leader %d", leader)
						}
						return nil
					})

					require.NoError(t, insertRange(ctx, failoverCli, "failover", 40, 80))

					require.NoError(t, c.Start(ctx, leader))
					waitNoError(t, 30*time.Second, func() error {
						return assertClusterNames(ctx, c, []int{0, 1, 2}, expectedNames("failover", 80))
					})
				})
			})

			t.Run("follower_catchup_after_downtime", func(t *testing.T) {
				withRealReplicationCluster(t, mode, func(ctx context.Context, c *Cluster) {
					_, leader, err := leaderClient(ctx, c)
					require.NoError(t, err)

					follower := (leader + 1) % 3
					require.NoError(t, c.Stop(follower))

					cli, _, err := leaderClient(ctx, c)
					require.NoError(t, err)
					require.NoError(t, insertRange(ctx, cli, "catchup", 0, 60))

					require.NoError(t, c.Start(ctx, follower))
					waitNoError(t, 30*time.Second, func() error {
						return assertClusterNames(ctx, c, []int{0, 1, 2}, expectedNames("catchup", 60))
					})
				})
			})
		})
	}
}

func withRealReplicationCluster(t *testing.T, mode protodb.ReplicationMode, fn func(ctx context.Context, c *Cluster)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	path := filepath.Join(data, fmt.Sprintf("TestReplicationModes-%s-%s", mode.String(), t.Name()))
	require.NoError(t, os.RemoveAll(path))
	defer os.RemoveAll(path)

	c := NewCluster(path, 3, mode, protodb.WithApplyDefaults(true))
	require.NoError(t, c.StartAll(ctx))
	defer func() {
		require.NoError(t, c.StopAll())
	}()

	fn(ctx, c)
}

func leaderClient(ctx context.Context, c *Cluster) (protodb.Client, int, error) {
	for i := range c.dbs {
		db := c.Get(i)
		if db == nil || !db.IsLeader() {
			continue
		}
		srv, err := protodb.NewServer(db)
		if err != nil {
			return nil, -1, err
		}
		tr := &inprocgrpc.Channel{}
		srv.RegisterService(tr)
		cli, err := protodb.NewClient(tr)
		if err != nil {
			return nil, -1, err
		}
		_, _, err = cli.Get(ctx, &testpb.Interface{}, protodb.WithPaging(&protodb.Paging{Limit: 1}))
		if err != nil {
			continue
		}
		return cli, i, nil
	}
	return nil, -1, fmt.Errorf("leader not found")
}

func insertRange(ctx context.Context, cli protodb.Client, prefix string, start, end int) error {
	tx, err := cli.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	for i := start; i < end; i++ {
		if _, err := tx.Set(ctx, &testpb.Interface{Name: fmt.Sprintf("%s-%04d", prefix, i)}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func expectedNames(prefix string, count int) []string {
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		names = append(names, fmt.Sprintf("%s-%04d", prefix, i))
	}
	return names
}

func assertClusterNames(ctx context.Context, c *Cluster, nodes []int, want []string) error {
	w := slices.Clone(want)
	sort.Strings(w)
	for _, idx := range nodes {
		db := c.Get(idx)
		if db == nil {
			return fmt.Errorf("node %d db is nil", idx)
		}
		msgs, _, err := db.Get(ctx, &testpb.Interface{})
		if err != nil {
			return fmt.Errorf("node %d get: %w", idx, err)
		}
		got := make([]string, 0, len(msgs))
		for _, m := range msgs {
			v, ok := m.(*testpb.Interface)
			if !ok {
				return fmt.Errorf("node %d unexpected type %T", idx, m)
			}
			got = append(got, v.Name)
		}
		sort.Strings(got)
		if !slices.Equal(w, got) {
			return fmt.Errorf("node %d mismatch: got=%d want=%d", idx, len(got), len(w))
		}
	}
	return nil
}
