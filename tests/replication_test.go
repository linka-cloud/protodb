package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"go.linka.cloud/protodb"
	testpb "go.linka.cloud/protodb/tests/pb"
)

func TestReplicationModes(t *testing.T) {
	for _, mode := range []protodb.ReplicationMode{protodb.ReplicationModeAsync, protodb.ReplicationModeSync} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Run("leader_churn_under_writes", func(t *testing.T) {
				withRealReplicationCluster(t, mode, func(ctx context.Context, c *Cluster) {
					cli, leader, err := leaderClient(ctx, c)
					require.NoError(t, err)
					require.NoError(t, insertRange(ctx, cli, "churn", 0, 30))

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

					require.NoError(t, insertRange(ctx, failoverCli, "churn", 30, 70))

					require.NoError(t, c.Start(ctx, leader))
					waitNoError(t, 30*time.Second, func() error {
						return assertClusterNames(ctx, c, []int{0, 1, 2}, expectedNames("churn", 70))
					})
				})
			})

			t.Run("follower_offline_catchup_under_load", func(t *testing.T) {
				withRealReplicationCluster(t, mode, func(ctx context.Context, c *Cluster) {
					_, leader, err := leaderClient(ctx, c)
					require.NoError(t, err)

					follower := (leader + 1) % 3
					require.NoError(t, c.Stop(follower))

					cli, _, err := leaderClient(ctx, c)
					require.NoError(t, err)
					require.NoError(t, insertRange(ctx, cli, "catchup", 0, 120))

					require.NoError(t, c.Start(ctx, follower))
					waitNoError(t, 30*time.Second, func() error {
						return assertClusterNames(ctx, c, []int{0, 1, 2}, expectedNames("catchup", 120))
					})
				})
			})

			t.Run("rolling_restart_no_data_loss", func(t *testing.T) {
				withRealReplicationCluster(t, mode, func(ctx context.Context, c *Cluster) {
					cli, _, err := leaderClient(ctx, c)
					require.NoError(t, err)

					require.NoError(t, insertRange(ctx, cli, "rolling", 0, 30))
					total := 30

					for i := range 3 {
						require.NoError(t, c.Stop(i))

						waitNoError(t, 30*time.Second, func() error {
							_, leader, e := leaderClient(ctx, c)
							if e != nil {
								return e
							}
							if leader == i {
								return fmt.Errorf("stopped node %d still leader", i)
							}
							return nil
						})

						cli, _, err = leaderClient(ctx, c)
						require.NoError(t, err)
						require.NoError(t, insertRange(ctx, cli, "rolling", total, total+20))
						total += 20

						require.NoError(t, c.Start(ctx, i))
						waitNoError(t, 30*time.Second, func() error {
							return assertClusterNames(ctx, c, []int{0, 1, 2}, expectedNames("rolling", total))
						})
					}
				})
			})

			t.Run("delete_propagation_no_resurrection", func(t *testing.T) {
				withRealReplicationCluster(t, mode, func(ctx context.Context, c *Cluster) {
					cli, _, err := leaderClient(ctx, c)
					require.NoError(t, err)

					require.NoError(t, insertRange(ctx, cli, "delete", 0, 80))
					require.NoError(t, deleteByStep(ctx, cli, "delete", 0, 80, 2))
					require.NoError(t, insertRange(ctx, cli, "delete", 80, 100))

					waitNoError(t, 30*time.Second, func() error {
						return assertClusterNames(ctx, c, []int{0, 1, 2}, expectedDeletedNames("delete", 0, 80, 2, 80, 100))
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
	leaders := make([]int, 0, len(c.dbs))
	for i := range c.dbs {
		db := c.Get(i)
		if db == nil || !db.IsLeader() {
			continue
		}
		leaders = append(leaders, i)
	}
	if len(leaders) != 1 {
		return nil, -1, fmt.Errorf("expected exactly one leader, got %v", leaders)
	}
	i := leaders[0]
	db := c.Get(i)
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
		return nil, -1, err
	}
	return cli, i, nil
}

func insertRange(ctx context.Context, cli protodb.Client, prefix string, start, end int) error {
	tx, err := cli.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	for i := start; i < end; i++ {
		if _, err := tx.Set(ctx, &testpb.Interface{
			Name:   fmt.Sprintf("%s-%04d", prefix, i),
			Mac:    wrapperspb.String(fmt.Sprintf("00:00:00:%02x:%02x:%02x", (i>>16)&0xff, (i>>8)&0xff, i&0xff)),
			Status: testpb.InterfaceStatus(i % 4),
			Mtu:    uint32(1400 + (i % 200)),
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func deleteByStep(ctx context.Context, cli protodb.Client, prefix string, start, end, step int) error {
	tx, err := cli.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	for i := start; i < end; i += step {
		if err := tx.Delete(ctx, &testpb.Interface{Name: fmt.Sprintf("%s-%04d", prefix, i)}); err != nil {
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

func expectedDeletedNames(prefix string, deleteStart, deleteEnd, step, appendStart, appendEnd int) []string {
	names := make([]string, 0, deleteEnd-deleteStart+appendEnd-appendStart)
	for i := deleteStart; i < deleteEnd; i++ {
		if (i-deleteStart)%step == 0 {
			continue
		}
		names = append(names, fmt.Sprintf("%s-%04d", prefix, i))
	}
	for i := appendStart; i < appendEnd; i++ {
		names = append(names, fmt.Sprintf("%s-%04d", prefix, i))
	}
	return names
}

func assertClusterNames(ctx context.Context, c *Cluster, nodes []int, want []string) error {
	w := slices.Clone(want)
	sort.Strings(w)
	var base map[string]string
	leaders := make([]int, 0, len(nodes))
	for _, idx := range nodes {
		db := c.Get(idx)
		if db == nil {
			return fmt.Errorf("node %d db is nil", idx)
		}
		if db.IsLeader() {
			leaders = append(leaders, idx)
		}
		msgs, _, err := db.Get(ctx, &testpb.Interface{})
		if err != nil {
			return fmt.Errorf("node %d get: %w", idx, err)
		}
		got := make([]string, 0, len(msgs))
		vals := make(map[string]string, len(msgs))
		for _, m := range msgs {
			v, ok := m.(*testpb.Interface)
			if !ok {
				return fmt.Errorf("node %d unexpected type %T", idx, m)
			}
			got = append(got, v.Name)
			vals[v.Name] = valueSig(v)
		}
		sort.Strings(got)
		if !slices.Equal(w, got) {
			missing := diff(w, got)
			extra := diff(got, w)
			return fmt.Errorf("node %d mismatch: leaders=%v got=%d want=%d delta=%d missing=[%s] extra=[%s]", idx, leaders, len(got), len(w), len(got)-len(w), sample(missing, 5), sample(extra, 5))
		}
		if base == nil {
			base = vals
			continue
		}
		if missing, changed := diffValues(base, vals); len(missing) > 0 || len(changed) > 0 {
			return fmt.Errorf("node %d value mismatch: leaders=%v missing_keys=[%s] changed_values=[%s]", idx, leaders, sample(missing, 5), sample(changed, 5))
		}
	}
	return nil
}

func valueSig(v *testpb.Interface) string {
	mac := ""
	if v.Mac != nil {
		mac = v.Mac.Value
	}
	return fmt.Sprintf("mac=%s,status=%d,mtu=%d", mac, v.Status, v.Mtu)
}

func diffValues(want, got map[string]string) ([]string, []string) {
	missing := make([]string, 0)
	changed := make([]string, 0)
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			missing = append(missing, k)
			continue
		}
		if g != w {
			changed = append(changed, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(changed)
	return missing, changed
}

func diff(a, b []string) []string {
	out := make([]string, 0)
	j := 0
	for _, x := range a {
		for j < len(b) && b[j] < x {
			j++
		}
		if j >= len(b) || b[j] != x {
			out = append(out, x)
		}
	}
	return out
}

func sample(values []string, n int) string {
	if len(values) == 0 {
		return ""
	}
	if len(values) > n {
		return strings.Join(values[:n], ",") + ",..."
	}
	return strings.Join(values, ",")
}
