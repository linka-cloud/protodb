package badgerd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOracleHasConflictTable(t *testing.T) {
	const (
		k1 = uint64(101)
		k2 = uint64(202)
	)

	tests := []struct {
		name      string
		reads     []uint64
		readTs    uint64
		committed []committedTxn
		want      bool
	}{
		{
			name:      "no_reads_no_conflict",
			reads:     nil,
			readTs:    10,
			committed: []committedTxn{{ts: 11, conflictKeys: map[uint64]struct{}{k1: {}}}},
			want:      false,
		},
		{
			name:   "committed_before_read_ts_no_conflict",
			reads:  []uint64{k1},
			readTs: 10,
			committed: []committedTxn{
				{ts: 9, conflictKeys: map[uint64]struct{}{k1: {}}},
				{ts: 10, conflictKeys: map[uint64]struct{}{k1: {}}},
			},
			want: false,
		},
		{
			name:   "committed_after_read_ts_conflicts",
			reads:  []uint64{k1},
			readTs: 10,
			committed: []committedTxn{
				{ts: 11, conflictKeys: map[uint64]struct{}{k1: {}}},
			},
			want: true,
		},
		{
			name:   "committed_after_read_ts_different_key_no_conflict",
			reads:  []uint64{k1},
			readTs: 10,
			committed: []committedTxn{
				{ts: 11, conflictKeys: map[uint64]struct{}{k2: {}}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &oracle{committedTxns: tt.committed}
			tx := &tx{reads: tt.reads, readTs: tt.readTs}
			assert.Equal(t, tt.want, o.hasConflict(tx))
		})
	}
}
