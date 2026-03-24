package rpcerrs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	for _, v := range entries {
		t.Run(v.reason, func(t *testing.T) {
			require := require.New(t)
			e := ToStatus(v.err)
			require.Error(e)
			require.ErrorIs(FromStatus(e), v.err)
			e = ToStatus(fmt.Errorf("wrapped: %w", v.err))
			require.Error(e)
			require.ErrorIs(FromStatus(e), v.err)
		})
	}
}

func TestFromMessage(t *testing.T) {
	for _, v := range entries {
		t.Run(v.reason, func(t *testing.T) {
			require := require.New(t)
			require.ErrorIs(FromMessage(v.err.Error()), v.err)
			require.ErrorIs(FromMessage("failed op: "+v.err.Error()), v.err)
		})
	}
}
