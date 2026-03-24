package tests

import (
	"testing"
	"time"

	require2 "github.com/stretchr/testify/require"
)

func waitNoError(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	var err error
	require2.Eventuallyf(t, func() bool {
		err = fn()
		return err == nil
	}, timeout, 20*time.Millisecond, "condition not met: %v", err)
}
