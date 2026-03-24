package tests

import (
	"testing"
	"time"

	assert2 "github.com/stretchr/testify/assert"
	require2 "github.com/stretchr/testify/require"
)

func waitNoError(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	require2.EventuallyWithT(t, func(c *assert2.CollectT) {
		assert2.NoError(c, fn())
	}, timeout, 20*time.Millisecond)
}
