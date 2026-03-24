package miekgdns

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolverLookupRecursionLimitTable(t *testing.T) {
	r := &Resolver{}

	_, _, err := r.lookupSRV("", "", "example.org", 9, 8)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maximum number of recursive iterations reached")

	_, err = r.lookupIPAddr("example.org", 9, 8)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maximum number of recursive iterations reached")
}

func TestResolverIsNotFoundTable(t *testing.T) {
	r := &Resolver{}

	assert.False(t, r.IsNotFound(nil))
	assert.True(t, r.IsNotFound(ErrNoSuchHost))
	assert.True(t, r.IsNotFound(errors.Wrap(ErrNoSuchHost, "wrapped")))
	assert.False(t, r.IsNotFound(errors.New("other")))
}

func TestFmtErrsJoinsAllErrors(t *testing.T) {
	got := fmtErrs([]error{errors.New("a"), errors.New("b")})
	assert.Equal(t, ";a;b", got)
}
