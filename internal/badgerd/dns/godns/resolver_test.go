package godns

import (
	"net"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestResolverIsNotFoundTable(t *testing.T) {
	r := &Resolver{}

	assert.False(t, r.IsNotFound(nil))
	assert.True(t, r.IsNotFound(&net.DNSError{IsNotFound: true}))
	assert.False(t, r.IsNotFound(&net.DNSError{IsNotFound: false}))
	assert.True(t, r.IsNotFound(errors.Wrap(&net.DNSError{IsNotFound: true}, "wrapped")))
	assert.False(t, r.IsNotFound(errors.New("other")))
}
