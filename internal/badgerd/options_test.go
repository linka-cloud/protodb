package badgerd

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

type testLogger struct{}

func (testLogger) Errorf(string, ...any)   {}
func (testLogger) Warningf(string, ...any) {}
func (testLogger) Infof(string, ...any)    {}
func (testLogger) Debugf(string, ...any)   {}
func (testLogger) Tracef(string, ...any)   {}

func TestWithPathTable(t *testing.T) {
	o := options{}
	WithPath(":memory:")(&o)
	assert.True(t, o.inMemory)
	assert.Equal(t, "", o.path)

	o = options{}
	WithPath("/tmp/pdb")(&o)
	assert.False(t, o.inMemory)
	assert.Equal(t, "/tmp/pdb", o.path)
}

func TestWithInMemoryTable(t *testing.T) {
	o := options{path: "/tmp/pdb"}
	WithInMemory(true)(&o)
	assert.True(t, o.inMemory)
	assert.Equal(t, "", o.path)

	WithInMemory(false)(&o)
	assert.False(t, o.inMemory)
	assert.Equal(t, "", o.path)
}

func TestOptionsBuildAppliesBadgerOptionsFunc(t *testing.T) {
	o := options{inMemory: true, logger: testLogger{}}
	WithBadgerOptionsFunc(func(opts badger.Options) badger.Options {
		opts.SyncWrites = false
		opts.NumMemtables = 7
		return opts
	})(&o)

	b := o.build()
	assert.True(t, b.InMemory)
	assert.False(t, b.SyncWrites)
	assert.Equal(t, 7, b.NumMemtables)
}
