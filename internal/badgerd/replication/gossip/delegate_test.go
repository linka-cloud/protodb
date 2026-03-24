package gossip

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelegateMetaTable(t *testing.T) {
	d := &delegate{}

	d.SetMeta([]byte("meta"))
	assert.Equal(t, []byte("meta"), d.NodeMeta(4))

	assert.PanicsWithValue(t, "meta too long", func() {
		d.NodeMeta(3)
	})
}

func TestDelegateNoopMethods(t *testing.T) {
	d := &delegate{}
	assert.Nil(t, d.GetBroadcasts(0, 0))
	assert.Nil(t, d.LocalState(false))
	assert.NotPanics(t, func() {
		d.NotifyMsg([]byte("x"))
		d.MergeRemoteState([]byte("x"), true)
	})
}
