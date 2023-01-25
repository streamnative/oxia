package oxia

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptionalPresent(t *testing.T) {
	o := optionalOf(1)
	assert.True(t, o.Present())
	assert.False(t, o.Empty())
	v, ok := o.Get()
	assert.True(t, ok)
	assert.Equal(t, 1, v)
}

func TestOptionalEmpty(t *testing.T) {
	e := empty[string]()
	assert.False(t, e.Present())
	assert.True(t, e.Empty())
	es, ok := e.Get()
	assert.False(t, ok)
	assert.Equal(t, "", es)
}
