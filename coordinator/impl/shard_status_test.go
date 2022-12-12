package impl

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShardStatus_String(t *testing.T) {
	assert.Equal(t, "Unknown", ShardStatusUnknown.String())
	assert.Equal(t, "SteadyState", ShardStatusSteadyState.String())
	assert.Equal(t, "Election", ShardStatusElection.String())
}

func TestShardStatus_JSON(t *testing.T) {
	j, err := ShardStatusSteadyState.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, []byte("\"SteadyState\""), j)

	var s ShardStatus
	err = s.UnmarshalJSON(j)
	assert.NoError(t, err)
	assert.Equal(t, ShardStatusSteadyState, s)

	err = s.UnmarshalJSON([]byte("xyz"))
	assert.Error(t, err)
}
