package impl

import (
	"github.com/stretchr/testify/assert"
	"oxia/coordinator/model"
	"testing"
)

func TestShardStatus_String(t *testing.T) {
	assert.Equal(t, "Unknown", model.ShardStatusUnknown.String())
	assert.Equal(t, "SteadyState", model.ShardStatusSteadyState.String())
	assert.Equal(t, "Election", model.ShardStatusElection.String())
}

func TestShardStatus_JSON(t *testing.T) {
	j, err := model.ShardStatusSteadyState.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, []byte("\"SteadyState\""), j)

	var s model.ShardStatus
	err = s.UnmarshalJSON(j)
	assert.NoError(t, err)
	assert.Equal(t, model.ShardStatusSteadyState, s)

	err = s.UnmarshalJSON([]byte("xyz"))
	assert.Error(t, err)
}
