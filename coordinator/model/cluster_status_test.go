package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClusterStatus_Clone(t *testing.T) {
	cs1 := &ClusterStatus{
		ReplicationFactor: 3,
		Shards: map[uint32]ShardMetadata{
			0: {
				Status: ShardStatusSteadyState,
				Epoch:  1,
				Leader: &ServerAddress{
					Public:   "l1",
					Internal: "l1",
				},
				Ensemble: []ServerAddress{{
					Public:   "f1",
					Internal: "f1",
				}, {
					Public:   "f2",
					Internal: "f2",
				}},
				Int32HashRange: Int32HashRange{},
			},
		},
	}

	cs2 := cs1.Clone()

	assert.Equal(t, cs1, cs2)
	assert.NotSame(t, cs1, cs2)
	assert.Equal(t, cs1.Shards, cs2.Shards)
	assert.NotSame(t, cs1.Shards, cs2.Shards)
	assert.Equal(t, cs1.Shards[0], cs2.Shards[0])
	assert.NotSame(t, cs1.Shards[0], cs2.Shards[0])
}
