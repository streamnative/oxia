package single

import (
	"testing"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
)

func TestDefaultShardsRank(t *testing.T) {
	params := &model.RatioParams{
		NodeShardsInfos: map[string][]model.ShardInfo{
			"sv-1": {
				{Namespace: "ns-1", ShardID: 1, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-2", ShardID: 2, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-3", ShardID: 3, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-4", ShardID: 4, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-3"}, {Internal: "sv-4"}}},
				{Namespace: "ns-5", ShardID: 5, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-4"}, {Internal: "sv-5"}}},
			},
			"sv-2": {
				{Namespace: "ns-1", ShardID: 1, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-2", ShardID: 2, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-3", ShardID: 3, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
			},
			"sv-3": {
				{Namespace: "ns-1", ShardID: 1, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-2", ShardID: 2, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-3", ShardID: 3, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-4", ShardID: 4, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-3"}, {Internal: "sv-4"}}},
			},
			"sv-4": {
				{Namespace: "ns-4", ShardID: 4, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-3"}, {Internal: "sv-4"}}},
				{Namespace: "ns-5", ShardID: 5, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-4"}, {Internal: "sv-5"}}},
			},
			"sv-5": {
				{Namespace: "ns-5", ShardID: 5, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-4"}, {Internal: "sv-5"}}},
			},
		},
	}
	ratios := DefaultShardsRank(params)
	highestLoadNode := ratios.DequeueHighestNode()
	assert.NotNil(t, highestLoadNode)
	assert.Equal(t, "sv-1", highestLoadNode.NodeID)
	assert.Equal(t, 0.3333333333333333, highestLoadNode.Ratio)
	shardRatios := highestLoadNode.ShardRatios
	assert.Equal(t, 5, shardRatios.Size())

	highestLoadNode = ratios.DequeueHighestNode()
	assert.NotNil(t, highestLoadNode)
	assert.Equal(t, "sv-3", highestLoadNode.NodeID)
	assert.Equal(t, 0.26666666666666666, highestLoadNode.Ratio)
	shardRatios = highestLoadNode.ShardRatios
	assert.Equal(t, 4, shardRatios.Size())

	highestLoadNode = ratios.DequeueHighestNode()
	assert.NotNil(t, highestLoadNode)
	assert.Equal(t, "sv-2", highestLoadNode.NodeID)
	assert.Equal(t, 0.2, highestLoadNode.Ratio)
	shardRatios = highestLoadNode.ShardRatios
	assert.Equal(t, 3, shardRatios.Size())

	highestLoadNode = ratios.DequeueHighestNode()
	assert.NotNil(t, highestLoadNode)
	assert.Equal(t, "sv-4", highestLoadNode.NodeID)
	assert.Equal(t, 0.13333333333333333, highestLoadNode.Ratio)
	shardRatios = highestLoadNode.ShardRatios
	assert.Equal(t, 2, shardRatios.Size())

	highestLoadNode = ratios.DequeueHighestNode()
	assert.NotNil(t, highestLoadNode)
	assert.Equal(t, "sv-5", highestLoadNode.NodeID)
	assert.Equal(t, 0.06666666666666667, highestLoadNode.Ratio)
	shardRatios = highestLoadNode.ShardRatios
	assert.Equal(t, 1, shardRatios.Size())
}
