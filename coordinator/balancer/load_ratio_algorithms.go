package balancer

import (
	"github.com/emirpasic/gods/queues/priorityqueue"
)

func DefaultShardsRank(params *LoadRatioParams) *LoadRatio {
	totalShards := 0
	for _, shards := range params.nodeShardsInfos {
		totalShards = totalShards + len(shards)
	}
	nodeLoadRatios := &priorityqueue.Queue{}
	maxNodeLoadRatio := 0.0
	minNodeLoadRatio := 0.0
	for nodeID, shards := range params.nodeShardsInfos {
		shardRatios := &priorityqueue.Queue{}
		for _, info := range shards {
			shardRatios.Enqueue(&ShardLoadRatio{
				shardInfo: &info,
				ratio:     1.0,
			})
		}
		nodeLoadRatio := float64(len(shards)) / float64(totalShards)
		if nodeLoadRatio > maxNodeLoadRatio {
			maxNodeLoadRatio = nodeLoadRatio
		}
		if nodeLoadRatio < minNodeLoadRatio {
			minNodeLoadRatio = nodeLoadRatio
		}
		nodeLoadRatios.Enqueue(&NodeLoadRatio{
			nodeID:      nodeID,
			ratio:       nodeLoadRatio,
			shardRatios: shardRatios,
		})
	}
	return &LoadRatio{
		maxNodeLoadRatio: maxNodeLoadRatio,
		minNodeLoadRatio: minNodeLoadRatio,
		nodeLoadRatios:   nodeLoadRatios,
	}
}
