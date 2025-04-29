package single

import (
	"github.com/emirpasic/gods/queues/priorityqueue"
	"github.com/streamnative/oxia/coordinator/model"
)

func DefaultShardsRank(params *model.RatioParams) *model.Ratio {
	totalShards := 0
	for _, shards := range params.NodeShardsInfos {
		totalShards = totalShards + len(shards)
	}
	nodeLoadRatios := priorityqueue.NewWith(model.NodeLoadRatioComparator)
	maxNodeLoadRatio := 0.0
	minNodeLoadRatio := 0.0
	for nodeID, shards := range params.NodeShardsInfos {
		shardRatios := priorityqueue.NewWith(model.ShardLoadRatioComparator)
		for _, info := range shards {
			shardRatios.Enqueue(&model.ShardLoadRatio{
				ShardInfo: &info,
				Ratio:     1.0,
			})
		}
		nodeLoadRatio := float64(len(shards)) / float64(totalShards)
		if nodeLoadRatio > maxNodeLoadRatio {
			maxNodeLoadRatio = nodeLoadRatio
		} else if nodeLoadRatio < minNodeLoadRatio {
			minNodeLoadRatio = nodeLoadRatio
		}
		nodeLoadRatios.Enqueue(&model.NodeLoadRatio{
			NodeID:      nodeID,
			Ratio:       nodeLoadRatio,
			ShardRatios: shardRatios,
		})
	}
	return model.NewRatio(maxNodeLoadRatio, minNodeLoadRatio, nodeLoadRatios)
}
