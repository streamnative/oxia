package balancer

import (
	"github.com/emirpasic/gods/queues/priorityqueue"
	"github.com/streamnative/oxia/coordinator/model"
)

type shardInfo struct {
	namespace string
	shardID   int64
	ensemble  []model.Server
}

type LoadRatioParams struct {
	nodeShardsInfos map[string][]shardInfo
}

type LoadRatio struct {
	maxNodeLoadRatio float64
	minNodeLoadRatio float64
	nodeLoadRatios   *priorityqueue.Queue
}

type NodeLoadRatio struct {
	nodeID      string
	ratio       float64
	shardRatios *priorityqueue.Queue
}

type ShardLoadRatio struct {
	*shardInfo
	ratio float64
}
