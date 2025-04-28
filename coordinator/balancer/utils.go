package balancer

import (
	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func filterEnsemble(ensemble []model.Server, filterNodeId string) *linkedhashset.Set {
	selected := linkedhashset.New()
	for _, candidate := range ensemble {
		nodeID := candidate.GetIdentifier()
		if nodeID == filterNodeId {
			continue
		}
		selected.Add(candidate.GetIdentifier())
	}
	return selected
}

func groupingShardsNodeByStatus(status *model.ClusterStatus) map[string][]shardInfo {
	groupingShardByNode := make(map[string][]shardInfo)
	for namespace, namespaceStatus := range status.Namespaces {
		for shard, shardStatus := range namespaceStatus.Shards {
			for _, server := range shardStatus.Ensemble {
				nodeID := server.GetIdentifier()
				var groupedShard []shardInfo
				var exist bool
				if groupedShard, exist = groupingShardByNode[nodeID]; !exist {
					tmp := make([]shardInfo, 0)
					groupingShardByNode[nodeID] = tmp
					groupedShard = tmp
					continue
				}
				groupedShard = append(groupedShard, shardInfo{
					namespace: namespace,
					shardID:   shard,
					ensemble:  shardStatus.Ensemble,
				})
			}
		}
	}
	return groupingShardByNode
}
