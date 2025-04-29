package utils

import (
	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func GroupingShardsNodeByStatus(status *model.ClusterStatus) map[string][]model.ShardInfo {
	groupingShardByNode := make(map[string][]model.ShardInfo)
	for namespace, namespaceStatus := range status.Namespaces {
		for shard, shardStatus := range namespaceStatus.Shards {
			for _, server := range shardStatus.Ensemble {
				nodeID := server.GetIdentifier()
				var groupedShard []model.ShardInfo
				var exist bool
				if groupedShard, exist = groupingShardByNode[nodeID]; !exist {
					tmp := make([]model.ShardInfo, 0)
					groupingShardByNode[nodeID] = tmp
					groupedShard = tmp
					continue
				}
				groupedShard = append(groupedShard, model.ShardInfo{
					Namespace: namespace,
					ShardID:   shard,
					Ensemble:  shardStatus.Ensemble,
				})
			}
		}
	}
	return groupingShardByNode
}

func GroupingCandidatesWithLabelValue(candidates *linkedhashset.Set, candidatesMetadata map[string]model.ServerMetadata) map[string]map[string]*linkedhashset.Set {
	groupedCandidates := make(map[string]map[string]*linkedhashset.Set)
	for iterator := candidates.Iterator(); iterator.Next(); {
		candidate, ok := iterator.Value().(string)
		if !ok {
			panic("unexpected type in iterator")
		}
		metadata, exist := candidatesMetadata[candidate]
		if !exist {
			continue
		}
		for label, labelValue := range metadata.Labels {
			labelGroup, exist := groupedCandidates[label]
			if !exist {
				tmp := make(map[string]*linkedhashset.Set)
				groupedCandidates[label] = tmp
				labelGroup = tmp
			}
			labelValueGroup, exist := labelGroup[labelValue]
			if !exist {
				tmp := linkedhashset.New()
				labelGroup[labelValue] = tmp
				labelValueGroup = tmp
			}
			labelValueGroup.Add(candidate)
		}
	}
	return groupedCandidates
}

func GroupingValueWithLabel(candidates *linkedhashset.Set, candidatesMetadata map[string]model.ServerMetadata) map[string]*linkedhashset.Set {
	selectedLabelValues := make(map[string]*linkedhashset.Set)
	for selectedIter := candidates.Iterator(); selectedIter.Next(); {
		if metadata, exist := candidatesMetadata[selectedIter.Value().(string)]; exist {
			for label, labelValue := range metadata.Labels {
				collection := selectedLabelValues[label]
				if collection == nil {
					collection = linkedhashset.New(labelValue)
				} else {
					collection.Add(labelValue)
				}
				selectedLabelValues[label] = collection
			}
		}
	}
	return selectedLabelValues
}
