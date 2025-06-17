// Copyright 2025 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func GroupingShardsNodeByStatus(candidates *linkedhashset.Set[string], status *model.ClusterStatus) map[string][]model.ShardInfo {
	groupingShardByNode := make(map[string][]model.ShardInfo)
	if status != nil {
		for namespace, namespaceStatus := range status.Namespaces {
			for shard, shardStatus := range namespaceStatus.Shards {
				for _, server := range shardStatus.Ensemble {
					nodeID := server.GetIdentifier()
					var groupedShard []model.ShardInfo
					var exist bool
					if groupedShard, exist = groupingShardByNode[nodeID]; !exist {
						tmp := make([]model.ShardInfo, 0)
						groupedShard = tmp
					}
					groupedShard = append(groupedShard, model.ShardInfo{
						Namespace: namespace,
						ShardID:   shard,
						Ensemble:  shardStatus.Ensemble,
					})
					groupingShardByNode[nodeID] = groupedShard
				}
			}
		}
	}
	for iter := candidates.Iterator(); iter.Next(); {
		nodeID := iter.Value()
		_, exist := groupingShardByNode[nodeID]
		if !exist {
			groupingShardByNode[nodeID] = make([]model.ShardInfo, 0)
		}
	}
	return groupingShardByNode
}

func GroupingCandidatesWithLabelValue(candidates *linkedhashset.Set[string], candidatesMetadata map[string]model.ServerMetadata) map[string]map[string]*linkedhashset.Set[string] {
	groupedCandidates := make(map[string]map[string]*linkedhashset.Set[string])
	for iterator := candidates.Iterator(); iterator.Next(); {
		candidate := iterator.Value()
		metadata, exist := candidatesMetadata[candidate]
		if !exist {
			continue
		}
		for label, labelValue := range metadata.Labels {
			labelGroup, exist := groupedCandidates[label]
			if !exist {
				tmp := make(map[string]*linkedhashset.Set[string])
				groupedCandidates[label] = tmp
				labelGroup = tmp
			}
			labelValueGroup, exist := labelGroup[labelValue]
			if !exist {
				tmp := linkedhashset.New[string]()
				labelGroup[labelValue] = tmp
				labelValueGroup = tmp
			}
			labelValueGroup.Add(candidate)
		}
	}
	return groupedCandidates
}

func GroupingValueWithLabel(candidates *linkedhashset.Set[string], candidatesMetadata map[string]model.ServerMetadata) map[string]*linkedhashset.Set[string] {
	selectedLabelValues := make(map[string]*linkedhashset.Set[string])
	for selectedIter := candidates.Iterator(); selectedIter.Next(); {
		if metadata, exist := candidatesMetadata[selectedIter.Value()]; exist {
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
