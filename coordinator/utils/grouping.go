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
