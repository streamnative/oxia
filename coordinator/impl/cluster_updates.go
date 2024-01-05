// Copyright 2024 StreamNative, Inc.
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

package impl

import (
	"oxia/common"
	"oxia/coordinator/model"
)

func getServers(servers []model.ServerAddress, startIdx uint32, count uint32) []model.ServerAddress {
	n := len(servers)
	res := make([]model.ServerAddress, count)
	for i := uint32(0); i < count; i++ {
		res[i] = servers[int(startIdx+i)%n]
	}
	return res
}

func applyClusterChanges(config *model.ClusterConfig, currentStatus *model.ClusterStatus) (
	newStatus *model.ClusterStatus,
	shardsToAdd map[int64]string,
	shardsToDelete []int64) {

	shardsToAdd = map[int64]string{}
	shardsToDelete = []int64{}

	newStatus = &model.ClusterStatus{
		Namespaces:       map[string]model.NamespaceStatus{},
		ShardIdGenerator: currentStatus.ShardIdGenerator,
		ServerIdx:        currentStatus.ServerIdx,
	}

	// Check for new namespaces
	for _, nc := range config.Namespaces {
		nss, existing := currentStatus.Namespaces[nc.Name]
		if !existing {
			// This is a new namespace
			nss = model.NamespaceStatus{
				Shards:            map[int64]model.ShardMetadata{},
				ReplicationFactor: nc.ReplicationFactor,
			}
			for _, shard := range common.GenerateShards(newStatus.ShardIdGenerator, nc.InitialShardCount) {
				shardMetadata := model.ShardMetadata{
					Status:   model.ShardStatusUnknown,
					Term:     -1,
					Leader:   nil,
					Ensemble: getServers(config.Servers, newStatus.ServerIdx, nc.ReplicationFactor),
					Int32HashRange: model.Int32HashRange{
						Min: shard.Min,
						Max: shard.Max,
					},
				}

				nss.Shards[shard.Id] = shardMetadata
				newStatus.ServerIdx = (newStatus.ServerIdx + nc.ReplicationFactor) % uint32(len(config.Servers))
				shardsToAdd[shard.Id] = nc.Name
			}
			newStatus.Namespaces[nc.Name] = nss

			newStatus.ShardIdGenerator += int64(nc.InitialShardCount)
		} else {
			// The namespace was already existing, nothing special to do
			newStatus.Namespaces[nc.Name] = nss.Clone()
		}
	}

	// Check for any namespace that was removed
	for name, ns := range currentStatus.Namespaces {
		namespaceStillExists := false
		for _, cns := range config.Namespaces {
			if name == cns.Name {
				namespaceStillExists = true
				break
			}
		}

		if !namespaceStillExists {
			// Keep the shards in the status and mark them as being deleted
			nss := ns.Clone()
			for shardId, shard := range nss.Shards {
				shard.Status = model.ShardStatusDeleting
				nss.Shards[shardId] = shard
				shardsToDelete = append(shardsToDelete, shardId)
			}

			newStatus.Namespaces[name] = nss
		}
	}

	return newStatus, shardsToAdd, shardsToDelete
}
