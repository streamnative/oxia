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

package single

import (
	"github.com/emirpasic/gods/lists/arraylist"

	"github.com/streamnative/oxia/coordinator/model"
)

func DefaultShardsRank(params *model.RatioParams) *model.Ratio {
	totalShards := 0
	for _, shards := range params.NodeShardsInfos {
		totalShards += len(shards)
	}
	fTotalShards := float64(totalShards)

	// the 1 means we are using count as a ratio.
	shardLoadRatio := 1 / fTotalShards

	nodeLoadRatios := arraylist.New()

	first := true
	maxNodeLoadRatio := 0.0
	minNodeLoadRatio := 0.0

	for nodeID, shards := range params.NodeShardsInfos {
		if params.QuarantineNodes != nil {
			if _, found := params.QuarantineNodes[nodeID]; found {
				continue
			}
		}

		shardRatios := arraylist.New()
		for _, info := range shards {
			shardRatios.Add(&model.ShardLoadRatio{
				ShardInfo: &info,
				Ratio:     shardLoadRatio,
			})
		}
		nodeLoadRatio := float64(len(shards)) / fTotalShards

		if first {
			maxNodeLoadRatio = nodeLoadRatio
			minNodeLoadRatio = nodeLoadRatio
			first = false
		} else {
			if nodeLoadRatio > maxNodeLoadRatio {
				maxNodeLoadRatio = nodeLoadRatio
			} else if nodeLoadRatio < minNodeLoadRatio {
				minNodeLoadRatio = nodeLoadRatio
			}
		}

		shardRatios.Sort(model.ShardLoadRatioComparator)
		nodeLoadRatios.Add(&model.NodeLoadRatio{
			NodeID:      nodeID,
			Ratio:       nodeLoadRatio,
			ShardRatios: shardRatios,
		})
	}
	nodeLoadRatios.Sort(model.NodeLoadRatioComparator)
	return model.NewRatio(maxNodeLoadRatio, minNodeLoadRatio, shardLoadRatio, nodeLoadRatios)
}
