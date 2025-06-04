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

package model

import (
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/emirpasic/gods/utils"
)

type ShardInfo struct {
	Namespace string
	ShardID   int64
	Ensemble  []Server
}

type RatioParams struct {
	NodeShardsInfos map[string][]ShardInfo
}

type RatioSnapshot struct {
	maxNodeLoadRatio float64
	minNodeLoadRatio float64
	NodeLoadRatios   *arraylist.List
}

func (r *RatioSnapshot) MaxNodeLoadRatio() float64 {
	return r.maxNodeLoadRatio
}

func (r *RatioSnapshot) MinNodeLoadRatio() float64 {
	return r.minNodeLoadRatio
}

func (r *RatioSnapshot) RatioGap() float64 {
	return r.maxNodeLoadRatio - r.minNodeLoadRatio
}

func (r *RatioSnapshot) MoveShardToNode(shard *ShardLoadRatio, nodeID string) {
	iterator := r.NodeLoadRatios.Iterator()
	iterator.Last()
	for iterator.Prev() {
		node := iterator.Value().(*NodeLoadRatio) //nolint:revive
		if node.NodeID == nodeID {
			node.AddShard(shard)
			return
		}
	}
}

func (r *RatioSnapshot) ReCalculateRatios() {
	for iter := r.NodeLoadRatios.Iterator(); iter.Next(); {
		nodeLoadRatio := iter.Value().(*NodeLoadRatio) //nolint:revive
		if nodeLoadRatio.Ratio > r.maxNodeLoadRatio {
			r.maxNodeLoadRatio = nodeLoadRatio.Ratio
		} else if nodeLoadRatio.Ratio < r.minNodeLoadRatio {
			r.minNodeLoadRatio = nodeLoadRatio.Ratio
		}
	}
}

func NewRatio(maxNodeLoadRatio float64, minNodeLoadRatio float64, nodeLoadRatios *arraylist.List) *RatioSnapshot {
	return &RatioSnapshot{
		maxNodeLoadRatio: maxNodeLoadRatio,
		minNodeLoadRatio: minNodeLoadRatio,
		NodeLoadRatios:   nodeLoadRatios,
	}
}

type NodeLoadRatio struct {
	NodeID      string
	Ratio       float64
	ShardRatios *arraylist.List
}

func (n *NodeLoadRatio) AddShard(shard *ShardLoadRatio) {
	n.Ratio += shard.Ratio
	n.ShardRatios.Add(shard)
}

type ShardLoadRatio struct {
	*ShardInfo
	Ratio float64
}

func NodeLoadRatioComparator(a, b any) int {
	tmpA := a.(*NodeLoadRatio) //nolint:revive
	tmpB := b.(*NodeLoadRatio) //nolint:revive
	return utils.Float64Comparator(tmpA.Ratio, tmpB.Ratio)
}

func ShardLoadRatioComparator(a, b any) int {
	tmpA := a.(*ShardLoadRatio) //nolint:revive
	tmpB := b.(*ShardLoadRatio) //nolint:revive
	return utils.Float64Comparator(tmpA.Ratio, tmpB.Ratio)
}
