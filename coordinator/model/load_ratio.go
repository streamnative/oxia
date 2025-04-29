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
	"github.com/emirpasic/gods/queues/priorityqueue"
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

type Ratio struct {
	maxNodeLoadRatio float64
	minNodeLoadRatio float64
	nodeLoadRatios   *priorityqueue.Queue
}

func (r *Ratio) MaxNodeLoadRatio() float64 {
	return r.maxNodeLoadRatio
}

func (r *Ratio) MinNodeLoadRatio() float64 {
	return r.minNodeLoadRatio
}

func (r *Ratio) NodeLoadRatios() *priorityqueue.Queue { return r.nodeLoadRatios }

func (r *Ratio) RatioGap() float64 {
	return r.maxNodeLoadRatio - r.minNodeLoadRatio
}

func (r *Ratio) PeekHighestNode() *NodeLoadRatio {
	if v, ok := r.nodeLoadRatios.Peek(); ok {
		return v.(*NodeLoadRatio) //nolint:revive
	}
	return nil
}

func (r *Ratio) DequeueHighestNode() *NodeLoadRatio {
	if v, ok := r.nodeLoadRatios.Dequeue(); ok {
		return v.(*NodeLoadRatio) //nolint:revive
	}
	return nil
}

func (r *Ratio) MoveShardToNode(shard *ShardLoadRatio, nodeID string) {
	iterator := r.nodeLoadRatios.Iterator()
	iterator.Last()
	for iterator.Prev() {
		node := iterator.Value().(*NodeLoadRatio) //nolint:revive
		if node.NodeID == nodeID {
			node.EnqueueShard(shard)
			return
		}
	}
}

func (r *Ratio) ReCalculateRatios() {
	for iter := r.nodeLoadRatios.Iterator(); iter.Next(); {
		nodeLoadRatio := iter.Value().(*NodeLoadRatio) //nolint:revive
		if nodeLoadRatio.Ratio > r.maxNodeLoadRatio {
			r.maxNodeLoadRatio = nodeLoadRatio.Ratio
		} else if nodeLoadRatio.Ratio < r.minNodeLoadRatio {
			r.minNodeLoadRatio = nodeLoadRatio.Ratio
		}
	}
}

func NewRatio(maxNodeLoadRatio float64, minNodeLoadRatio float64, nodeLoadRatios *priorityqueue.Queue) *Ratio {
	return &Ratio{
		maxNodeLoadRatio: maxNodeLoadRatio,
		minNodeLoadRatio: minNodeLoadRatio,
		nodeLoadRatios:   nodeLoadRatios,
	}
}

type NodeLoadRatio struct {
	NodeID      string
	Ratio       float64
	ShardRatios *priorityqueue.Queue
}

func (n *NodeLoadRatio) PeekHighestShard() *ShardLoadRatio {
	if v, ok := n.ShardRatios.Peek(); ok {
		return v.(*ShardLoadRatio) //nolint:revive
	}
	return nil
}
func (n *NodeLoadRatio) EnqueueShard(shard *ShardLoadRatio) {
	n.Ratio += shard.Ratio
	n.ShardRatios.Enqueue(shard)
}

func (n *NodeLoadRatio) DequeueHighestShard() *ShardLoadRatio {
	if v, ok := n.ShardRatios.Dequeue(); ok {
		shardLoadRatio := v.(*ShardLoadRatio) //nolint:revive
		n.Ratio -= shardLoadRatio.Ratio
		return shardLoadRatio
	}
	return nil
}

type ShardLoadRatio struct {
	*ShardInfo
	Ratio float64
}

func NodeLoadRatioComparator(a, b any) int {
	tmpA := a.(*NodeLoadRatio) //nolint:revive
	tmpB := b.(*NodeLoadRatio) //nolint:revive
	return -utils.Float64Comparator(tmpA.Ratio, tmpB.Ratio)
}

func ShardLoadRatioComparator(a, b any) int {
	tmpA := a.(*ShardLoadRatio) //nolint:revive
	tmpB := b.(*ShardLoadRatio) //nolint:revive
	return -utils.Float64Comparator(tmpA.Ratio, tmpB.Ratio)
}
