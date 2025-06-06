// Copyright 2023 StreamNative, Inc.
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
	"log/slog"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/common/collection"

	"github.com/streamnative/oxia/coordinator/model"
)

func TestClusterRebalance_Count(t *testing.T) {
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s1, s2, s3},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32,
						},
					},
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					1: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
	}

	count, deletedServers := getShardsPerServer([]model.Server{s1, s2, s3, s4, s5}, cs)

	assert.Equal(t, map[string]ServerContext{
		s1.Internal: {
			s1,
			collection.NewSetFrom[int64]([]int64{0, 1, 2}),
		},
		s2.Internal: {
			s2,
			collection.NewSetFrom[int64]([]int64{0, 1}),
		},
		s3.Internal: {
			s3,
			collection.NewSetFrom[int64]([]int64{0, 2}),
		},
		s4.Internal: {
			s4,
			collection.NewSetFrom[int64]([]int64{1, 2}),
		},
		s5.Internal: {
			s5,
			collection.NewSet[int64](),
		},
	}, count)

	assert.Equal(t, map[string]ServerContext{}, deletedServers)
}

func TestClusterRebalance_Single(t *testing.T) {
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s1, s2, s3},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32,
						},
					},
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					1: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
	}

	actions := rebalanceCluster([]model.Server{s1, s2, s3, s4, s5}, cs)
	assert.Equal(t, []SwapNodeAction{{
		Shard: 0,
		From:  s1,
		To:    s5,
	}}, actions)
}

func TestClusterRebalance_Multiple(t *testing.T) {
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				Shards: map[int64]model.ShardMetadata{
					0: {Ensemble: []model.Server{s1, s2, s3}},
					1: {Ensemble: []model.Server{s2, s3, s4}},
					2: {Ensemble: []model.Server{s4, s1, s2}},
					3: {Ensemble: []model.Server{s1, s2, s3}},
					4: {Ensemble: []model.Server{s2, s3, s4}},
				},
			},
		},
	}

	actions := rebalanceCluster([]model.Server{s1, s2, s3, s4, s5}, cs)
	slog.Info(
		"actions",
		slog.Any("actions", actions),
	)
	assert.Equal(t, []SwapNodeAction{{
		Shard: 0,
		From:  s2,
		To:    s5,
	}, {
		Shard: 1,
		From:  s2,
		To:    s5,
	}, {
		Shard: 3,
		From:  s3,
		To:    s5,
	}}, actions)
}

func TestClusterRebalance_DoubleSize(t *testing.T) {
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				Shards: map[int64]model.ShardMetadata{
					0: {Ensemble: []model.Server{s1, s2, s3}},
					1: {Ensemble: []model.Server{s2, s3, s1}},
					2: {Ensemble: []model.Server{s3, s1, s2}},
					3: {Ensemble: []model.Server{s1, s2, s3}},
					4: {Ensemble: []model.Server{s2, s3, s1}},
					5: {Ensemble: []model.Server{s3, s1, s2}},
				},
			},
		},
	}

	actions := rebalanceCluster([]model.Server{s1, s2, s3, s4, s5, s6}, cs)
	slog.Info(
		"actions",
		slog.Any("actions", actions),
	)
	assert.Equal(t, []SwapNodeAction{{
		Shard: 0,
		From:  s1,
		To:    s6,
	}, {
		Shard: 0,
		From:  s2,
		To:    s5,
	}, {
		Shard: 0,
		From:  s3,
		To:    s4,
	}, {
		Shard: 1,
		From:  s1,
		To:    s6,
	}, {
		Shard: 1,
		From:  s2,
		To:    s5,
	}, {
		Shard: 1,
		From:  s3,
		To:    s4,
	}, {
		Shard: 2,
		From:  s1,
		To:    s6,
	}, {
		Shard: 2,
		From:  s2,
		To:    s5,
	}, {
		Shard: 2,
		From:  s3,
		To:    s4,
	}}, actions)
}

func TestClusterRebalance_ShrinkOne(t *testing.T) {
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				Shards: map[int64]model.ShardMetadata{
					0: {Ensemble: []model.Server{s1, s2, s3}},
					1: {Ensemble: []model.Server{s4, s5, s6}},
					2: {Ensemble: []model.Server{s1, s2, s3}},
					3: {Ensemble: []model.Server{s4, s5, s6}},
					4: {Ensemble: []model.Server{s1, s2, s3}},
					5: {Ensemble: []model.Server{s4, s5, s6}},
				},
			},
		},
	}

	actions := rebalanceCluster([]model.Server{s1, s2, s3, s4, s5}, cs)
	slog.Info(
		"actions",
		slog.Any("actions", actions),
	)
	assert.Equal(t, []SwapNodeAction{{
		Shard: 1,
		From:  s6,
		To:    s3,
	}, {
		Shard: 3,
		From:  s6,
		To:    s2,
	}, {
		Shard: 5,
		From:  s6,
		To:    s1,
	}}, actions)
}

func TestClusterRebalance_ShrinkToHalf(t *testing.T) {
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				Shards: map[int64]model.ShardMetadata{
					0: {Ensemble: []model.Server{s1, s2, s3}},
					1: {Ensemble: []model.Server{s4, s5, s6}},
					2: {Ensemble: []model.Server{s1, s2, s3}},
					3: {Ensemble: []model.Server{s4, s5, s6}},
					4: {Ensemble: []model.Server{s1, s2, s3}},
					5: {Ensemble: []model.Server{s4, s5, s6}},
				},
			},
		},
	}

	actions := rebalanceCluster([]model.Server{s1, s2, s3}, cs)
	slog.Info(
		"actions",
		slog.Any("actions", actions),
	)
	assert.Equal(t, []SwapNodeAction{{
		Shard: 1,
		From:  s4,
		To:    s3,
	}, {
		Shard: 3,
		From:  s4,
		To:    s2,
	}, {
		Shard: 5,
		From:  s4,
		To:    s1,
	}, {
		Shard: 3,
		From:  s5,
		To:    s3,
	}, {
		Shard: 1,
		From:  s5,
		To:    s2,
	}, {
		Shard: 5,
		From:  s5,
		To:    s3,
	}, {
		Shard: 1,
		From:  s6,
		To:    s1,
	}, {
		Shard: 5,
		From:  s6,
		To:    s2,
	}, {
		Shard: 3,
		From:  s6,
		To:    s1,
	}}, actions)
}
