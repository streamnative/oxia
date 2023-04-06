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
	"github.com/stretchr/testify/assert"
	"math"
	"oxia/coordinator/model"
	"sort"
	"testing"
)

var (
	s1 = model.ServerAddress{Public: "s1:6648", Internal: "s1:6649"}
	s2 = model.ServerAddress{Public: "s2:6648", Internal: "s2:6649"}
	s3 = model.ServerAddress{Public: "s3:6648", Internal: "s3:6649"}
	s4 = model.ServerAddress{Public: "s4:6648", Internal: "s4:6649"}
)

func TestClientUpdates_ClusterInit(t *testing.T) {
	newStatus, shardsAdded, shardsToRemove := applyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: []model.ServerAddress{s1, s2, s3, s4},
	}, model.NewClusterStatus())

	assert.Equal(t, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s1, s2, s3},
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
						Ensemble: []model.ServerAddress{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, newStatus)

	assert.Equal(t, []int64{}, shardsToRemove)
	assert.Equal(t, map[int64]string{
		0: "ns-1",
		1: "ns-2",
		2: "ns-2"}, shardsAdded)
}

func TestClientUpdates_NamespaceAdded(t *testing.T) {
	newStatus, shardsAdded, shardsToRemove := applyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: []model.ServerAddress{s1, s2, s3, s4},
	}, &model.ClusterStatus{Namespaces: map[string]model.NamespaceStatus{
		"ns-1": {
			ReplicationFactor: 3,
			Shards: map[int64]model.ShardMetadata{
				0: {
					Status:   model.ShardStatusUnknown,
					Term:     -1,
					Leader:   nil,
					Ensemble: []model.ServerAddress{s1, s2, s3},
					Int32HashRange: model.Int32HashRange{
						Min: 0,
						Max: math.MaxUint32,
					},
				},
			},
		},
	}, ShardIdGenerator: 1,
		ServerIdx: 3})

	assert.Equal(t, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s1, s2, s3},
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
						Ensemble: []model.ServerAddress{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, newStatus)

	assert.Equal(t, []int64{}, shardsToRemove)
	assert.Equal(t, map[int64]string{
		1: "ns-2",
		2: "ns-2"}, shardsAdded)
}

func TestClientUpdates_NamespaceRemoved(t *testing.T) {
	newStatus, shardsAdded, shardsToRemove := applyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: []model.ServerAddress{s1, s2, s3, s4},
	}, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s1, s2, s3},
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
						Ensemble: []model.ServerAddress{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1})

	assert.Equal(t, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s1, s2, s3},
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
						Status:   model.ShardStatusDeleting,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusDeleting,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.ServerAddress{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, newStatus)

	sort.Slice(shardsToRemove, func(i, j int) bool { return shardsToRemove[i] < shardsToRemove[j] })
	assert.Equal(t, []int64{1, 2}, shardsToRemove)
	assert.Equal(t, map[int64]string{}, shardsAdded)
}
