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

package impl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/common/rpc"
	"github.com/streamnative/oxia/coordinator/model"
)

func TestCoordinatorInitiateLeaderElection(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinator, err := NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	metadata := model.ShardMetadata{
		Status:         model.ShardStatusSteadyState,
		Term:           999,
		Leader:         nil,
		Ensemble:       []model.Server{},
		RemovedNodes:   []model.Server{},
		Int32HashRange: model.Int32HashRange{Min: 2000, Max: 100000},
	}
	err = coordinator.InitiateLeaderElection("default", 1, metadata)
	assert.NoError(t, err)

	status := coordinator.ClusterStatus()
	assert.EqualValues(t, status.Namespaces["default"].Shards[1], metadata)
}
