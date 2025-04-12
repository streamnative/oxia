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
	"fmt"
	"testing"
	"time"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
)

type TestClusterOptions struct {
	ServerNum     int
	ClusterConfig model.ClusterConfig
}

func CreateCluster(t *testing.T, options TestClusterOptions) (impl.Coordinator, []model.Server, map[string]*server.Server, func()) {
	t.Helper()
	servers := map[string]*server.Server{}
	var serverInfos []model.Server
	for range options.ServerNum {
		s, err := server.New(server.Config{
			PublicServiceAddr:          "localhost:0",
			InternalServiceAddr:        "localhost:0",
			MetricsServiceAddr:         "", // Disable metrics to avoid conflict
			DataDir:                    t.TempDir(),
			WalDir:                     t.TempDir(),
			NotificationsRetentionTime: 1 * time.Minute,
		})
		assert.NoError(t, err)
		addr := model.Server{
			Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
			Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
		}
		serverInfos = append(serverInfos, addr)
		servers[addr.GetIdentifier()] = s
	}

	metadataProvider := impl.NewMetadataProviderMemory()
	clientPool := common.NewClientPool(nil, nil)
	options.ClusterConfig.Servers = serverInfos
	coordinator, err := impl.NewCoordinator(metadataProvider,
		func() (model.ClusterConfig, error) { return options.ClusterConfig, nil },
		nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	return coordinator, serverInfos, servers, func() {
		assert.NoError(t, coordinator.Close())
		for _, sv := range servers {
			assert.NoError(t, sv.Close())
		}
	}
}

func GetClusterLeader(t *testing.T, coordinator impl.Coordinator, servers map[string]*server.Server, namespace string,
	shard int64) (server.LeaderController, *model.Server) {
	t.Helper()

	status := coordinator.ClusterStatus()
	namespaceStatus := status.Namespaces[namespace]
	firstShare := namespaceStatus.Shards[shard]
	leaderServer := firstShare.Leader
	leader := servers[leaderServer.GetIdentifier()]
	lc, err := leader.GetShardsDirector().GetLeader(0)
	assert.NoError(t, err)
	return lc, leaderServer
}
