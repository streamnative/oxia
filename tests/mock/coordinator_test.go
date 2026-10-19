// Copyright 2023-2026 The Oxia Authors
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

package mock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNewCoordinatorElectsShardLeaders(t *testing.T) {
	server, identity := NewServer(t, "s1")
	defer server.Close()

	coordinator := NewCoordinator(t, NewConfigProvider(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns", ReplicationFactor: 1, InitialShardCount: 1}},
		Servers:    []*proto.DataServerIdentity{identity},
	}))
	defer coordinator.Close()

	assert.Eventually(t, func() bool {
		namespace, ok := StatusSnapshot(t, coordinator.Metadata()).Namespaces["ns"]
		if !ok || len(namespace.Shards) != 1 {
			return false
		}
		shard := namespace.Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState &&
			shard.GetLeader().GetNameOrDefault() == "s1"
	}, 20*time.Second, 50*time.Millisecond)
}
