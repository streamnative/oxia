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

package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
)

func TestLoadBalance(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{
		"sv-1": {},
		"sv-2": {},
		"sv-3": {},
		"sv-4": {},
		"sv-5": {},
	}
	candidates := linkedhashset.New("sv-1", "sv-2", "sv-3", "sv-4", "sv-5")
	nc := &model.NamespaceConfig{
		Name:              "default",
		InitialShardCount: 1,
		ReplicationFactor: 3,
	}
	shardsMetadata := map[int64]model.ShardMetadata{
		0: {
			Ensemble: []model.Server{
				{Internal: "sv-1", Public: "sv-1"},
				{Internal: "sv-2", Public: "sv-2"},
				{Internal: "sv-3", Public: "sv-3"},
			},
		},
		1: {
			Ensemble: []model.Server{
				{Internal: "sv-2", Public: "sv-2"},
				{Internal: "sv-3", Public: "sv-3"},
				{Internal: "sv-4", Public: "sv-4"},
			},
		},
		2: {
			Ensemble: []model.Server{
				{Internal: "sv-1", Public: "sv-1"},
				{Internal: "sv-2", Public: "sv-2"},
				{Internal: "sv-3", Public: "sv-3"},
			},
		},
	}
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"default": {ReplicationFactor: 3, Shards: shardsMetadata},
		},
		ShardIdGenerator: 0,
		ServerIdx:        0,
	}

	balancer := NewLoadBalancer(Options{
		Context:                   t.Context(),
		CandidateMetadataSupplier: func() map[string]model.ServerMetadata { return candidatesMetadata },
		CandidatesSupplier:        func() *linkedhashset.Set { return candidates },
		NamespaceConfigSupplier:   func(namespace string) *model.NamespaceConfig { return nc },
		StatusSupplier:            func() *model.ClusterStatus { return cs },
	})

	go func() {
		ApplyActions(t.Context(), shardsMetadata, balancer.Action())
	}()

	assert.Eventually(t, func() bool {
		balancer.Trigger()
		return balancer.IsBalanced()
	}, 15*time.Second, 100*time.Millisecond)
	assert.NoError(t, balancer.Close())
}

func ApplyActions(ctx context.Context, shardsMetadata map[int64]model.ShardMetadata, actionCh <-chan Action) {
	for {
		select {
		case action, more := <-actionCh:
			if !more {
				return
			}
			if action.Type() != SwapNode {
				action.Done()
				continue
			}
			swapAction := action.(*SwapNodeAction)
			metadata := shardsMetadata[swapAction.Shard]
			if metadata.Ensemble == nil {
				action.Done()
				continue
			}
			fromNodeID := swapAction.From
			toNodeID := swapAction.To
			var servers []model.Server
			for _, node := range metadata.Ensemble {
				if node.GetIdentifier() != fromNodeID {
					servers = append(servers, node)
				}
			}
			servers = append(servers, model.Server{Internal: toNodeID, Public: toNodeID}) // simplify
			metadata.Ensemble = servers
			shardsMetadata[swapAction.Shard] = metadata
			action.Done()
		case <-ctx.Done():
			return
		}
	}
}

func TestLoadBalanceQuarantined(t *testing.T) {

}
