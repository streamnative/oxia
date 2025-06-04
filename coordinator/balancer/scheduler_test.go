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
	"testing"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/streamnative/oxia/coordinator/model"
)

func TestLoadBalanceDeleteNode(t *testing.T) {
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
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"default": {ReplicationFactor: 3, Shards: map[int64]model.ShardMetadata{
				0: {
					Ensemble: []model.Server{
						{Internal: "sv-1", Public: "sv1"},
						{Internal: "sv-2", Public: "sv2"},
						{Internal: "sv-3", Public: "sv3"},
					},
				},
				1: {
					Ensemble: []model.Server{
						{Internal: "sv-2", Public: "sv2"},
						{Internal: "sv-3", Public: "sv3"},
						{Internal: "sv-4", Public: "sv4"},
					},
				},
				2: {
					Ensemble: []model.Server{
						{Internal: "sv-1", Public: "sv1"},
						{Internal: "sv-2", Public: "sv2"},
						{Internal: "sv-3", Public: "sv3"},
					},
				},
			}},
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

	balancer.Trigger()

	ch := make(chan string, 1)
	<-ch
}

func TestLoadBalanceLoadRatio(t *testing.T) {

}

func TestLoadBalanceQuarantined(t *testing.T) {

}

func TestLoadBalanceWithAntiAffinity(t *testing.T) {

}
