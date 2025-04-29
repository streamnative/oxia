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

	"github.com/emirpasic/gods/sets/linkedhashset"

	"github.com/streamnative/oxia/coordinator/model"
)

func TestLoadBalancerActions(t *testing.T) {
	status := &model.ClusterStatus{}
	serverIDs := linkedhashset.New()
	balancer := NewLoadBalancer(Options{
		Context: context.Background(),
		MetadataSupplier: func() map[string]model.ServerMetadata {
			return make(map[string]model.ServerMetadata)
		},
		ClusterServerIDsSupplier: func() *linkedhashset.Set {
			return serverIDs
		},
		ClusterStatusSupplier: func() *model.ClusterStatus { return status },
		NamespaceConfigSupplier: func(namespace string) *model.NamespaceConfig {
			return nil
		},
	})

	<-balancer.Action()
}
