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

package kubernetes

import (
	"github.com/stretchr/testify/assert"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oxia/pkg/apis/oxia/v1alpha1"
	"testing"
)

func TestConfigMap(t *testing.T) {
	cluster := v1alpha1.OxiaCluster{
		ObjectMeta: metaV1.ObjectMeta{
			Namespace: "nyns",
			Name:      "oxia",
		},
		Spec: v1alpha1.OxiaClusterSpec{
			InitialShardCount: 1,
			ReplicationFactor: 2,
			Server: v1alpha1.Server{
				Replicas: 3,
			},
		},
	}
	configMap := configMap(cluster)

	assert.Equal(t, `initialShardCount: 1
replicationFactor: 2
servers:
- public: oxia-0.oxia.nyns.svc.cluster.local:6648
  internal: oxia-0.oxia.nyns.svc.cluster.local:6649
- public: oxia-1.oxia.nyns.svc.cluster.local:6648
  internal: oxia-1.oxia.nyns.svc.cluster.local:6649
- public: oxia-2.oxia.nyns.svc.cluster.local:6648
  internal: oxia-2.oxia.nyns.svc.cluster.local:6649
`, configMap.Data["config.yaml"])
}
