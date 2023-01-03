package kubernetes

import (
	"github.com/stretchr/testify/assert"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
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
			ReplicationFactor: pointer.Uint32(2),
			ShardCount:        pointer.Uint32(1),
			ServerReplicas:    pointer.Uint32(3),
		},
	}
	configMap := configMap(cluster)

	assert.Equal(t, `replicationFactor: 1
shardCount: 2
servers:
- public: oxia-0.oxia.nyns.svc.cluster.local:6648
  internal: oxia-0.oxia.nyns.svc.cluster.local:6649
- public: oxia-1.oxia.nyns.svc.cluster.local:6648
  internal: oxia-1.oxia.nyns.svc.cluster.local:6649
- public: oxia-2.oxia.nyns.svc.cluster.local:6648
  internal: oxia-2.oxia.nyns.svc.cluster.local:6649
`, configMap.Data["config.yaml"])
}
