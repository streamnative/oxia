package cluster

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"oxia/pkg/apis/oxia/v1alpha1"
	"oxia/pkg/generated/clientset/versioned/fake"
	"testing"
)

func TestManagedClient(t *testing.T) {
	_fake := fake.NewSimpleClientset()
	client := managedClientImpl{
		oxia: _fake,
	}

	config := NewConfig()
	config.Name = "oxia"
	config.Namespace = "oxia"

	var out bytes.Buffer
	err := client.Apply(&out, config)
	assert.NoError(t, err)
	assert.Equal(t, "OxiaCluster apply succeeded\n", out.String())
	out.Reset()

	var cluster *v1alpha1.OxiaCluster
	cluster, err = _fake.OxiaV1alpha1().OxiaClusters("oxia").Get(context.Background(), "oxia", metaV1.GetOptions{})
	assert.NoError(t, err)

	spec := cluster.Spec
	assert.Equal(t, pointer.Uint32(3), spec.ShardCount)
	assert.Equal(t, pointer.Uint32(3), spec.ReplicationFactor)
	assert.Equal(t, pointer.Uint32(3), spec.ServerReplicas)
	assert.Equal(t, "100m", spec.ServerResources.Cpu)
	assert.Equal(t, "128Mi", spec.ServerResources.Memory)
	assert.Equal(t, "1Gi", spec.ServerVolume)
	assert.Equal(t, "100m", spec.CoordinatorResources.Cpu)
	assert.Equal(t, "128Mi", spec.CoordinatorResources.Memory)
	assert.Equal(t, "oxia:latest", spec.Image)
	assert.Equal(t, true, spec.MonitoringEnabled)

	err = client.Delete(&out, config)
	assert.NoError(t, err)

	var list *v1alpha1.OxiaClusterList
	list, err = _fake.OxiaV1alpha1().OxiaClusters("oxia").List(context.Background(), metaV1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, list.Items, 0)

}
