package controller

import (
	"context"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned/fake"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"oxia/pkg/apis/oxia/v1alpha1"
	oxia "oxia/pkg/generated/clientset/versioned/fake"
	"testing"
	"time"
)

func TestWatcher(t *testing.T) {
	namespace := "myns"
	name := "oxia"

	_oxia := oxia.NewSimpleClientset()
	_kubernetes := kubernetes.NewSimpleClientset()
	_monitoring := monitoring.NewSimpleClientset()

	watcher, err := newWatcher(_oxia, newReconciler(_kubernetes, _monitoring))
	assert.NoError(t, err)

	cluster := testOxiaCluster(name)

	_, err = _oxia.OxiaV1alpha1().OxiaClusters(namespace).
		Create(context.Background(), cluster, v1.CreateOptions{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		get, err := _kubernetes.AppsV1().StatefulSets(namespace).
			Get(context.Background(), name, v1.GetOptions{})
		assert.NoError(t, err)
		assert.Equal(t, int32(1), *get.Spec.Replicas)
		return true
	}, 10*time.Second, 100*time.Millisecond)

	err = _oxia.OxiaV1alpha1().OxiaClusters(namespace).
		Delete(context.Background(), name, v1.DeleteOptions{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		_, err := _kubernetes.AppsV1().StatefulSets(namespace).
			Get(context.Background(), name, v1.GetOptions{})
		assert.True(t, errors.IsNotFound(err))
		return true
	}, 10*time.Second, 100*time.Millisecond)

	err = watcher.Close()
	assert.NoError(t, err)
}

func testOxiaCluster(name string) *v1alpha1.OxiaCluster {
	return &v1alpha1.OxiaCluster{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
		},
		Spec: v1alpha1.OxiaClusterSpec{
			InitialShardCount: 1,
			ReplicationFactor: 1,
			ServerReplicas:    1,
			ServerResources: v1alpha1.Resources{
				Cpu:    "100m",
				Memory: "128Mi",
			},
			ServerVolume: "1Gi",
			CoordinatorResources: v1alpha1.Resources{
				Cpu:    "100m",
				Memory: "128Mi",
			},
			Image:             "oxia:latest",
			MonitoringEnabled: true,
		},
	}
}
