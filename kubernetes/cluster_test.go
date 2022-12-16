package kubernetes

import (
	"context"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	fakeMonitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned/fake"
	"github.com/stretchr/testify/assert"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakeKubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/pointer"
	"oxia/pkg/apis/oxia/v1alpha1"
	"testing"
)

func TestCluster(t *testing.T) {
	_kubernetes := fakeKubernetes.NewSimpleClientset()
	_monitoring := fakeMonitoring.NewSimpleClientset()
	client := &clusterClientImpl{
		kubernetes: _kubernetes,
		monitoring: _monitoring,
	}

	cluster := v1alpha1.OxiaCluster{
		ObjectMeta: metaV1.ObjectMeta{
			Namespace: "nyns",
			Name:      "oxia",
		},
		Spec: v1alpha1.OxiaClusterSpec{
			ShardCount:        pointer.Uint32(1),
			ReplicationFactor: pointer.Uint32(2),
			ServerReplicas:    pointer.Uint32(3),
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
			ImagePullPolicy:   coreV1.PullAlways,
			MonitoringEnabled: true,
		},
	}

	err := client.Apply(cluster)
	assert.NoError(t, err)

	assertClusterResources(t, _kubernetes, _monitoring, cluster.Namespace, 1)

	err = client.Delete(cluster.Namespace, cluster.Name, true)
	assert.NoError(t, err)

	assertClusterResources(t, _kubernetes, _monitoring, cluster.Namespace, 0)
}

func assertClusterResources(t *testing.T, _kubernetes kubernetes.Interface, _monitoring monitoring.Interface, namespace string, length int) {
	serviceAccounts, err := _kubernetes.CoreV1().ServiceAccounts(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, serviceAccounts.Items, length*2)

	clusterRoles, err := _kubernetes.RbacV1().Roles(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, clusterRoles.Items, length)

	clusterRoleBindings, err := _kubernetes.RbacV1().RoleBindings(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, clusterRoleBindings.Items, length)

	deployments, err := _kubernetes.AppsV1().Deployments(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, deployments.Items, length)

	statefulsets, err := _kubernetes.AppsV1().StatefulSets(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, statefulsets.Items, length)

	services, err := _kubernetes.CoreV1().Services(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, services.Items, length*2)

	serviceMonitors, err := _monitoring.MonitoringV1().ServiceMonitors(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, serviceMonitors.Items, length*2)
}
