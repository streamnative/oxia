package cluster

import (
	"bytes"
	"context"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	fakeMonitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned/fake"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakeKubernetes "k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestCluster(t *testing.T) {
	_kubernetes := fakeKubernetes.NewSimpleClientset()
	_monitoring := fakeMonitoring.NewSimpleClientset()
	client := &clientImpl{
		kubernetes: _kubernetes,
		monitoring: _monitoring,
	}

	config := NewConfig()
	config.Namespace = "myns"
	config.MonitoringEnabled = true

	var out bytes.Buffer
	err := client.Apply(&out, config)
	assert.NoError(t, err)
	assert.Equal(t, `coordinator ServiceAccount apply succeeded
coordinator Role apply succeeded
coordinator RoleBinding apply succeeded
coordinator Deployment apply succeeded
coordinator Service apply succeeded
coordinator ServiceMonitor apply succeeded
server ServiceAccount apply succeeded
server StatefulSet apply succeeded
server Service apply succeeded
server ServiceMonitor apply succeeded
`, out.String())
	out.Reset()

	assertClusterResources(t, _kubernetes, _monitoring, config.Namespace, 1)

	err = client.Delete(&out, config)
	assert.NoError(t, err)
	assert.Equal(t, `server ServiceMonitor delete succeeded
server Service delete succeeded
server StatefulSet delete succeeded
server ServiceAccount delete succeeded
coordinator ServiceMonitor delete succeeded
coordinator Service delete succeeded
coordinator Deployment delete succeeded
coordinator RoleBinding delete succeeded
coordinator Role delete succeeded
coordinator ServiceAccount delete succeeded
`, out.String())

	assertClusterResources(t, _kubernetes, _monitoring, config.Namespace, 0)
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
