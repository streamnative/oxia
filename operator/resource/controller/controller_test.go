package controller

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

func TestController(t *testing.T) {
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
	err := client.Install(&out, config)
	assert.NoError(t, err)
	assert.Equal(t, `ServiceAccount install succeeded
ClusterRole install succeeded
ClusterRoleBinding install succeeded
Deployment install succeeded
Service install succeeded
ServiceMonitor install succeeded
`, out.String())
	out.Reset()

	assertControllerResources(t, _kubernetes, _monitoring, config.Namespace, 1)

	err = client.Uninstall(&out, config)
	assert.NoError(t, err)
	assert.Equal(t, `ServiceMonitor uninstall succeeded
Service uninstall succeeded
Deployment uninstall succeeded
ClusterRoleBinding uninstall succeeded
ClusterRole uninstall succeeded
ServiceAccount uninstall succeeded
`, out.String())

	assertControllerResources(t, _kubernetes, _monitoring, config.Namespace, 0)
}

func assertControllerResources(t *testing.T, _kubernetes kubernetes.Interface, _monitoring monitoring.Interface, namespace string, length int) {
	serviceAccounts, err := _kubernetes.CoreV1().ServiceAccounts(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, serviceAccounts.Items, length)

	clusterRoles, err := _kubernetes.RbacV1().ClusterRoles().
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, clusterRoles.Items, length)

	clusterRoleBindings, err := _kubernetes.RbacV1().ClusterRoleBindings().
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, clusterRoleBindings.Items, length)

	deployments, err := _kubernetes.AppsV1().Deployments(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, deployments.Items, length)

	services, err := _kubernetes.CoreV1().Services(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, services.Items, length)

	serviceMonitors, err := _monitoring.MonitoringV1().ServiceMonitors(namespace).
		List(context.Background(), v1.ListOptions{})
	assert.NoError(t, err)
	assert.Len(t, serviceMonitors.Items, length)
}
