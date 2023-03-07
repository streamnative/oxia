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
	"context"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned"
	fakeMonitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned/fake"
	"github.com/stretchr/testify/assert"
	coreV1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	fakeKubernetes "k8s.io/client-go/kubernetes/fake"
	"oxia/pkg/apis/oxia/v1alpha1"
	"testing"
)

func TestCluster(t *testing.T) {
	_kubernetes := fakeKubernetes.NewSimpleClientset()
	_monitoring := fakeMonitoring.NewSimpleClientset()
	client := NewClusterClient(_kubernetes, _monitoring)
	pullAlways := coreV1.PullAlways
	cluster := v1alpha1.OxiaCluster{
		ObjectMeta: metaV1.ObjectMeta{
			Namespace: "nyns",
			Name:      "oxia",
		},
		Spec: v1alpha1.OxiaClusterSpec{
			InitialShardCount: 1,
			ReplicationFactor: 2,
			Coordinator: v1alpha1.Coordinator{
				Cpu:    k8sResource.MustParse("100m"),
				Memory: k8sResource.MustParse("128Mi"),
			},
			Server: v1alpha1.Server{
				Replicas: 3,
				Cpu:      k8sResource.MustParse("100m"),
				Memory:   k8sResource.MustParse("128Mi"),
				Storage:  k8sResource.MustParse("1Gi"),
			},
			Image: v1alpha1.Image{
				Repository: "streamnative/oxia",
				Tag:        "latest",
				PullPolicy: &pullAlways,
			},
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
