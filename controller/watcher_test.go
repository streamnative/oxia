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

package controller

import (
	"context"
	monitoring "github.com/prometheus-operator/prometheus-operator/pkg/client/versioned/fake"
	"github.com/stretchr/testify/assert"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
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
		assert.Equal(t, int32(3), *get.Spec.Replicas)
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
	pullAlways := coreV1.PullAlways
	return &v1alpha1.OxiaCluster{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
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
}
