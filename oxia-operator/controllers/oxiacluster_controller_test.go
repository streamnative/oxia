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

package controllers

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	oxiav1alpha1 "github.com/streamnative/oxia/api/v1alpha1"
	. "github.com/streamnative/oxia/controllers/common"
	appsV1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	rbacV1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"time"
)

var _ = Describe("Oxiacluster controller", func() {
	const (
		OxiaClusterKind       = "OxiaCluster"
		OxiaClusterApiVersion = "oxia.io.streamnative/v1alpha1"
		OxiaClusterName       = "oxia-test"
		OxiaImage             = "sn/oxia:v1.0"
		Timeout               = time.Second * 10
		Interval              = time.Millisecond * 250
	)
	Context("When create Oxia cluster", func() {
		oxiaNamespace := "oxia-cluster-test-1"
		imagePullPolicy := v1.PullAlways
		cluster := oxiav1alpha1.OxiaCluster{
			TypeMeta:   metav1.TypeMeta{Kind: OxiaClusterKind, APIVersion: OxiaClusterApiVersion},
			ObjectMeta: metav1.ObjectMeta{Namespace: oxiaNamespace, Name: OxiaClusterName},
			Spec: oxiav1alpha1.OxiaClusterSpec{
				Namespaces: []oxiav1alpha1.NamespaceConfig{
					{
						Name:              "default",
						InitialShardCount: 3,
						ReplicationFactor: 3,
					},
				},
				Coordinator:       oxiav1alpha1.Coordinator{Cpu: resource.MustParse("100m"), Memory: resource.MustParse("128Mi")},
				Server:            oxiav1alpha1.Server{Replicas: 3, Cpu: resource.MustParse("1"), Memory: resource.MustParse("1Gi"), Storage: resource.MustParse("8Gi")},
				Image:             oxiav1alpha1.Image{PullPolicy: &imagePullPolicy, Tag: "main", Repository: OxiaImage},
				PprofEnabled:      false,
				MonitoringEnabled: true,
			},
		}
		namespace := v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: oxiaNamespace,
			},
		}
		It("Should created all of sub-components.", func() {
			Expect(k8sClient.Create(context.Background(), &namespace)).Should(Succeed())
			Expect(k8sClient.Create(context.Background(), &cluster)).Should(Succeed())
			// Test cluster
			_cluster := &oxiav1alpha1.OxiaCluster{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
				return k8sClient.Get(context.Background(), _namespaceName, _cluster)
			}, Timeout, Interval).Should(Succeed())
			Expect(cluster).ShouldNot(BeNil())
			Expect(cluster.Spec).Should(Equal(_cluster.Spec))

			// Test Service Account
			_serviceAccount := &v1.ServiceAccount{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Coordinator, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _serviceAccount)
			}, Timeout, Interval).Should(Succeed())
			Expect(_serviceAccount).ShouldNot(BeNil())
			// Test MakeRole
			_role := &rbacV1.Role{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Coordinator, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _role)
			}, Timeout, Interval).Should(Succeed())
			Expect(_role).ShouldNot(BeNil())
			// Test MakeRole Binding
			_roleBinding := &rbacV1.RoleBinding{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Coordinator, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _roleBinding)
			}, Timeout, Interval).Should(Succeed())
			Expect(_roleBinding).ShouldNot(BeNil())
			Expect(len(_roleBinding.Subjects)).Should(Equal(1))
			Expect(_roleBinding.Subjects[0].Name).Should(Equal(MakeResourceName(Coordinator, cluster.Name)))
			Expect(_roleBinding.RoleRef.Name).Should(Equal(MakeResourceName(Coordinator, cluster.Name)))
			// Test configmap
			_configMap := &coreV1.ConfigMap{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Coordinator, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _configMap)
			}, Timeout, Interval).Should(Succeed())
			Expect(_configMap).ShouldNot(BeNil())
			Expect(len(_configMap.Data)).Should(Equal(1))
			Expect(_configMap.Data["config.yaml"]).Should(Equal("namespaces:\n- name: default\n  initialShardCount: 3\n  replicationFactor: 3\nservers:\n- public: oxia-test-0.oxia-test.oxia-cluster-test-1.svc.cluster.local:6648\n  internal: oxia-test-0.oxia-test:6649\n- public: oxia-test-1.oxia-test.oxia-cluster-test-1.svc.cluster.local:6648\n  internal: oxia-test-1.oxia-test:6649\n- public: oxia-test-2.oxia-test.oxia-cluster-test-1.svc.cluster.local:6648\n  internal: oxia-test-2.oxia-test:6649\n"))
			// Test deployment
			_deployment := &appsV1.Deployment{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Coordinator, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _deployment)
			}, Timeout, Interval).Should(Succeed())
			Expect(_deployment).ShouldNot(BeNil())
			Expect(*_deployment.Spec.Replicas).Should(Equal(int32(1)))
			// Test coordinator service
			_service := &coreV1.Service{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Coordinator, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _service)
			}, Timeout, Interval).Should(Succeed())
			Expect(_service).ShouldNot(BeNil())
			// Test server service account
			_serverServiceAccount := &v1.ServiceAccount{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Server, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _serverServiceAccount)
			}, Timeout, Interval).Should(Succeed())
			Expect(_serverServiceAccount).ShouldNot(BeNil())
			// Test server stateful set
			_statefulSet := &appsV1.StatefulSet{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Server, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _statefulSet)
			}, Timeout, Interval).Should(Succeed())
			Expect(_statefulSet).ShouldNot(BeNil())
			// Test server service
			_serverService := &coreV1.Service{}
			Eventually(func() error {
				_namespaceName := types.NamespacedName{Namespace: cluster.Namespace, Name: MakeResourceName(Server, cluster.Name)}
				return k8sClient.Get(context.Background(), _namespaceName, _serverService)
			}, Timeout, Interval).Should(Succeed())
			Expect(_serverService).ShouldNot(BeNil())
		})
	})

})
