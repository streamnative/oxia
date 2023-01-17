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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OxiaCluster is a specification for an OxiaCluster resource
type OxiaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OxiaClusterSpec   `json:"spec"`
	Status OxiaClusterStatus `json:"status"`
}

// OxiaClusterSpec is the spec for an OxiaCluster resource
type OxiaClusterSpec struct {
	// InitialShardCount is the initial number of shard to bootstrap a new cluster with
	InitialShardCount uint32 `json:"initialShardCount"`

	// ReplicationFactor is the number of copies the cluster will maintain for each shard. leader + followers
	ReplicationFactor uint32 `json:"replicationFactor"`

	// Coordinator contains configuration specific to the coordinator component
	Coordinator Coordinator `json:"coordinator"`

	// Server contains configuration specific to the server component
	Server Server `json:"server"`

	// Image contains configuration specific to the image being used
	Image Image `json:"image"`

	// MonitoringEnabled determines whether a Prometheus ServiceMonitor should be created
	MonitoringEnabled bool `json:"monitoringEnabled"`
}

type Coordinator struct {
	// Cpu describes the requests and limits of CPU cores allocated to the pod
	Cpu resource.Quantity `json:"cpu"`

	// Memory describes the requests and limits of Memory allocated to the pod
	Memory resource.Quantity `json:"memory"`
}

type Server struct {
	// Replicas is the number of server pods that should be running
	Replicas uint32 `json:"replicas"`

	// Cpu describes the requests and limits of CPU cores allocated to each pod
	Cpu resource.Quantity `json:"cpu"`

	// Memory describes the requests and limits of memory allocated to each pod
	Memory resource.Quantity `json:"memory"`

	// Storage describes the size of the persistent volume allocated to each pod
	Storage resource.Quantity `json:"storage"`

	// StorageClassName is the name of StorageClass to which the persistent volume belongs
	StorageClassName *string `json:"storageClassName,omitempty"`
}

type Image struct {
	// Image is the container image name
	Name string `json:"name"`

	// PullPolicy is one of Always, Never, IfNotPresent
	PullPolicy *corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// PullSecrets is the optional name of a secret in the same namespace to use for pulling the image
	PullSecrets *string `json:"pullSecrets,omitempty"`
}

// OxiaClusterStatus is the status for an OxiaCluster resource
type OxiaClusterStatus struct{}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OxiaClusterList is a list of OxiaCluster resources
type OxiaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []OxiaCluster `json:"items"`
}
