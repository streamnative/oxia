package v1alpha1

import (
	coreV1 "k8s.io/api/core/v1"
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
	InitialShardCount    uint32             `json:"initialShardCount"`
	ReplicationFactor    uint32             `json:"replicationFactor"`
	ServerReplicas       uint32             `json:"serverReplicas"`
	ServerResources      Resources          `json:"serverResources"`
	ServerVolume         string             `json:"serverVolume"`
	StorageClassName     *string            `json:"storageClassName,omitempty"`
	CoordinatorResources Resources          `json:"coordinatorResources"`
	Image                string             `json:"image"`
	ImagePullSecrets     *string            `json:"imagePullSecrets,omitempty"`
	ImagePullPolicy      *coreV1.PullPolicy `json:"imagePullPolicy,omitempty"`
	MonitoringEnabled    bool               `json:"monitoringEnabled"`
}

// OxiaClusterStatus is the status for an OxiaCluster resource
type OxiaClusterStatus struct{}

type Resources struct {
	Cpu    string `json:"cpu"`
	Memory string `json:"memory"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OxiaClusterList is a list of OxiaCluster resources
type OxiaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []OxiaCluster `json:"items"`
}
