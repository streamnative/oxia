package v1alpha1

import (
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
	ServerReplicas    *int32 `json:"serverReplicas"`
	ShardCount        *int32 `json:"shardCount"`
	ReplicationFactor *int32 `json:"replicationFactor"`
}

// OxiaClusterStatus is the status for an OxiaCluster resource
type OxiaClusterStatus struct {
	//TODO CRD status fields
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OxiaClusterList is a list of OxiaCluster resources
type OxiaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []OxiaCluster `json:"items"`
}
