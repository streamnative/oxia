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
	ServerReplicas    *uint32 `json:"serverReplicas"`
	ShardCount        *uint32 `json:"shardCount"`
	ReplicationFactor *uint32 `json:"replicationFactor"`
}

// OxiaClusterStatus is the status for an OxiaCluster resource
type OxiaClusterStatus struct {
	Shards []*ShardMetadata `json:"shards"`
}

type ShardMetadata struct {
	Id        uint32          `json:"id"`
	Status    string          `json:"shardStatus"`
	Epoch     int64           `json:"epoch"`
	Leader    ServerAddress   `json:"leader"`
	Ensemble  []ServerAddress `json:"ensemble"`
	HashRange HashRange       `json:"hashRange"`
}

type ServerAddress struct {
	Public   string `json:"public"`
	Internal string `json:"internal"`
}

type HashRange struct {
	Min uint32 `json:"min"`
	Max uint32 `json:"max"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OxiaClusterList is a list of OxiaCluster resources
type OxiaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []OxiaCluster `json:"items"`
}
