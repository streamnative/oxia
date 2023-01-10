package model

type ClusterConfig struct {
	InitialShardCount uint32          `json:"initialShardCount" yaml:"initialShardCount"`
	ReplicationFactor uint32          `json:"replicationFactor" yaml:"replicationFactor"`
	Servers           []ServerAddress `json:"servers" yaml:"servers"`
}
