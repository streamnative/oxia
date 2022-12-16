package impl

type ClusterConfig struct {
	ReplicationFactor uint32          `json:"replicationFactor" yaml:"replicationFactor"`
	ShardCount        uint32          `json:"shardCount" yaml:"shardCount"`
	Servers           []ServerAddress `json:"servers" yaml:"servers"`
}
