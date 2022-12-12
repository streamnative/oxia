package impl

type ClusterConfig struct {
	ReplicationFactor uint32          `json:"replicationFactor"`
	ShardCount        uint32          `json:"shardCount"`
	StorageServers    []ServerAddress `json:"storageServers"`
}
