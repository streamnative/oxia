package impl

type ClusterConfig struct {
	ReplicationFactor uint32          `json:"replicationFactor"`
	ShardsCount       uint32          `json:"shardsCount"`
	StorageServers    []ServerAddress `json:"storageServers"`
}
