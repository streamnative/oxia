package coordinator

type ServerAddress struct {
	// Public is the endpoint that is advertised to clients
	Public string `json:"public"`

	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal"`
}

type Int32HashRange struct {
	// The minimum inclusive hash that the shard can contain
	Min uint32 `json:"min"`

	// The maximum inclusive hash that the shard can contain
	Max uint32 `json:"max"`
}

type ShardMetadata struct {
	Status         ShardStatus     `json:"shardStatus"`
	Epoch          int64           `json:"epoch"`
	Leader         *ServerAddress  `json:"leader"`
	Ensemble       []ServerAddress `json:"ensemble"`
	Int32HashRange Int32HashRange  `json:"int32HashRange"`
}

type ClusterStatus struct {
	ReplicationFactor uint32 `json:"replicationFactor"`

	Shards map[uint32]*ShardMetadata
}

////////////////////////////////////////////////////////////////////////////////////////////////

func (sm Int32HashRange) Clone() Int32HashRange {
	return Int32HashRange{
		Min: sm.Min,
		Max: sm.Max,
	}
}

func (sm *ShardMetadata) Clone() *ShardMetadata {
	r := &ShardMetadata{
		Status:         sm.Status,
		Epoch:          sm.Epoch,
		Leader:         sm.Leader,
		Ensemble:       make([]ServerAddress, len(sm.Ensemble)),
		Int32HashRange: sm.Int32HashRange.Clone(),
	}

	copy(r.Ensemble, sm.Ensemble)

	return r
}

func (c *ClusterStatus) Clone() *ClusterStatus {
	r := &ClusterStatus{
		ReplicationFactor: c.ReplicationFactor,
		Shards:            make(map[uint32]*ShardMetadata),
	}

	for shard, sm := range c.Shards {
		r.Shards[shard] = sm.Clone()
	}

	return r
}
