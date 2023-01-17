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

package model

type ServerAddress struct {
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`

	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

type Int32HashRange struct {
	// The minimum inclusive hash that the shard can contain
	Min uint32 `json:"min"`

	// The maximum inclusive hash that the shard can contain
	Max uint32 `json:"max"`
}

type ShardMetadata struct {
	Status         ShardStatus     `json:"status" yaml:"status"`
	Epoch          int64           `json:"epoch" yaml:"epoch"`
	Leader         *ServerAddress  `json:"leader" yaml:"leader"`
	Ensemble       []ServerAddress `json:"ensemble" yaml:"ensemble"`
	Int32HashRange Int32HashRange  `json:"int32HashRange" yaml:"int32HashRange"`
}

type ClusterStatus struct {
	ReplicationFactor uint32                   `json:"replicationFactor" yaml:"replicationFactor"`
	Shards            map[uint32]ShardMetadata `json:"shards" yaml:"shards"`
}

////////////////////////////////////////////////////////////////////////////////////////////////

func (sm Int32HashRange) Clone() Int32HashRange {
	return Int32HashRange{
		Min: sm.Min,
		Max: sm.Max,
	}
}

func (sm ShardMetadata) Clone() ShardMetadata {
	r := ShardMetadata{
		Status:         sm.Status,
		Epoch:          sm.Epoch,
		Leader:         sm.Leader,
		Ensemble:       make([]ServerAddress, len(sm.Ensemble)),
		Int32HashRange: sm.Int32HashRange.Clone(),
	}

	copy(r.Ensemble, sm.Ensemble)

	return r
}

func (c ClusterStatus) Clone() *ClusterStatus {
	r := &ClusterStatus{
		ReplicationFactor: c.ReplicationFactor,
		Shards:            make(map[uint32]ShardMetadata),
	}

	for shard, sm := range c.Shards {
		r.Shards[shard] = sm.Clone()
	}

	return r
}
