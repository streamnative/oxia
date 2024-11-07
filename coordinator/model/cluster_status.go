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

type NodeId string

type ServerAddress struct {
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`

	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

func (s *ServerAddress) GetNodeId() NodeId {
	// use the internal address as the node id by default.
	// todo: introduce node id in the future
	return NodeId(s.Internal)
}

type Int32HashRange struct {
	// The minimum inclusive hash that the shard can contain
	Min uint32 `json:"min"`

	// The maximum inclusive hash that the shard can contain
	Max uint32 `json:"max"`
}

type ShardMetadata struct {
	Status         ShardStatus     `json:"status" yaml:"status"`
	Term           int64           `json:"term" yaml:"term"`
	Leader         *ServerAddress  `json:"leader" yaml:"leader"`
	Ensemble       []ServerAddress `json:"ensemble" yaml:"ensemble"`
	RemovedNodes   []ServerAddress `json:"removedNodes" yaml:"removedNodes"`
	Int32HashRange Int32HashRange  `json:"int32HashRange" yaml:"int32HashRange"`
}

type NamespaceStatus struct {
	ReplicationFactor uint32                  `json:"replicationFactor" yaml:"replicationFactor"`
	Shards            map[int64]ShardMetadata `json:"shards" yaml:"shards"`
}

type ClusterStatus struct {
	Namespaces       map[string]NamespaceStatus `json:"namespaces" yaml:"namespaces"`
	ShardIdGenerator int64                      `json:"shardIdGenerator" yaml:"shardIdGenerator"`
	ServerIdx        uint32                     `json:"serverIdx" yaml:"serverIdx"`
}

func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		Namespaces:       map[string]NamespaceStatus{},
		ShardIdGenerator: 0,
		ServerIdx:        0,
	}
}

func (sm Int32HashRange) Clone() Int32HashRange {
	return Int32HashRange{
		Min: sm.Min,
		Max: sm.Max,
	}
}

func (sm ShardMetadata) Clone() ShardMetadata {
	r := ShardMetadata{
		Status:         sm.Status,
		Term:           sm.Term,
		Leader:         sm.Leader,
		Ensemble:       make([]ServerAddress, len(sm.Ensemble)),
		RemovedNodes:   make([]ServerAddress, len(sm.RemovedNodes)),
		Int32HashRange: sm.Int32HashRange.Clone(),
	}

	copy(r.Ensemble, sm.Ensemble)
	copy(r.RemovedNodes, sm.RemovedNodes)

	return r
}

func (n NamespaceStatus) Clone() NamespaceStatus {
	r := NamespaceStatus{
		Shards:            make(map[int64]ShardMetadata),
		ReplicationFactor: n.ReplicationFactor,
	}

	for shard, sm := range n.Shards {
		r.Shards[shard] = sm.Clone()
	}

	return r
}

func (c ClusterStatus) Clone() *ClusterStatus {
	r := &ClusterStatus{
		Namespaces:       make(map[string]NamespaceStatus),
		ShardIdGenerator: c.ShardIdGenerator,
		ServerIdx:        c.ServerIdx,
	}

	for name, n := range c.Namespaces {
		r.Namespaces[name] = n.Clone()
	}

	return r
}
