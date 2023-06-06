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

package coordinator

import (
	"github.com/streamnative/oxia/controllers/common"
)

type ServerAddress struct {
	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`
	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

type ClusterConfig struct {
	Namespaces []NamespaceConfig `json:"namespaces" yaml:"namespaces"`
	Servers    []ServerAddress   `json:"servers" yaml:"servers"`
}

type NamespaceConfig struct {
	Name              string `json:"name" yaml:"name"`
	InitialShardCount uint32 `json:"initialShardCount" yaml:"initialShardCount"`
	ReplicationFactor uint32 `json:"replicationFactor" yaml:"replicationFactor"`
}

var Ports = []common.NamedPort{common.InternalPort, common.MetricsPort}
