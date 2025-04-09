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

import (
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/policies"
)

type ClusterConfig struct {
	Namespaces []NamespaceConfig `json:"namespaces" yaml:"namespaces"`
	Servers    []Server          `json:"servers" yaml:"servers"`
}

type NamespaceConfig struct {
	Name                 string                       `json:"name" yaml:"name"`
	InitialShardCount    uint32                       `json:"initialShardCount" yaml:"initialShardCount"`
	ReplicationFactor    uint32                       `json:"replicationFactor" yaml:"replicationFactor"`
	NotificationsEnabled common.OptBooleanDefaultTrue `json:"notificationsEnabled" yaml:"notificationsEnabled"`
	Policies             *policies.Policies           `json:"policies,omitempty" yaml:"policies,omitempty"`
}
