// Copyright 2023-2026 The Oxia Authors
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

package metadata

import (
	"io"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Metadata interface {
	io.Closer

	WaitToBecomeLeader() (lost <-chan struct{}, err error)
	GetSelf() (*commonproto.Coordinator, error)
	GetLeader() (*commonproto.Coordinator, error)
	GetInstanceID() (string, error)
	AllocateShardIDs(count uint32) (int64, error)

	CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) error
	ListNamespaceStatus() (map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], error)
	GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool, error)
	PatchNamespaceStatus(namespace string, modify func(status *commonproto.NamespaceStatus) bool) error
	DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus]

	GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool, error)
	UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error
	DeleteShardStatus(namespace string, shard int64) error

	GetConfig() (commonobject.Borrowed[*commonproto.ClusterConfiguration], error)
	SubscribeConfig() *cache.Subscription[provider.Versioned[*commonproto.ClusterConfiguration]]
	GetLoadBalancerConfig() (commonobject.Borrowed[*commonproto.LoadBalancer], error)

	CreateNamespace(namespace *commonproto.Namespace) error
	PatchNamespace(namespace *commonproto.Namespace) (*commonproto.Namespace, error)
	DeleteNamespace(name string) (*commonproto.Namespace, error)
	ListNamespace() (map[string]commonobject.Borrowed[*commonproto.Namespace], error)
	GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool, error)
	GetNamespaceTermOptions(namespace string) (*commonproto.NewTermOptions, error)

	CreateDataServer(dataServer *commonproto.DataServer) error
	PatchDataServer(dataServer *commonproto.DataServer) (*commonproto.DataServer, error)
	DeleteDataServer(name string) (*commonproto.DataServer, error)
	ListDataServer() (map[string]commonobject.Borrowed[*commonproto.DataServer], error)
	GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool, error)
}
