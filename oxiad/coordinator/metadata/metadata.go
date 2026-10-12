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

// Metadata is the coordinator's view of the cluster metadata: the status,
// which the coordinator maintains (the namespaces and their shards), and the
// configuration (the namespaces and the data servers to run them on).
//
// The reads return the error of the load, and the writes the error of the
// write, without retrying: the caller decides how to retry. The returned
// borrowed values must not be modified.
type Metadata interface {
	io.Closer

	// WaitToBecomeLeader blocks until this coordinator holds the leadership.
	// lost is closed when the leadership is lost, or nil if it can't be.
	WaitToBecomeLeader() (lost <-chan struct{}, err error)
	// GetSelf returns this coordinator, from the configuration.
	GetSelf() (*commonproto.Coordinator, error)
	// GetLeader returns the coordinator that holds the leadership.
	GetLeader() (*commonproto.Coordinator, error)
	// GetInstanceID returns the ID of the cluster.
	GetInstanceID() (string, error)
	// AllocateShardIDs reserves count shard IDs, and returns the first one.
	AllocateShardIDs(count uint32) (int64, error)

	// CreateNamespaceStatus creates the status of a namespace, or returns
	// ErrAlreadyExists.
	CreateNamespaceStatus(namespace string, status *commonproto.NamespaceStatus) error
	ListNamespaceStatus() (map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], error)
	GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool, error)
	// PatchNamespaceStatus applies modify to a copy of the current status of a
	// namespace, and stores it in a single write, unless modify returns false.
	// A namespace left without shards is deleted. It returns ErrNotFound if
	// the namespace doesn't exist.
	PatchNamespaceStatus(namespace string, modify func(status *commonproto.NamespaceStatus) bool) error
	// DeleteNamespaceStatus marks the shards of a namespace as deleting, and
	// returns its status. Unlike the other writes, it retries until the write
	// succeeds or the metadata closes, and returns an empty value then.
	DeleteNamespaceStatus(namespace string) commonobject.Borrowed[*commonproto.NamespaceStatus]

	GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool, error)
	// UpdateShardStatus replaces the metadata of an existing shard, or returns
	// ErrNotFound.
	UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error
	// DeleteShardStatus deletes a shard, and its namespace if it was the last
	// shard.
	DeleteShardStatus(namespace string, shard int64) error

	GetConfig() (commonobject.Borrowed[*commonproto.ClusterConfiguration], error)
	// SubscribeConfig returns a subscription notified on each change of the
	// configuration.
	SubscribeConfig() *cache.Subscription[provider.Versioned[*commonproto.ClusterConfiguration]]
	// GetLoadBalancerConfig returns the load balancer configuration, with its
	// defaults.
	GetLoadBalancerConfig() (commonobject.Borrowed[*commonproto.LoadBalancer], error)

	// CreateNamespace adds a namespace to the configuration, or returns
	// ErrAlreadyExists, or ErrFailedPrecondition if there are fewer data
	// servers than its replication factor.
	CreateNamespace(namespace *commonproto.Namespace) error
	// PatchNamespace updates the replication factor and the notifications of
	// a namespace, and returns it, or ErrNotFound.
	PatchNamespace(namespace *commonproto.Namespace) (*commonproto.Namespace, error)
	// DeleteNamespace removes a namespace from the configuration, and returns
	// it, or ErrNotFound.
	DeleteNamespace(name string) (*commonproto.Namespace, error)
	ListNamespace() (map[string]commonobject.Borrowed[*commonproto.Namespace], error)
	GetNamespace(name string) (commonobject.Borrowed[*commonproto.Namespace], bool, error)
	// GetNamespaceTermOptions returns the NewTerm options of the shards of a
	// namespace, or the defaults if it doesn't exist.
	GetNamespaceTermOptions(name string) (*commonproto.NewTermOptions, error)

	// CreateDataServer adds a data server to the configuration, or returns
	// ErrAlreadyExists.
	CreateDataServer(dataServer *commonproto.DataServer) error
	// PatchDataServer updates the addresses and the labels of a data server,
	// and returns it, or ErrNotFound.
	PatchDataServer(dataServer *commonproto.DataServer) (*commonproto.DataServer, error)
	// DeleteDataServer removes a data server from the configuration, and
	// returns it, or ErrNotFound, or ErrFailedPrecondition if a namespace
	// would be left with fewer data servers than its replication factor.
	DeleteDataServer(name string) (*commonproto.DataServer, error)
	ListDataServer() (map[string]commonobject.Borrowed[*commonproto.DataServer], error)
	GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool, error)
}
