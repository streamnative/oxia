// Copyright 2025 StreamNative, Inc.
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

package resources

import (
	"sync"

	"github.com/streamnative/oxia/coordinator/metadata"
	"github.com/streamnative/oxia/coordinator/model"
)

type StatusResource interface {
	Load() *model.ClusterStatus

	LoadWithVersion() (*model.ClusterStatus, metadata.Version)

	Swap(newStatus *model.ClusterStatus, version metadata.Version) bool

	Update(newStatus *model.ClusterStatus)

	UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata)

	DeleteShardMetadata(namespace string, shard int64)
}

var _ StatusResource = &status{}

type status struct {
	metadata metadata.Provider

	lock             sync.RWMutex
	current          *model.ClusterStatus
	currentVersionID metadata.Version
}

func (s *status) loadWithInitSlow() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.current != nil {
		return
	}
	var err error
	if s.current, s.currentVersionID, err = s.metadata.Get(); err != nil {
		panic(err)
	}
	if s.current == nil {
		s.current = &model.ClusterStatus{}
	}
}

func (s *status) Load() *model.ClusterStatus {
	current, _ := s.LoadWithVersion()
	return current
}

func (s *status) LoadWithVersion() (*model.ClusterStatus, metadata.Version) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.current == nil {
		s.lock.RUnlock()
		s.loadWithInitSlow()
		s.lock.RLock()
	}
	return s.current, s.currentVersionID
}

func (s *status) Swap(newStatus *model.ClusterStatus, version metadata.Version) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != version {
		return false
	}
	var err error
	if s.currentVersionID, err = s.metadata.Store(newStatus, s.currentVersionID); err != nil {
		panic(err)
	}
	s.current = newStatus
	return true
}

func (s *status) Update(newStatus *model.ClusterStatus) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var err error
	if s.currentVersionID, err = s.metadata.Store(newStatus, s.currentVersionID); err != nil {
		panic(err)
	}
	s.current = newStatus
}

func (s *status) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) {
	s.lock.Lock()
	defer s.lock.Unlock()

	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	ns.Shards[shard] = shardMetadata
	version, err := s.metadata.Store(clonedStatus, s.currentVersionID)
	if err != nil {
		panic(err)
	}
	s.current = clonedStatus
	s.currentVersionID = version
}

func (s *status) DeleteShardMetadata(namespace string, shard int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(clonedStatus.Namespaces, namespace)
	}
	version, err := s.metadata.Store(clonedStatus, s.currentVersionID)
	if err != nil {
		panic(err)
	}
	s.current = clonedStatus
	s.currentVersionID = version
}

func NewStatusResource(meta metadata.Provider) StatusResource {
	s := status{
		lock:             sync.RWMutex{},
		metadata:         meta,
		currentVersionID: metadata.NotExists,
		current:          nil,
	}
	s.Load()
	return &s
}
