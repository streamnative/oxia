package resources

import (
	"sync"

	"github.com/streamnative/oxia/coordinator/metadata"
	"github.com/streamnative/oxia/coordinator/model"
)

type StatusResource interface {
	Load() *model.ClusterStatus

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
}

func (s *status) Load() *model.ClusterStatus {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.current == nil {
		s.lock.RUnlock()
		s.loadWithInitSlow()
		s.lock.RLock()
	}
	return s.current
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
	return &status{
		lock:             sync.RWMutex{},
		metadata:         meta,
		currentVersionID: metadata.NotExists,
		current:          nil,
	}
}
