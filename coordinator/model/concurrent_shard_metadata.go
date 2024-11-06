package model

import (
	"sync"
)

// ConcurrentShardMetadata is a race safe mutable shard metadata
type ConcurrentShardMetadata struct {
	inner *ShardMetadata
	sync.RWMutex
}

func (c *ConcurrentShardMetadata) GetTerm() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.inner.Term
}

func (c *ConcurrentShardMetadata) CloneData() ShardMetadata {
	c.RLock()
	defer c.RUnlock()
	return c.inner.Clone()
}

func (c *ConcurrentShardMetadata) GetStatus() ShardStatus {
	c.RLock()
	defer c.RUnlock()
	return c.inner.Status
}

func (c *ConcurrentShardMetadata) GetLeader() *ServerAddress {
	c.RLock()
	defer c.RUnlock()
	return c.inner.Leader
}

func (c *ConcurrentShardMetadata) GetEnsemble() []ServerAddress {
	c.RLock()
	defer c.RUnlock()
	return c.inner.Ensemble
}

func (c *ConcurrentShardMetadata) GetRemovedNodes() []ServerAddress {
	c.RLock()
	defer c.RUnlock()
	return c.inner.RemovedNodes
}

func (c *ConcurrentShardMetadata) PrepareNewElection(servers []ServerAddress) {
	c.Lock()
	defer c.Unlock()
	c.inner.Status = ShardStatusElection
	c.inner.Leader = nil
	c.inner.Term++

	index := map[string]*ServerAddress{}
	for key, server := range servers {
		index[server.Internal] = &servers[key]
	}
	refreshedEnsembleServiceInfo := make([]ServerAddress, 0)
	for _, currentServer := range c.inner.Ensemble {
		logicalNodeId := currentServer.Internal
		if refreshedServiceInfo := index[logicalNodeId]; refreshedServiceInfo != nil {
			refreshedEnsembleServiceInfo = append(refreshedEnsembleServiceInfo, *refreshedServiceInfo)
			continue
		}
		refreshedEnsembleServiceInfo = append(refreshedEnsembleServiceInfo, currentServer)
	}
	// it's a safe point to update the service info
	c.inner.Ensemble = refreshedEnsembleServiceInfo
}

func (c *ConcurrentShardMetadata) PrepareSwap(removedNodes []ServerAddress, newEnsemble []ServerAddress) {
	c.Lock()
	defer c.Unlock()
	c.inner.RemovedNodes = removedNodes
	c.inner.Ensemble = newEnsemble
}

func (c *ConcurrentShardMetadata) Update(metadata *ShardMetadata) {
	c.Lock()
	defer c.Unlock()

	c.inner = metadata
}

func NewConcurrentShardMetadata(metadata *ShardMetadata) *ConcurrentShardMetadata {
	return &ConcurrentShardMetadata{
		inner:   metadata,
		RWMutex: sync.RWMutex{},
	}
}
