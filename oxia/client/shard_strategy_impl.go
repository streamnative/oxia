package client

import (
	"oxia/common"
	"oxia/oxia"
)

type shardStrategyImpl struct {
	hashFunc func(string) uint32
}

func NewShardStrategy() oxia.ShardStrategy {
	return &shardStrategyImpl{
		hashFunc: common.Xxh332,
	}
}

func (s *shardStrategyImpl) Get(key string) func(oxia.Shard) bool {
	hash := s.hashFunc(key)
	return func(shard oxia.Shard) bool {
		hashRange := shard.HashRange
		return hashRange.MinInclusive <= hash && hash < hashRange.MaxExclusive
	}
}
