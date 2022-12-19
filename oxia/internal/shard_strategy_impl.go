package internal

import (
	"oxia/common"
)

type shardStrategyImpl struct {
	hashFunc func(string) uint32
}

func NewShardStrategy() ShardStrategy {
	return &shardStrategyImpl{
		hashFunc: common.Xxh332,
	}
}

func (s *shardStrategyImpl) Get(key string) func(Shard) bool {
	hash := s.hashFunc(key)
	return func(shard Shard) bool {
		hashRange := shard.HashRange
		return hashRange.MinInclusive <= hash && hash <= hashRange.MaxInclusive
	}
}
