package client

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShardStrategy(t *testing.T) {
	shardStrategy := &shardStrategyImpl{
		hashFunc: func(key string) uint32 {
			return 2
		},
	}
	predicate := shardStrategy.Get("foo")

	for _, item := range []struct {
		minInclusive uint32
		maxExclusive uint32
		match        bool
	}{
		{1, 4, true},
		{2, 4, true},
		{3, 4, false},
		{1, 2, false},
	} {
		shard := Shard{
			HashRange: HashRange{
				MinInclusive: item.minInclusive,
				MaxExclusive: item.maxExclusive,
			},
		}
		assert.Equal(t, item.match, predicate(shard))
	}
}
