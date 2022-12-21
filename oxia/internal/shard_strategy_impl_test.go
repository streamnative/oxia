package internal

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
		maxInclusive uint32
		match        bool
	}{
		{1, 3, true},
		{2, 3, true},
		{1, 2, true},
		{1, 1, false},
		{3, 3, false},
	} {
		shard := Shard{
			HashRange: HashRange{
				MinInclusive: item.minInclusive,
				MaxInclusive: item.maxInclusive,
			},
		}
		assert.Equal(t, item.match, predicate(shard))
	}
}
