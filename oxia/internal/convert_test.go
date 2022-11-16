package internal

import (
	"github.com/stretchr/testify/assert"
	"oxia/oxia"
	"oxia/proto"
	"testing"
)

func TestToShard(t *testing.T) {
	for _, item := range []struct {
		assignment *proto.ShardAssignment
		shard      oxia.Shard
		err        error
	}{
		{
			&proto.ShardAssignment{
				ShardId: 1,
				Leader:  "leader:1234",
				ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
					Int32HashRange: &proto.Int32HashRange{
						MinHashInclusive: 1,
						MaxHashExclusive: 2,
					},
				},
			}, oxia.Shard{
				Id:        1,
				Leader:    "leader:1234",
				HashRange: hashRange(1, 2),
			}, nil},
	} {
		result := toShard(item.assignment)
		assert.Equal(t, item.shard, result)
	}
}
