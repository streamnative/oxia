package oxia

import (
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"testing"
)

func TestToShard(t *testing.T) {
	for _, item := range []struct {
		assignment proto.ShardAssignment
		shard      Shard
		err        error
	}{
		{
			proto.ShardAssignment{
				ShardId: 1,
				Leader:  "leader:1234",
				ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
					Int32HashRange: &proto.Int32HashRange{
						MinHashInclusive: 1,
						MaxHashExclusive: 2,
					},
				},
			}, Shard{1, "leader:1234", HashRange{1, 2}}, nil},
		{proto.ShardAssignment{}, Shard{}, ErrorUnknownShardRange},
	} {
		result, err := toShard(&item.assignment)
		assert.Equal(t, item.shard, result)
		assert.ErrorIs(t, item.err, err)
	}
}
