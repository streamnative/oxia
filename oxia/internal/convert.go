package internal

import (
	"oxia/proto"
)

func toShard(assignment *proto.ShardAssignment) Shard {
	return Shard{
		Id:        assignment.ShardId,
		Leader:    assignment.Leader,
		HashRange: toHashRange(assignment),
	}
}

func toHashRange(assignment *proto.ShardAssignment) HashRange {
	switch boundaries := assignment.ShardBoundaries.(type) {
	case *proto.ShardAssignment_Int32HashRange:
		return HashRange{
			MinInclusive: boundaries.Int32HashRange.MinHashInclusive,
			MaxInclusive: boundaries.Int32HashRange.MaxHashInclusive,
		}
	default:
		panic("unknown shard boundary")
	}
}

func hashRange(minInclusive uint32, maxInclusive uint32) HashRange {
	return HashRange{
		MinInclusive: minInclusive,
		MaxInclusive: maxInclusive,
	}
}
