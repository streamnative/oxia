package internal

import (
	"oxia/oxia"
	"oxia/proto"
)

func ErrPutResult(err error) oxia.PutResult {
	return oxia.PutResult{
		Err: err,
	}
}

func toShard(assignment *proto.ShardAssignment) oxia.Shard {
	return oxia.Shard{
		Id:        assignment.ShardId,
		Leader:    assignment.Leader,
		HashRange: toHashRange(assignment),
	}
}

func toHashRange(assignment *proto.ShardAssignment) oxia.HashRange {
	switch boundaries := assignment.ShardBoundaries.(type) {
	case *proto.ShardAssignment_Int32HashRange:
		return oxia.HashRange{
			MinInclusive: boundaries.Int32HashRange.MinHashInclusive,
			MaxExclusive: boundaries.Int32HashRange.MaxHashExclusive,
		}
	default:
		panic("unknown shard boundary")
	}
}
