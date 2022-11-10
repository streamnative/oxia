package oxia

import "oxia/proto"

func errPutResult(err error) PutResult {
	return PutResult{
		Stat: Stat{},
		Err:  err,
	}
}

func toShard(assignment *proto.ShardAssignment) (Shard, error) {
	if hashRange, err := toHashRange(assignment); err != nil {
		return Shard{}, err
	} else {
		return Shard{
			Id:        assignment.ShardId,
			Leader:    assignment.Leader,
			HashRange: hashRange,
		}, nil
	}
}

// TODO handle different shard range types
func toHashRange(assignment *proto.ShardAssignment) (HashRange, error) {
	switch boundaries := assignment.ShardBoundaries.(type) {
	case *proto.ShardAssignment_Int32HashRange:
		return HashRange{
			MinInclusive: boundaries.Int32HashRange.MinHashInclusive,
			MaxExclusive: boundaries.Int32HashRange.MaxHashExclusive,
		}, nil
	default:
		return HashRange{}, ErrorUnknownShardRange
	}
}
