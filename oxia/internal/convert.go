// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"

	"github.com/oxia-db/oxia/common/proto"
)

func toShard(assignment *proto.ShardAssignment) (Shard, error) {
	hashRange, err := toHashRange(assignment)
	if err != nil {
		return Shard{}, err
	}
	return Shard{
		Id:        assignment.Shard,
		Leader:    assignment.Leader,
		HashRange: hashRange,
	}, nil
}

func toHashRange(assignment *proto.ShardAssignment) (HashRange, error) {
	switch boundaries := assignment.ShardBoundaries.(type) {
	case *proto.ShardAssignment_Int32HashRange:
		return HashRange{
			MinInclusive: boundaries.Int32HashRange.MinHashInclusive,
			MaxInclusive: boundaries.Int32HashRange.MaxHashInclusive,
		}, nil
	default:
		return HashRange{}, fmt.Errorf("shard %d has an unsupported hash range boundary %T",
			assignment.GetShard(), assignment.ShardBoundaries)
	}
}

func hashRange(minInclusive uint32, maxInclusive uint32) HashRange {
	return HashRange{
		MinInclusive: minInclusive,
		MaxInclusive: maxInclusive,
	}
}
