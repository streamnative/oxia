package common

import (
	"math"
)

type Shard struct {
	Id  uint32
	Min uint32
	Max uint32
}

func GenerateShards(numShards uint32) []Shard {
	bucketSize := (math.MaxUint32 / numShards) + 1
	shards := make([]Shard, numShards)
	for i := uint32(0); i < numShards; i++ {
		lowerBound := i * bucketSize
		upperBound := lowerBound + bucketSize - 1
		if i == numShards-1 {
			upperBound = math.MaxUint32
		}
		shards[i] = Shard{
			Id:  i,
			Min: lowerBound,
			Max: upperBound,
		}
	}
	return shards
}
