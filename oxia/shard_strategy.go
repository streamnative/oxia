package oxia

type ShardStrategy interface {
	Get(key string) func(Shard) bool
}

type Shard struct {
	Id        uint32
	Leader    string
	HashRange HashRange
}

type HashRange struct {
	MinInclusive uint32
	MaxExclusive uint32
}

type hashRangeShardStrategy struct {
	hash func(string) uint32
}

func NewHashRangeShardStrategy(hash func(string) uint32) ShardStrategy {
	return &hashRangeShardStrategy{
		hash: hash,
	}
}

func (s *hashRangeShardStrategy) Get(key string) func(Shard) bool {
	hash := s.hash(key)
	return func(shard Shard) bool {
		hashRange := shard.HashRange
		return hashRange.MinInclusive <= hash && hash < hashRange.MaxExclusive
	}
}

func overlap(a *HashRange, b *HashRange) bool {
	return !(a.MinInclusive >= b.MaxExclusive || a.MaxExclusive <= b.MinInclusive)
}
