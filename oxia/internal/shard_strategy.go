package internal

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
	MaxInclusive uint32
}
