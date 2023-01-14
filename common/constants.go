package common

import "time"

const (
	MetadataShardId   = "shard-id"
	MetadataSessionId = "session-id"

	MaxSessionTimeout = 5 * time.Minute
	MinSessionTimeout = 2 * time.Second
)
