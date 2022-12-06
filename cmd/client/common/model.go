package common

type OutputStat struct {
	Version           int64  `json:"version"`
	CreatedTimestamp  uint64 `json:"created_timestamp"`
	ModifiedTimestamp uint64 `json:"modified_timestamp"`
}

type OutputError struct {
	Err string `json:"error,omitempty"`
}
