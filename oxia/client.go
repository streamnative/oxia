package oxia

import (
	"errors"
	"io"
	"time"
)

const (
	DefaultBatchLinger  = 100 * time.Millisecond
	DefaultBatchMaxSize = 100
	DefaultBatchTimeout = 1 * time.Minute
)

var (
	//not easy to use as a pointer if a const
	VersionNotExists int64 = -1

	ErrorKeyNotFound   = errors.New("key not found")
	ErrorBadVersion    = errors.New("bad version")
	ErrorUnknownStatus = errors.New("unknown status")
	ErrorShuttingDown  = errors.New("shutting down")
)

type Options struct {
	ServiceUrl   string
	BatchLinger  time.Duration
	BatchMaxSize int
	BatchTimeout time.Duration
	InMemory     bool
}

type Client interface {
	io.Closer
	Put(key string, payload []byte, expectedVersion *int64) <-chan PutResult
	Delete(key string, expectedVersion *int64) <-chan error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error
	Get(key string) <-chan GetResult
	GetRange(minKeyInclusive string, maxKeyExclusive string) <-chan GetRangeResult
}

type Stat struct {
	Version           int64
	CreatedTimestamp  uint64
	ModifiedTimestamp uint64
}

type PutResult struct {
	Stat Stat
	Err  error
}

type GetResult struct {
	Payload []byte
	Stat    Stat
	Err     error
}

type GetRangeResult struct {
	Keys []string
	Err  error
}
