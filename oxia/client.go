package oxia

import (
	"errors"
	"io"
)

const (
	VersionNotExists int64 = -1
)

var (
	ErrorKeyNotFound   = errors.New("key not found")
	ErrorBadVersion    = errors.New("bad version")
	ErrorUnknownStatus = errors.New("unknown status")
	ErrorShuttingDown  = errors.New("shutting down")
)

type Client interface {
	io.Closer
	Put(key string, payload []byte, expectedVersion *int64) <-chan PutResult
	Delete(key string, expectedVersion *int64) <-chan error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error
	Get(key string) <-chan GetResult
	GetRange(minKeyInclusive string, maxKeyExclusive string) <-chan GetRangeResult
}

type ClientOptions struct {
	serviceURL string
}

func NewClient(options *ClientOptions) Client {
	return newMemoryClient()
}

type Stat struct {
	Version           int64
	CreatedTimestamp  uint64
	ModifiedTimestamp uint64
}

type Value struct {
	Payload []byte
	Stat    Stat
}

type PutResult struct {
	Stat Stat
	Err  error
}

type GetResult struct {
	Value Value
	Err   error
}

type GetRangeResult struct {
	Keys []string
	Err  error
}
