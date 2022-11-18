package oxia

import (
	"errors"
	"io"
	"time"
)

const (
	DefaultBatchLinger  = 5 * time.Millisecond
	DefaultBatchMaxSize = 1000
	DefaultBatchTimeout = 30 * time.Second
)

var (
	//not easy to use as a pointer if a const
	VersionNotExists int64 = -1

	ErrorKeyNotFound   = errors.New("key not found")
	ErrorBadVersion    = errors.New("bad version")
	ErrorUnknownStatus = errors.New("unknown status")
)

type ClientOptions struct {
	ServiceUrl   string
	BatchLinger  time.Duration
	BatchMaxSize int
	BatchTimeout time.Duration
}

type AsyncClient interface {
	io.Closer
	Put(key string, payload []byte, expectedVersion *int64) <-chan PutResult
	Delete(key string, expectedVersion *int64) <-chan error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error
	Get(key string) <-chan GetResult
	GetRange(minKeyInclusive string, maxKeyExclusive string) <-chan GetRangeResult
}

type SyncClient interface {
	io.Closer
	Put(key string, payload []byte, expectedVersion *int64) (Stat, error)
	Delete(key string, expectedVersion *int64) error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) error
	Get(key string) ([]byte, Stat, error)
	GetRange(minKeyInclusive string, maxKeyExclusive string) ([]string, error)
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
