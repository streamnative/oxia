package oxia

import (
	"github.com/pkg/errors"
	"io"
)

const (
	VersionNotExists int64 = -1
)

var (
	ErrorKeyNotFound = errors.New("Key not found")
	ErrorBadVersion  = errors.New("Bad version")
)

type Stat struct {
	Version           int64
	CreatedTimestamp  uint64
	ModifiedTimestamp uint64
}

type Value struct {
	Payload []byte
	Stat    Stat
}

type Client interface {
	io.Closer
	Put(key string, payload []byte, expectedVersion *int64) (Stat, error)
	Delete(key string, expectedVersion *int64) error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) error
	Get(key string) (Value, error)
	GetRange(minKeyInclusive string, maxKeyExclusive string) ([]string, error)
}
