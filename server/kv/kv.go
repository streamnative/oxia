package kv

import (
	"github.com/pkg/errors"
	"io"
)

var (
	ErrorKeyNotFound = errors.New("oxia: key not found")
)

type WriteBatch interface {
	io.Closer

	Put(key string, payload []byte) error
	Delete(key string) error
	Get(key string) ([]byte, io.Closer, error)

	DeleteRange(lowerBound, upperBound string) error
	KeyRangeScan(lowerBound, upperBound string) KeyIterator

	Commit() error
}

type KeyIterator interface {
	io.Closer

	Valid() bool
	Key() string
	Next() bool
}

type KeyValueIterator interface {
	KeyIterator

	Value() ([]byte, error)
}

type KV interface {
	io.Closer

	NewWriteBatch() WriteBatch

	Get(key string) ([]byte, io.Closer, error)

	KeyRangeScan(lowerBound, upperBound string) KeyIterator

	Snapshot() KeyValueIterator
}

type KVFactoryOptions struct {
	DataDir   string
	CacheSize int64

	// Create a pure in-memory database. Used for unit-tests
	InMemory bool
}

var DefaultKVFactoryOptions = &KVFactoryOptions{
	DataDir:   "data",
	CacheSize: 100 * 1024 * 1024,
	InMemory:  false,
}

type KVFactory interface {
	io.Closer

	NewKV(shardId uint32) (KV, error)
}
