package server

import (
	"github.com/pkg/errors"
	"io"
	"oxia/proto"
)

type KeyValueStore interface {
	io.Closer
	Apply(op *proto.PutOp, timestamp uint64) (KVEntry, error)
	Get(op *proto.GetOp) (KVEntry, error)
}

type inMemoryKVStore struct {
	store map[string]KVEntry
}

type KVEntry struct {
	Payload []byte
	Version uint64
	Created uint64
	Updated uint64
}

func NewInMemoryKVStore() KeyValueStore {
	return &inMemoryKVStore{
		store: make(map[string]KVEntry),
	}
}

func (k *inMemoryKVStore) Close() error {
	k.store = make(map[string]KVEntry)
	return nil
}

func (k *inMemoryKVStore) Apply(op *proto.PutOp, timestamp uint64) (KVEntry, error) {
	old, existed := k.store[op.Key]
	var version uint64
	var created uint64
	if existed {
		created = old.Created
		version = 0
	} else {
		created = timestamp
		version = old.Version
	}
	if op.ExpectedVersion != nil && *op.ExpectedVersion != version {
		return KVEntry{}, errors.Errorf("Version check (%d != %d)", *op.ExpectedVersion, version)
	}
	entry := KVEntry{
		Payload: op.Payload,
		Version: version,
		Created: created,
		Updated: timestamp,
	}
	k.store[op.Key] = entry
	return entry, nil

}

func (k *inMemoryKVStore) Get(op *proto.GetOp) (KVEntry, error) {
	val, ok := k.store[op.Key]
	if ok {
		return val, nil
	} else {
		return KVEntry{}, nil
	}
}
