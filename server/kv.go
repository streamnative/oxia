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
	Payload   []byte
	VersionId uint64
	Created   uint64
	Updated   uint64
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
	var versionId uint64
	var created uint64
	if existed {
		created = old.Created
		versionId = 0
	} else {
		created = timestamp
		versionId = old.VersionId
	}
	if op.ExpectedVersionId != nil && *op.ExpectedVersionId != versionId {
		return KVEntry{}, errors.Errorf("Version check (%d != %d)", *op.ExpectedVersionId, versionId)
	}
	entry := KVEntry{
		Payload:   op.Payload,
		VersionId: versionId,
		Created:   created,
		Updated:   timestamp,
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
