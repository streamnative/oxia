package server

import (
	"io"
	"oxia/proto"
	"time"
)

type KeyValueStore interface {
	io.Closer
	Apply(op *proto.PutOp) (*proto.Stat, error)
	Get(op *proto.PutOp) ([]byte, error)
}

type kvStore struct {
	// TODO persistent implementation
	store map[string][]byte
}

func NewKVStore(shard uint32) KeyValueStore {
	return &kvStore{
		store: make(map[string][]byte),
	}
}

func (k kvStore) Close() error {
	k.store = make(map[string][]byte)
	return nil
}

func (k kvStore) Apply(op *proto.PutOp) (*proto.Stat, error) {
	k.store[op.Key] = op.Payload
	return &proto.Stat{
		// TODO values that make sense
		Version:           0,
		CreatedTimestamp:  uint64(time.Now().Nanosecond()),
		ModifiedTimestamp: uint64(time.Now().Nanosecond()),
	}, nil
}

func (k kvStore) Get(op *proto.PutOp) ([]byte, error) {
	// TODO define GetOp
	val, _ := k.store[op.Key]
	return val, nil
}
