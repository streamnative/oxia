package server

import (
	"github.com/boltdb/bolt"
	"oxia/proto"
)

// TODO keep track of version for expectedVersion checks
// TODO keep track of createTimestamp, lastUpdateTS for the fun of it
type boltKV struct {
	store *bolt.DB
	shard string
}

func (b *boltKV) Close() error {
	return b.store.Close()
}

func (b *boltKV) Apply(op *proto.PutOp) (*proto.Stat, error) {
	stat := &proto.Stat{}
	err := b.store.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.shard))
		key := []byte(op.Key)
		bucket.Get(key)
		// TODO fill stat, save new stat
		return bucket.Put(key, op.Payload)
	})
	return stat, err

}

func (b *boltKV) Get(op *proto.PutOp) ([]byte, error) {
	var value []byte
	err := b.store.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.shard))
		key := []byte(op.Key)
		value = bucket.Get(key)
		return nil
	})
	return value, err
}

func NewBoltKV(shard string, kvDir string) KeyValueStore {
	db, err := bolt.Open(kvDir+"/"+shard, 0600, nil)
	if err != nil {
		return nil
	}
	return &boltKV{
		store: db,
		shard: shard,
	}
}
