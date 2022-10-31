package server

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"oxia/proto"
)

type boltKV struct {
	store *bolt.DB
	shard string
}

func (b *boltKV) Close() error {
	return b.store.Close()
}

func (b *boltKV) Apply(op *proto.PutOp, timestamp uint64) (KVEntry, error) {
	entry := KVEntry{}
	err := b.store.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.shard))
		key := []byte(op.Key)
		oldBytes := bucket.Get(key)
		old := &KVEntry{}
		err := json.Unmarshal(oldBytes, old)
		if err != nil {
			return err
		}
		var version uint64
		var created uint64
		if old != nil {
			created = old.Created
			version = 0
		} else {
			created = timestamp
			version = old.Version
		}
		if op.ExpectedVersion != nil && *op.ExpectedVersion != version {
			return errors.Errorf("Version check (%d != %d)", *op.ExpectedVersion, version)
		}
		entry.Payload = op.Payload
		entry.Version = version
		entry.Created = created
		entry.Updated = timestamp
		newBytes, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		return bucket.Put(key, newBytes)
	})
	return entry, err

}

func (b *boltKV) Get(op *proto.GetOp) (KVEntry, error) {
	entry := KVEntry{}
	err := b.store.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.shard))
		key := []byte(op.Key)
		oldBytes := bucket.Get(key)
		old := &KVEntry{}
		err := json.Unmarshal(oldBytes, old)
		if err != nil {
			return err
		}
		if old != nil {
			entry.Payload = old.Payload
			entry.Version = old.Version
			entry.Created = old.Created
			entry.Updated = old.Updated
		}
		return nil
	})
	return entry, err
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
