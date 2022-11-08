package kv

import (
	"github.com/pkg/errors"
	"io"
	"oxia/proto"
	"time"

	pb "google.golang.org/protobuf/proto"
)

var (
	ErrorBadVersion = errors.New("oxia: bad version")
)

type DB interface {
	io.Closer

	ProcessBatch(b *proto.BatchRequest) (*proto.BatchResponse, error)
}

func NewDB(shardId int32, factory KVFactory) (DB, error) {
	kv, err := factory.NewKV(shardId)
	if err != nil {
		return nil, err
	}

	return &db{
		kv: kv,
	}, nil
}

type db struct {
	kv KV
}

func (d *db) Close() error {
	return d.kv.Close()
}

func (d *db) ProcessBatch(b *proto.BatchRequest) (*proto.BatchResponse, error) {
	res := &proto.BatchResponse{}

	batch := d.kv.NewWriteBatch()
	now := uint64(time.Now().UnixMilli())

	for _, putReq := range b.GetPut() {
		se, err := checkExpectedVersion(batch, putReq.Key, putReq.ExpectedVersion)
		if errors.Is(err, ErrorBadVersion) {
			res.Put = append(res.Put, &proto.PutResponse{
				Error: proto.Error_BAD_VERSION,
			})
		} else if err != nil {
			return nil, errors.Wrap(err, "oxia db: failed to apply batch")
		} else {
			// No version conflict
			if se == nil {
				se = &proto.StorageEntry{
					Version:               0,
					Payload:               putReq.Payload,
					CreationTimestamp:     now,
					ModificationTimestamp: now,
				}
			} else {
				se.Version += 1
				se.Payload = putReq.Payload
				se.ModificationTimestamp = now
			}

			ser, err := pb.Marshal(se)
			if err != nil {
				return nil, err
			}

			if err = batch.Put(putReq.Key, ser); err != nil {
				return nil, err
			}

			res.Put = append(res.Put, &proto.PutResponse{
				Stat: &proto.Stat{
					Version:           se.Version,
					CreatedTimestamp:  se.CreationTimestamp,
					ModifiedTimestamp: se.ModificationTimestamp,
				},
			})
		}
	}

	for _, delReq := range b.GetDelete() {
		var deleteResult proto.Error

		se, err := checkExpectedVersion(batch, delReq.Key, delReq.ExpectedVersion)
		if errors.Is(err, ErrorBadVersion) {
			deleteResult = proto.Error_BAD_VERSION
		} else if err != nil {
			return nil, errors.Wrap(err, "oxia db: failed to apply batch")
		} else if se == nil {
			deleteResult = proto.Error_KEY_NOT_FOUND
		} else {
			deleteResult = proto.Error_NO_ERROR
			batch.Delete(delReq.Key)
		}

		res.Delete = append(res.Delete, &proto.DeleteResponse{
			Error: deleteResult,
		})
	}

	for _, getReq := range b.GetGet() {
		payload, closer, err := batch.Get(getReq.Key)
		if errors.Is(err, ErrorKeyNotFound) {
			res.Get = append(res.Get, &proto.GetResponse{
				Error: proto.Error_KEY_NOT_FOUND,
			})
			continue
		} else if err != nil {
			return nil, errors.Wrap(err, "oxia db: failed to apply batch")
		}

		se, err := deserialize(payload, closer)
		if err != nil {
			return nil, err
		}

		resPayload := se.Payload
		if !getReq.IncludePayload {
			resPayload = nil
		}

		res.Get = append(res.Get, &proto.GetResponse{
			Payload: resPayload,
			Stat: &proto.Stat{
				Version:           se.Version,
				CreatedTimestamp:  se.CreationTimestamp,
				ModifiedTimestamp: se.ModificationTimestamp,
			},
		})
	}

	if err := batch.Commit(); err != nil {
		return nil, err
	}

	if err := batch.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func checkExpectedVersion(batch WriteBatch, key string, expectedVersion *int64) (*proto.StorageEntry, error) {
	payload, closer, err := batch.Get(key)
	if err != nil {
		if errors.Is(err, ErrorKeyNotFound) {
			if expectedVersion == nil || *expectedVersion == -1 {
				// OK, we were checking that the key was not there, and it's indeed not there
				return nil, nil
			} else {
				return nil, ErrorBadVersion
			}
		}
		return nil, err
	}

	se, err := deserialize(payload, closer)
	if err != nil {
		return nil, err
	}

	if expectedVersion != nil && se.Version != *expectedVersion {
		return nil, ErrorBadVersion
	}

	return se, nil
}

func deserialize(payload []byte, closer io.Closer) (*proto.StorageEntry, error) {
	se := &proto.StorageEntry{}
	if err := pb.Unmarshal(payload, se); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize storage entry")
	}

	if err := closer.Close(); err != nil {
		return nil, err
	}
	return se, nil
}
