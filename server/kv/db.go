package kv

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"io"
	"oxia/common"
	"oxia/proto"
	"oxia/server/wal"
	"time"

	pb "google.golang.org/protobuf/proto"
)

var ErrorBadVersion = errors.New("oxia: bad version")

const (
	commitIndexKey = common.InternalKeyPrefix + "commitIndex"
	epochKey       = common.InternalKeyPrefix + "epoch"
)

type UpdateOperationCallback interface {
	OnPut(WriteBatch, *proto.PutRequest, *proto.StorageEntry) (proto.Status, error)
	OnDelete(WriteBatch, string) error
}

type DB interface {
	io.Closer

	ProcessWrite(b *proto.WriteRequest, commitIndex int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error)
	ProcessRead(b *proto.ReadRequest) (*proto.ReadResponse, error)
	ReadCommitIndex() (int64, error)

	ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error)

	UpdateEpoch(newEpoch int64) error
	ReadEpoch() (epoch int64, err error)

	Snapshot() (Snapshot, error)
}

func NewDB(shardId uint32, factory KVFactory) (DB, error) {
	kv, err := factory.NewKV(shardId)
	if err != nil {
		return nil, err
	}

	db := &db{
		kv:      kv,
		shardId: shardId,
		log: log.Logger.With().
			Str("component", "db").
			Uint32("shard", shardId).
			Logger(),
	}

	commitIndex, err := db.ReadCommitIndex()
	if err != nil {
		return nil, err
	}

	db.notificationsTracker = newNotificationsTracker(commitIndex, kv)
	return db, nil
}

type db struct {
	kv                   KV
	shardId              uint32
	notificationsTracker *notificationsTracker
	log                  zerolog.Logger
}

func (d *db) Snapshot() (Snapshot, error) {
	return d.kv.Snapshot()
}

func (d *db) Close() error {
	return multierr.Combine(
		d.notificationsTracker.Close(),
		d.kv.Close(),
	)
}

func now() uint64 {
	return uint64(time.Now().UnixMilli())
}

func (d *db) ProcessWrite(b *proto.WriteRequest, commitIndex int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error) {
	res := &proto.WriteResponse{}

	batch := d.kv.NewWriteBatch()
	notifications := newNotifications(d.shardId, commitIndex, timestamp)

	for _, putReq := range b.Puts {
		if pr, err := d.applyPut(batch, notifications, putReq, timestamp, updateOperationCallback); err != nil {
			return nil, err
		} else {
			res.Puts = append(res.Puts, pr)
		}
	}

	for _, delReq := range b.Deletes {
		if dr, err := d.applyDelete(batch, notifications, delReq, updateOperationCallback); err != nil {
			return nil, err
		} else {
			res.Deletes = append(res.Deletes, dr)
		}
	}

	for _, delRangeReq := range b.DeleteRanges {
		if dr, err := d.applyDeleteRange(batch, notifications, delRangeReq, updateOperationCallback); err != nil {
			return nil, err
		} else {
			res.DeleteRanges = append(res.DeleteRanges, dr)
		}
	}
	if err := d.addCommitIndex(commitIndex, batch, timestamp); err != nil {
		return nil, err
	}

	// Add the notifications to the batch as well
	if err := d.addNotifications(batch, notifications); err != nil {
		return nil, err
	}

	if err := batch.Commit(); err != nil {
		return nil, err
	}

	d.notificationsTracker.UpdatedCommitIndex(commitIndex)

	if err := batch.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func (d *db) addNotifications(batch WriteBatch, notifications *notifications) error {
	value, err := pb.Marshal(&notifications.batch)
	if err != nil {
		return err
	}

	if err = batch.Put(notificationKey(notifications.batch.Offset), value); err != nil {
		return err
	}

	return nil
}

func (d *db) addCommitIndex(commitIndex int64, batch WriteBatch, timestamp uint64) error {
	commitIndexPayload := []byte(fmt.Sprintf("%d", commitIndex))
	_, err := d.applyPut(batch, nil, &proto.PutRequest{
		Key:             commitIndexKey,
		Payload:         commitIndexPayload,
		ExpectedVersion: nil,
	}, timestamp, nil)
	return err
}

func (d *db) ProcessRead(b *proto.ReadRequest) (*proto.ReadResponse, error) {
	res := &proto.ReadResponse{}

	for _, getReq := range b.Gets {
		if gr, err := applyGet(d.kv, getReq); err != nil {
			return nil, err
		} else {
			res.Gets = append(res.Gets, gr)
		}
	}

	for _, listReq := range b.Lists {
		if gr, err := applyList(d.kv, listReq); err != nil {
			return nil, err
		} else {
			res.Lists = append(res.Lists, gr)
		}
	}

	return res, nil
}

func (d *db) ReadCommitIndex() (int64, error) {
	kv := d.kv

	getReq := &proto.GetRequest{
		Key:            commitIndexKey,
		IncludePayload: true,
	}
	gr, err := applyGet(kv, getReq)
	if err != nil {
		return wal.InvalidOffset, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return wal.InvalidOffset, nil
	}

	var commitIndex int64
	if _, err = fmt.Sscanf(string(gr.Payload), "%d", &commitIndex); err != nil {
		return wal.InvalidOffset, err
	}
	return commitIndex, nil
}

func (d *db) UpdateEpoch(newEpoch int64) error {
	batch := d.kv.NewWriteBatch()

	if _, err := d.applyPut(batch, nil, &proto.PutRequest{
		Key:     epochKey,
		Payload: []byte(fmt.Sprintf("%d", newEpoch)),
	}, now(), nil); err != nil {
		return err
	}

	if err := batch.Commit(); err != nil {
		return err
	}

	if err := batch.Close(); err != nil {
		return err
	}

	// Since the epoch change is not stored in the WAL, we must force
	// the database to flush, in order to ensure the epoch change is durable
	return d.kv.Flush()
}

func (d *db) ReadEpoch() (epoch int64, err error) {
	getReq := &proto.GetRequest{
		Key:            epochKey,
		IncludePayload: true,
	}
	gr, err := applyGet(d.kv, getReq)
	if err != nil {
		return wal.InvalidEpoch, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return wal.InvalidEpoch, nil
	}

	if _, err = fmt.Sscanf(string(gr.Payload), "%d", &epoch); err != nil {
		return wal.InvalidEpoch, err
	}
	return epoch, nil
}

func (d *db) applyPut(batch WriteBatch, notifications *notifications, putReq *proto.PutRequest, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.PutResponse, error) {
	se, err := checkExpectedVersion(batch, putReq.Key, putReq.ExpectedVersion)
	if errors.Is(err, ErrorBadVersion) {
		return &proto.PutResponse{
			Status: proto.Status_UNEXPECTED_VERSION,
		}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	} else {
		// No version conflict
		if updateOperationCallback != nil {
			status, err := updateOperationCallback.OnPut(batch, putReq, se)
			if err != nil {
				return nil, err
			}
			if status != proto.Status_OK {
				return &proto.PutResponse{
					Status: status,
				}, nil
			}
		}
		if se == nil {
			se = &proto.StorageEntry{
				Version:               0,
				Payload:               putReq.Payload,
				CreationTimestamp:     timestamp,
				ModificationTimestamp: timestamp,
				SessionId:             putReq.SessionId,
			}
		} else {
			se.Version += 1
			se.Payload = putReq.Payload
			se.ModificationTimestamp = timestamp
			se.SessionId = putReq.SessionId
		}

		ser, err := pb.Marshal(se)
		if err != nil {
			return nil, err
		}

		if err = batch.Put(putReq.Key, ser); err != nil {
			return nil, err
		}

		if notifications != nil {
			notifications.Modified(putReq.Key, se.Version)
		}

		stat := &proto.Stat{
			Version:           se.Version,
			CreatedTimestamp:  se.CreationTimestamp,
			ModifiedTimestamp: se.ModificationTimestamp,
		}

		d.log.Debug().
			Str("key", putReq.Key).
			Interface("stat", stat).
			Msg("Applied put operation")

		return &proto.PutResponse{Stat: stat}, nil
	}
}

func (d *db) applyDelete(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteResponse, error) {
	se, err := checkExpectedVersion(batch, delReq.Key, delReq.ExpectedVersion)

	if errors.Is(err, ErrorBadVersion) {
		return &proto.DeleteResponse{Status: proto.Status_UNEXPECTED_VERSION}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	} else if se == nil {
		return &proto.DeleteResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else {
		if updateOperationCallback != nil {
			err = updateOperationCallback.OnDelete(batch, delReq.Key)
			if err != nil {
				return nil, err
			}

		}
		if err = batch.Delete(delReq.Key); err != nil {
			return &proto.DeleteResponse{}, err
		}

		if notifications != nil {
			notifications.Deleted(delReq.Key)
		}

		d.log.Debug().
			Str("key", delReq.Key).
			Msg("Applied delete operation")
		return &proto.DeleteResponse{Status: proto.Status_OK}, nil
	}
}

func (d *db) applyDeleteRange(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteRangeResponse, error) {
	if notifications != nil {
		it := batch.KeyRangeScan(delReq.StartInclusive, delReq.EndExclusive)
		for it.Next() {
			notifications.Deleted(it.Key())
			if updateOperationCallback != nil {
				err := updateOperationCallback.OnDelete(batch, it.Key())
				if err != nil {
					return nil,
						errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
				}
			}
		}

		if err := it.Close(); err != nil {
			return nil, errors.Wrap(err, "oxia db: failed to delete range")
		}

	}

	if err := batch.DeleteRange(delReq.StartInclusive, delReq.EndExclusive); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to delete range")
	}

	d.log.Debug().
		Str("key-start", delReq.StartInclusive).
		Str("key-end", delReq.EndExclusive).
		Msg("Applied delete range operation")
	return &proto.DeleteRangeResponse{Status: proto.Status_OK}, nil
}

func applyGet(kv KV, getReq *proto.GetRequest) (*proto.GetResponse, error) {
	payload, closer, err := kv.Get(getReq.Key)
	if errors.Is(err, ErrorKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
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

	return &proto.GetResponse{
		Payload: resPayload,
		Stat: &proto.Stat{
			Version:           se.Version,
			CreatedTimestamp:  se.CreationTimestamp,
			ModifiedTimestamp: se.ModificationTimestamp,
		},
	}, nil
}

func applyList(kv KV, listReq *proto.ListRequest) (*proto.ListResponse, error) {
	it := kv.KeyRangeScan(listReq.StartInclusive, listReq.EndExclusive)

	res := &proto.ListResponse{}

	for ; it.Valid(); it.Next() {
		res.Keys = append(res.Keys, it.Key())
	}

	if err := it.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func GetStorageEntry(batch WriteBatch, key string) (*proto.StorageEntry, error) {
	payload, closer, err := batch.Get(key)
	if err != nil {
		return nil, err
	}

	se, err := deserialize(payload, closer)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func checkExpectedVersion(batch WriteBatch, key string, expectedVersion *int64) (*proto.StorageEntry, error) {
	se, err := GetStorageEntry(batch, key)
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

func (d *db) ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error) {
	return d.notificationsTracker.ReadNextNotifications(ctx, startOffset)
}
