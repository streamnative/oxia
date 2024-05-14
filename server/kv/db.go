// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/wal"
)

var ErrBadVersionId = errors.New("oxia: bad version id")

const (
	commitOffsetKey = common.InternalKeyPrefix + "commit-offset"
	termKey         = common.InternalKeyPrefix + "term"
)

type UpdateOperationCallback interface {
	OnPut(WriteBatch, *proto.PutRequest, *proto.StorageEntry) (proto.Status, error)
	OnDelete(WriteBatch, string) error
}

type DB interface {
	io.Closer

	ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error)
	Get(request *proto.GetRequest) (*proto.GetResponse, error)
	List(request *proto.ListRequest) (KeyIterator, error)
	ReadCommitOffset() (int64, error)

	ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error)

	UpdateTerm(newTerm int64) error
	ReadTerm() (term int64, err error)

	Snapshot() (Snapshot, error)

	// Delete and close the database and all its files
	Delete() error
}

func NewDB(namespace string, shardId int64, factory Factory, notificationRetentionTime time.Duration, clock common.Clock) (DB, error) {
	kv, err := factory.NewKV(namespace, shardId)
	if err != nil {
		return nil, err
	}

	labels := metrics.LabelsForShard(namespace, shardId)
	db := &db{
		kv:      kv,
		shardId: shardId,
		log: slog.With(
			slog.String("component", "db"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
		),

		batchWriteLatencyHisto: metrics.NewLatencyHistogram("oxia_server_db_batch_write_latency",
			"The time it takes to write a batch in the db", labels),
		getLatencyHisto: metrics.NewLatencyHistogram("oxia_server_db_get_latency",
			"The time it takes to get from the db", labels),
		listLatencyHisto: metrics.NewLatencyHistogram("oxia_server_db_list_latency",
			"The time it takes to read a list from the db", labels),
		putCounter: metrics.NewCounter("oxia_server_db_puts",
			"The total number of put operations", "count", labels),
		deleteCounter: metrics.NewCounter("oxia_server_db_deletes",
			"The total number of delete operations", "count", labels),
		deleteRangesCounter: metrics.NewCounter("oxia_server_db_delete_ranges",
			"The total number of delete ranges operations", "count", labels),
		getCounter: metrics.NewCounter("oxia_server_db_gets",
			"The total number of get operations", "count", labels),
		listCounter: metrics.NewCounter("oxia_server_db_lists",
			"The total number of list operations", "count", labels),
	}

	commitOffset, err := db.ReadCommitOffset()
	if err != nil {
		return nil, err
	}

	db.notificationsTracker = newNotificationsTracker(namespace, shardId, commitOffset, kv, notificationRetentionTime, clock)
	return db, nil
}

type db struct {
	kv                   KV
	shardId              int64
	notificationsTracker *notificationsTracker
	log                  *slog.Logger

	putCounter          metrics.Counter
	deleteCounter       metrics.Counter
	deleteRangesCounter metrics.Counter
	getCounter          metrics.Counter
	listCounter         metrics.Counter

	batchWriteLatencyHisto metrics.LatencyHistogram
	getLatencyHisto        metrics.LatencyHistogram
	listLatencyHisto       metrics.LatencyHistogram
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

func (d *db) Delete() error {
	return multierr.Combine(
		d.notificationsTracker.Close(),
		d.kv.Delete(),
	)
}

func now() uint64 {
	return uint64(time.Now().UnixMilli())
}

func (d *db) applyWriteRequest(b *proto.WriteRequest, batch WriteBatch, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*notifications, *proto.WriteResponse, error) {
	res := &proto.WriteResponse{}
	notifications := newNotifications(d.shardId, commitOffset, timestamp)

	d.putCounter.Add(len(b.Puts))
	for _, putReq := range b.Puts {
		pr, err := d.applyPut(commitOffset, batch, notifications, putReq, timestamp, updateOperationCallback)
		if err != nil {
			return nil, nil, err
		}
		res.Puts = append(res.Puts, pr)
	}

	d.deleteCounter.Add(len(b.Deletes))
	for _, delReq := range b.Deletes {
		dr, err := d.applyDelete(batch, notifications, delReq, updateOperationCallback)
		if err != nil {
			return nil, nil, err
		}

		res.Deletes = append(res.Deletes, dr)
	}

	d.deleteRangesCounter.Add(len(b.DeleteRanges))
	for _, delRangeReq := range b.DeleteRanges {
		dr, err := d.applyDeleteRange(batch, notifications, delRangeReq, updateOperationCallback)
		if err != nil {
			return nil, nil, err
		}

		res.DeleteRanges = append(res.DeleteRanges, dr)
	}

	return notifications, res, nil
}

func (d *db) ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error) {
	timer := d.batchWriteLatencyHisto.Timer()
	defer timer.Done()

	batch := d.kv.NewWriteBatch()
	notifications, res, err := d.applyWriteRequest(b, batch, commitOffset, timestamp, updateOperationCallback)
	if err != nil {
		return nil, err
	}

	if err := d.addCommitOffset(commitOffset, batch, timestamp); err != nil {
		return nil, err
	}

	// Add the notifications to the batch as well
	if err := d.addNotifications(batch, notifications); err != nil {
		return nil, err
	}

	if err := batch.Commit(); err != nil {
		return nil, err
	}

	d.notificationsTracker.UpdatedCommitOffset(commitOffset)

	if err := batch.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func (*db) addNotifications(batch WriteBatch, notifications *notifications) error {
	value, err := notifications.batch.MarshalVT()
	if err != nil {
		return err
	}

	return batch.Put(notificationKey(notifications.batch.Offset), value)
}

func (d *db) addCommitOffset(commitOffset int64, batch WriteBatch, timestamp uint64) error {
	commitOffsetValue := []byte(fmt.Sprintf("%d", commitOffset))
	_, err := d.applyPut(commitOffset, batch, nil, &proto.PutRequest{
		Key:               commitOffsetKey,
		Value:             commitOffsetValue,
		ExpectedVersionId: nil,
	}, timestamp, NoOpCallback)
	return err
}

func (d *db) Get(request *proto.GetRequest) (*proto.GetResponse, error) {
	timer := d.getLatencyHisto.Timer()
	defer timer.Done()

	d.getCounter.Add(1)
	return applyGet(d.kv, request)
}

type listIterator struct {
	KeyIterator
	timer metrics.Timer
}

func (it *listIterator) Close() error {
	it.timer.Done()
	return it.KeyIterator.Close()
}

func (d *db) List(request *proto.ListRequest) (KeyIterator, error) {
	d.listCounter.Add(1)

	it, err := d.kv.KeyRangeScan(request.StartInclusive, request.EndExclusive)
	if err != nil {
		return nil, err
	}

	return &listIterator{
		KeyIterator: it,
		timer:       d.listLatencyHisto.Timer(),
	}, nil
}

func (d *db) ReadCommitOffset() (int64, error) {
	kv := d.kv

	getReq := &proto.GetRequest{
		Key:          commitOffsetKey,
		IncludeValue: true,
	}
	gr, err := applyGet(kv, getReq)
	if err != nil {
		return wal.InvalidOffset, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return wal.InvalidOffset, nil
	}

	var commitOffset int64
	if _, err = fmt.Sscanf(string(gr.Value), "%d", &commitOffset); err != nil {
		return wal.InvalidOffset, err
	}
	return commitOffset, nil
}

func (d *db) UpdateTerm(newTerm int64) error {
	batch := d.kv.NewWriteBatch()

	if _, err := d.applyPut(wal.InvalidOffset, batch, nil, &proto.PutRequest{
		Key:   termKey,
		Value: []byte(fmt.Sprintf("%d", newTerm)),
	}, now(), NoOpCallback); err != nil {
		return err
	}

	if err := batch.Commit(); err != nil {
		return err
	}

	if err := batch.Close(); err != nil {
		return err
	}

	// Since the term change is not stored in the WAL, we must force
	// the database to flush, in order to ensure the term change is durable
	return d.kv.Flush()
}

func (d *db) ReadTerm() (term int64, err error) {
	getReq := &proto.GetRequest{
		Key:          termKey,
		IncludeValue: true,
	}
	gr, err := applyGet(d.kv, getReq)
	if err != nil {
		return wal.InvalidTerm, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return wal.InvalidTerm, nil
	}

	if _, err = fmt.Sscanf(string(gr.Value), "%d", &term); err != nil {
		return wal.InvalidTerm, err
	}
	return term, nil
}

func (d *db) applyPut(commitOffset int64, batch WriteBatch, notifications *notifications, putReq *proto.PutRequest, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.PutResponse, error) {
	se, err := checkExpectedVersionId(batch, putReq.Key, putReq.ExpectedVersionId)

	switch {
	case errors.Is(err, ErrBadVersionId):
		return &proto.PutResponse{
			Status: proto.Status_UNEXPECTED_VERSION_ID,
		}, nil
	case err != nil:
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	// No version conflict
	status, err := updateOperationCallback.OnPut(batch, putReq, se)
	if err != nil {
		return nil, err
	}
	if status != proto.Status_OK {
		return &proto.PutResponse{
			Status: status,
		}, nil
	}

	if se == nil {
		se = proto.StorageEntryFromVTPool()
		se.VersionId = commitOffset
		se.ModificationsCount = 0
		se.Value = putReq.Value
		se.CreationTimestamp = timestamp
		se.ModificationTimestamp = timestamp
		se.SessionId = putReq.SessionId
		se.ClientIdentity = putReq.ClientIdentity
	} else {
		se.VersionId = commitOffset
		se.ModificationsCount++
		se.Value = putReq.Value
		se.ModificationTimestamp = timestamp
		se.SessionId = putReq.SessionId
		se.ClientIdentity = putReq.ClientIdentity
	}

	defer se.ReturnToVTPool()

	ser, err := se.MarshalVT()
	if err != nil {
		return nil, err
	}

	if err = batch.Put(putReq.Key, ser); err != nil {
		return nil, err
	}

	if notifications != nil {
		notifications.Modified(putReq.Key, se.VersionId, se.ModificationsCount)
	}

	version := &proto.Version{
		VersionId:          se.VersionId,
		ModificationsCount: se.ModificationsCount,
		CreatedTimestamp:   se.CreationTimestamp,
		ModifiedTimestamp:  se.ModificationTimestamp,
		SessionId:          se.SessionId,
		ClientIdentity:     se.ClientIdentity,
	}

	d.log.Debug(
		"Applied put operation",
		slog.String("key", putReq.Key),
		slog.Any("version", version),
	)

	return &proto.PutResponse{Version: version}, nil
}

func (d *db) applyDelete(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteResponse, error) {
	se, err := checkExpectedVersionId(batch, delReq.Key, delReq.ExpectedVersionId)
	if se != nil {
		defer se.ReturnToVTPool()
	}

	switch {
	case errors.Is(err, ErrBadVersionId):
		return &proto.DeleteResponse{Status: proto.Status_UNEXPECTED_VERSION_ID}, nil
	case err != nil:
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	case se == nil:
		return &proto.DeleteResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	default:
		err = updateOperationCallback.OnDelete(batch, delReq.Key)
		if err != nil {
			return nil, err
		}

		if err = batch.Delete(delReq.Key); err != nil {
			return &proto.DeleteResponse{}, err
		}

		if notifications != nil {
			notifications.Deleted(delReq.Key)
		}

		d.log.Debug(
			"Applied delete operation",
			slog.String("key", delReq.Key),
		)
		return &proto.DeleteResponse{Status: proto.Status_OK}, nil
	}
}

func (*db) applyDeleteRangeNotifications(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) error {
	if notifications == nil && updateOperationCallback == NoOpCallback {
		return nil
	}

	it, err := batch.KeyRangeScan(delReq.StartInclusive, delReq.EndExclusive)
	if err != nil {
		return err
	}

	for it.Next() {
		if notifications != nil {
			notifications.Deleted(it.Key())
		}
		err := updateOperationCallback.OnDelete(batch, it.Key())
		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
	}

	if err := it.Close(); err != nil {
		return errors.Wrap(err, "oxia db: failed to delete range")
	}

	return nil
}

func (d *db) applyDeleteRange(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteRangeResponse, error) {
	if err := d.applyDeleteRangeNotifications(batch, notifications, delReq, updateOperationCallback); err != nil {
		return nil, err
	}

	if err := batch.DeleteRange(delReq.StartInclusive, delReq.EndExclusive); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to delete range")
	}

	d.log.Debug(
		"Applied delete range operation",
		slog.String("key-start", delReq.StartInclusive),
		slog.String("key-end", delReq.EndExclusive),
	)
	return &proto.DeleteRangeResponse{Status: proto.Status_OK}, nil
}

func applyGet(kv KV, getReq *proto.GetRequest) (*proto.GetResponse, error) {
	_, value, closer, err := kv.Get(getReq.Key, ComparisonEqual)

	if errors.Is(err, ErrKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	var se *proto.StorageEntry
	if getReq.IncludeValue {
		// If we need to return the value we cannot pool the objects, because
		// the Value slice would be returned to pool
		se = &proto.StorageEntry{}
	} else {
		se = proto.StorageEntryFromVTPool()
		defer se.ReturnToVTPool()
	}

	if err = multierr.Append(
		deserialize(value, se),
		closer.Close(),
	); err != nil {
		return nil, err
	}

	resValue := se.Value
	if !getReq.IncludeValue {
		resValue = nil
	}

	return &proto.GetResponse{
		Value: resValue,
		Version: &proto.Version{
			VersionId:          se.VersionId,
			ModificationsCount: se.ModificationsCount,
			CreatedTimestamp:   se.CreationTimestamp,
			ModifiedTimestamp:  se.ModificationTimestamp,
			SessionId:          se.SessionId,
			ClientIdentity:     se.ClientIdentity,
		},
	}, nil
}

func GetStorageEntry(batch WriteBatch, key string) (*proto.StorageEntry, error) {
	value, closer, err := batch.Get(key)
	if err != nil {
		return nil, err
	}

	se := proto.StorageEntryFromVTPool()

	if err = multierr.Append(
		deserialize(value, se),
		closer.Close(),
	); err != nil {
		return nil, err
	}
	return se, nil
}

func checkExpectedVersionId(batch WriteBatch, key string, expectedVersionId *int64) (*proto.StorageEntry, error) {
	se, err := GetStorageEntry(batch, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			if expectedVersionId == nil || *expectedVersionId == -1 {
				// OK, we were checking that the key was not there, and it's indeed not there
				return nil, nil //nolint:nilnil
			}

			return nil, ErrBadVersionId
		}
		return nil, err
	}

	if expectedVersionId != nil && se.VersionId != *expectedVersionId {
		se.ReturnToVTPool()
		return nil, ErrBadVersionId
	}

	return se, nil
}

func deserialize(value []byte, se *proto.StorageEntry) error {
	if err := se.UnmarshalVT(value); err != nil {
		return errors.Wrap(err, "failed to deserialize storage entry")
	}

	return nil
}

func (d *db) ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error) {
	return d.notificationsTracker.ReadNextNotifications(ctx, startOffset)
}

type noopCallback struct{}

func (*noopCallback) OnPut(WriteBatch, *proto.PutRequest, *proto.StorageEntry) (proto.Status, error) {
	return proto.Status_OK, nil
}

func (*noopCallback) OnDelete(WriteBatch, string) error {
	return nil
}

var NoOpCallback UpdateOperationCallback = &noopCallback{}
