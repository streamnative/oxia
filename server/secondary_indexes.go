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

package server

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
)

const secondaryIdxKeyPrefix = common.InternalKeyPrefix + "idx"

type wrapperUpdateCallback struct{}

func (wrapperUpdateCallback) OnPut(batch kv.WriteBatch, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error) {
	// First update the session
	status, err := sessionManagerUpdateOperationCallback.OnPut(batch, req, se)
	if err != nil || status != proto.Status_OK {
		return status, err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnPut(batch, req, se)
}

func (wrapperUpdateCallback) OnDelete(batch kv.WriteBatch, key string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDelete(batch, key); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDelete(batch, key)
}

func (wrapperUpdateCallback) OnDeleteRange(batch kv.WriteBatch, keyStartInclusive string, keyEndExclusive string) error {
	// First update the session
	if err := sessionManagerUpdateOperationCallback.OnDeleteRange(batch, keyStartInclusive, keyEndExclusive); err != nil {
		return err
	}

	// Check secondary indexes
	return secondaryIndexesUpdateCallback.OnDeleteRange(batch, keyStartInclusive, keyEndExclusive)
}

var WrapperUpdateOperationCallback kv.UpdateOperationCallback = &wrapperUpdateCallback{}

type secondaryIndexesUpdateCallbackS struct{}

var secondaryIndexesUpdateCallback kv.UpdateOperationCallback = &secondaryIndexesUpdateCallbackS{}

func (secondaryIndexesUpdateCallbackS) OnPut(batch kv.WriteBatch, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	if existingEntry != nil {
		// TODO: We might want to check if there are indexes that did not change
		// between the existing and the new record.
		if err := deleteSecondaryIndexes(batch, request.Key, existingEntry); err != nil {
			return proto.Status_KEY_NOT_FOUND, err
		}
	}

	return proto.Status_OK, writeSecondaryIndexes(batch, request.Key, request.SecondaryIndexes)
}

func (secondaryIndexesUpdateCallbackS) OnDelete(batch kv.WriteBatch, key string) error {
	se, err := kv.GetStorageEntry(batch, key)
	defer se.ReturnToVTPool()

	if errors.Is(err, kv.ErrKeyNotFound) {
		return nil
	}
	if err == nil {
		err = deleteSecondaryIndexes(batch, key, se)
	}
	return err
}

func (secondaryIndexesUpdateCallbackS) OnDeleteRange(batch kv.WriteBatch, keyStartInclusive string, keyEndExclusive string) error {
	it, err := batch.RangeScan(keyStartInclusive, keyEndExclusive)
	if err != nil {
		return err
	}

	for ; it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
		se := proto.StorageEntryFromVTPool()

		err = kv.Deserialize(value, se)
		if err == nil {
			err = deleteSecondaryIndexes(batch, it.Key(), se)
		}

		se.ReturnToVTPool()

		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
	}

	if err := it.Close(); err != nil {
		return errors.Wrap(err, "oxia db: failed to delete range")
	}

	return err
}

const secondaryIdxSeparator = "\x01"
const secondaryIdxRangePrefixFormat = secondaryIdxKeyPrefix + "/%s/%s"
const secondaryIdxFormat = secondaryIdxRangePrefixFormat + secondaryIdxSeparator + "%s"

const regex = "^" + secondaryIdxKeyPrefix + "/[^/]+/[^" + secondaryIdxSeparator + "]+" + secondaryIdxSeparator + "(.+)$"

var secondaryIdxFormatRegex = regexp.MustCompile(regex)

func secondaryIndexKey(primaryKey, indexName, secondaryKey string) string {
	return fmt.Sprintf(secondaryIdxFormat, indexName, secondaryKey, url.PathEscape(primaryKey))
}

func secondaryIndexPrimaryKey(completeKey string) (string, error) {
	matches := secondaryIdxFormatRegex.FindStringSubmatch(completeKey)
	if len(matches) != 2 {
		return "", errors.Errorf("oxia db: failed to parse secondary index key")
	}

	return url.PathUnescape(matches[1])
}

func deleteSecondaryIndexes(batch kv.WriteBatch, primaryKey string, existingEntry *proto.StorageEntry) error {
	if len(existingEntry.SecondaryIndexes) > 0 {
		for idxName, secondaryKey := range existingEntry.SecondaryIndexes {
			if err := batch.Delete(secondaryIndexKey(primaryKey, idxName, secondaryKey)); err != nil {
				return err
			}
		}
	}
	return nil
}

var emptyValue []byte

func writeSecondaryIndexes(batch kv.WriteBatch, primaryKey string, secondaryIndexes map[string]string) error {
	if len(secondaryIndexes) > 0 {
		for idxName, secondaryKey := range secondaryIndexes {
			if err := batch.Put(secondaryIndexKey(primaryKey, idxName, secondaryKey), emptyValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexListIterator(req *proto.ListRequest, db kv.DB) (kv.KeyIterator, error) {
	indexName := *req.SecondaryIndexName
	it, err := db.List(&proto.ListRequest{
		StartInclusive: fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.StartInclusive),
		EndExclusive:   fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.EndExclusive),
	})
	if err != nil {
		return nil, err
	}

	return &secondaryIndexListIterator{it: it}, nil
}

type secondaryIndexListIterator struct {
	it kv.KeyIterator
}

func (it *secondaryIndexListIterator) Valid() bool {
	return it.it.Valid()
}

func (it *secondaryIndexListIterator) Key() string {
	idxKey := it.it.Key()
	primaryKey, err := secondaryIndexPrimaryKey(idxKey)
	if err != nil {
		// This should never happen since we control the key format
		panic(errors.Wrap(err, "Failed to parse secondary index key"))
	}

	return primaryKey
}

func (it *secondaryIndexListIterator) Next() bool {
	return it.it.Next()
}

func (it *secondaryIndexListIterator) Close() error {
	return it.it.Close()
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func newSecondaryIndexRangeScanIterator(req *proto.RangeScanRequest, db kv.DB) (kv.RangeScanIterator, error) {
	indexName := *req.SecondaryIndexName
	it, err := db.List(&proto.ListRequest{
		StartInclusive: fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.StartInclusive),
		EndExclusive:   fmt.Sprintf(secondaryIdxRangePrefixFormat, indexName, req.EndExclusive),
	})
	if err != nil {
		return nil, err
	}

	return &secondaryIndexRangeIterator{listIt: &secondaryIndexListIterator{it},
		db: db}, nil
}

type secondaryIndexRangeIterator struct {
	listIt *secondaryIndexListIterator
	db     kv.DB
}

func (it *secondaryIndexRangeIterator) Close() error {
	return it.listIt.Close()
}

func (it *secondaryIndexRangeIterator) Valid() bool {
	return it.listIt.Valid()
}

func (it *secondaryIndexRangeIterator) Key() string {
	return it.listIt.Key()
}

func (it *secondaryIndexRangeIterator) Next() bool {
	return it.listIt.Next()
}

func (it *secondaryIndexRangeIterator) Value() (*proto.GetResponse, error) {
	primaryKey := it.Key()
	gr, err := it.db.Get(&proto.GetRequest{
		Key:            primaryKey,
		IncludeValue:   true,
		ComparisonType: proto.KeyComparisonType_EQUAL,
	})

	if gr != nil {
		gr.Key = &primaryKey
	}

	return gr, err
}
