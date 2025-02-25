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

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/wal/codec"
)

func TestReadOnlySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	assert.NoError(t, rw.Close())

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
}

func TestRO_auto_recover_broken_index(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	idxPath := rwSegment.idxPath
	// inject wrong data
	file, err := os.OpenFile(idxPath, os.O_RDWR, 0644)
	assert.NoError(t, err)
	defer file.Close()
	faultData, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)
	_, err = file.WriteAt(faultData, 0)
	assert.NoError(t, err)

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
}

func TestReadOnlySegmentsGroupTrimSegments(t *testing.T) {
	basePath := t.TempDir()
	t.Run("when newReadOnlySegment failed", func(t *testing.T) {
		walFactory := NewWalFactory(&FactoryOptions{
			BaseWalDir:  basePath,
			Retention:   1 * time.Hour,
			SegmentSize: 128,
			SyncData:    true,
		})
		w, err := walFactory.NewWal("test", 0, nil)
		assert.NoError(t, err)
		for i := int64(0); i < 1000; i++ {
			assert.NoError(t, w.Append(&proto.LogEntry{
				Term:   1,
				Offset: i,
				Value:  fmt.Appendf(nil, "test-%d", i),
			}))
		}
		walBasePath := w.(*wal).walPath
		readOnlySegments, err := newReadOnlySegmentsGroup(walBasePath)
		assert.NoError(t, err)

		// Ensure newReadOnlySegment will return an error
		err = os.Truncate(filepath.Join(walBasePath, "0.txnx"), 0)
		assert.NoError(t, err)

		assert.NoError(t, err)
		err = readOnlySegments.TrimSegments(6)
		assert.Error(t, err)
	})

	t.Run("when txnFile is not exists", func(t *testing.T) {
		walFactory := NewWalFactory(&FactoryOptions{
			BaseWalDir:  basePath,
			Retention:   1 * time.Hour,
			SegmentSize: 128,
			SyncData:    true,
		})
		w, err := walFactory.NewWal("test", 1, nil)
		assert.NoError(t, err)
		for i := int64(0); i < 1000; i++ {
			assert.NoError(t, w.Append(&proto.LogEntry{
				Term:   1,
				Offset: i,
				Value:  fmt.Appendf(nil, "test-%d", i),
			}))
		}
		walBasePath := w.(*wal).walPath
		readOnlySegments, err := newReadOnlySegmentsGroup(walBasePath)
		assert.NoError(t, err)

		// Ensure newReadOnlySegment will return an NotExists error
		err = os.Remove(filepath.Join(walBasePath, "0.txnx"))
		assert.NoError(t, err)

		assert.NoError(t, err)
		err = readOnlySegments.TrimSegments(6)
		assert.NoError(t, err)
	})
}
