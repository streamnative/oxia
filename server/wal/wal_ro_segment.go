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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/streamnative/oxia/server/wal/codec"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
)

func segmentPath(basePath string, firstOffset int64) string {
	return filepath.Join(basePath, fmt.Sprintf("%d", firstOffset))
}

func fileOffset(idx []byte, firstOffset, offset int64) uint32 {
	return codec.ReadInt(idx, uint32((offset-firstOffset)*4))
}

type ReadOnlySegment interface {
	io.Closer

	BaseOffset() int64
	LastOffset() int64

	LastCrc() uint32

	Read(offset int64) ([]byte, error)

	Delete() error

	OpenTimestamp() time.Time
}

type readonlySegment struct {
	codec      codec.Codec
	txnPath    string
	idxPath    string
	baseOffset int64
	lastOffset int64
	lastCrc    uint32
	closed     bool

	txnFile       *os.File
	txnMappedFile mmap.MMap

	// Index file maps a logical "offset" to a physical file offset within the wal segment
	idx           []byte
	openTimestamp time.Time
}

func newReadOnlySegment(basePath string, baseOffset int64) (ReadOnlySegment, error) {
	_codec, _, err := codec.GetOrCreate(segmentPath(basePath, baseOffset))
	if err != nil {
		return nil, err
	}

	ms := &readonlySegment{
		codec:         _codec,
		txnPath:       segmentPath(basePath, baseOffset) + _codec.GetTxnExtension(),
		idxPath:       segmentPath(basePath, baseOffset) + _codec.GetIdxExtension(),
		baseOffset:    baseOffset,
		openTimestamp: time.Now(),
	}

	if ms.txnFile, err = os.OpenFile(ms.txnPath, os.O_RDONLY, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment txn file %s", ms.txnPath)
	}

	if ms.txnMappedFile, err = mmap.MapRegion(ms.txnFile, -1, mmap.RDONLY, 0, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to map segment txn file %s", ms.txnPath)
	}

	if ms.idx, err = ms.codec.ReadIndex(ms.idxPath); err != nil {
		if !errors.Is(err, codec.ErrDataCorrupted) {
			return nil, errors.Wrapf(err, "failed to decode segment index file %s", ms.idxPath)
		}
		slog.Warn("The segment index file is corrupted and the index is being rebuilt.", slog.String("path", ms.idxPath))
		// recover from txn
		if ms.idx, _, _, _, err = ms.codec.RecoverIndex(ms.txnMappedFile, 0,
			ms.baseOffset, nil); err != nil {
			slog.Error("The segment index file rebuild failed.", slog.String("path", ms.idxPath))
			return nil, errors.Wrapf(err, "failed to rebuild segment index file %s", ms.idxPath)
		}
		slog.Info("The segment index file has been rebuilt.", slog.String("path", ms.idxPath))
		if err := ms.codec.WriteIndex(ms.idxPath, ms.idx); err != nil {
			slog.Warn("write recovered segment index failed. it can continue work but will retry writing after restart.",
				slog.String("path", ms.idxPath))
		}
	}

	ms.lastOffset = ms.baseOffset + int64(len(ms.idx)/4-1)

	// recover the last crc
	fo := fileOffset(ms.idx, ms.baseOffset, ms.lastOffset)
	if _, _, ms.lastCrc, err = ms.codec.ReadHeaderWithValidation(ms.txnMappedFile, fo); err != nil {
		return nil, err
	}
	return ms, nil
}

func (ms *readonlySegment) LastCrc() uint32 {
	return ms.lastCrc
}

func (ms *readonlySegment) BaseOffset() int64 {
	return ms.baseOffset
}

func (ms *readonlySegment) LastOffset() int64 {
	return ms.lastOffset
}

func (ms *readonlySegment) Read(offset int64) ([]byte, error) {
	if offset < ms.baseOffset || offset > ms.lastOffset {
		return nil, codec.ErrOffsetOutOfBounds
	}
	fileReadOffset := fileOffset(ms.idx, ms.baseOffset, offset)
	var payload []byte
	var err error
	if payload, err = ms.codec.ReadRecordWithValidation(ms.txnMappedFile, fileReadOffset); err != nil {
		if errors.Is(err, codec.ErrDataCorrupted) {
			return nil, errors.Wrapf(err, "read record failed. entryOffset: %d", offset)
		}
		return nil, err
	}
	return payload, nil
}

func (ms *readonlySegment) Close() error {
	if ms.closed {
		return nil
	}

	ms.closed = true
	return multierr.Combine(
		ms.txnMappedFile.Unmap(),
		ms.txnFile.Close(),
	)
}

func (ms *readonlySegment) Delete() error {
	return multierr.Combine(
		ms.Close(),
		os.Remove(ms.txnPath),
		os.Remove(ms.idxPath),
	)
}

func (ms *readonlySegment) OpenTimestamp() time.Time {
	return ms.openTimestamp
}

const (
	maxReadOnlySegmentsInCacheCount = 5
	maxReadOnlySegmentsInCacheTime  = 5 * time.Minute
)

type ReadOnlySegmentsGroup interface {
	io.Closer

	Get(offset int64) (common.RefCount[ReadOnlySegment], error)

	TrimSegments(offset int64) error

	GetLastCrc(baseOffset int64) (uint32, error)

	AddedNewSegment(baseOffset int64)

	PollHighestSegment() (common.RefCount[ReadOnlySegment], error)
}

type readOnlySegmentsGroup struct {
	sync.Mutex

	basePath     string
	allSegments  *treeMap[int64, bool]
	openSegments *treeMap[int64, common.RefCount[ReadOnlySegment]]
}

func newReadOnlySegmentsGroup(basePath string) (ReadOnlySegmentsGroup, error) {
	g := &readOnlySegmentsGroup{
		basePath:     basePath,
		allSegments:  newInt64TreeMap[bool](),
		openSegments: newInt64TreeMap[common.RefCount[ReadOnlySegment]](),
	}

	segments, err := listAllSegments(basePath)
	if err != nil {
		return nil, err
	}

	if len(segments) > 0 {
		// Ignore last segment because is the "current" one and it's read-write
		for _, segment := range segments[:len(segments)-1] {
			g.allSegments.Put(segment, true)
		}
	}

	return g, nil
}

func (r *readOnlySegmentsGroup) AddedNewSegment(baseOffset int64) {
	r.Lock()
	defer r.Unlock()

	r.allSegments.Put(baseOffset, true)
}

func (r *readOnlySegmentsGroup) Close() error {
	r.Lock()
	defer r.Unlock()

	var err error
	r.openSegments.Each(func(_ int64, segment common.RefCount[ReadOnlySegment]) bool {
		err = multierr.Append(err, segment.(io.Closer).Close())
		return true
	})

	r.openSegments.Clear()
	r.allSegments.Clear()
	return err
}

func (r *readOnlySegmentsGroup) GetLastCrc(baseOffset int64) (uint32, error) {
	roSegment, err := r.Get(baseOffset)
	if err != nil {
		return 0, err
	}
	lastCrc := roSegment.Get().LastCrc()
	if err := roSegment.Close(); err != nil {
		return 0, err
	}
	return lastCrc, nil
}

func (r *readOnlySegmentsGroup) Get(offset int64) (common.RefCount[ReadOnlySegment], error) {
	r.Lock()
	defer r.Unlock()

	_, segment := r.openSegments.Floor(offset)
	if segment != nil && offset <= segment.Get().LastOffset() {
		return segment.Acquire(), nil
	}

	// Check if we have a segment file on disk
	baseOffset, found := r.allSegments.Floor(offset)
	if !found {
		return nil, codec.ErrOffsetOutOfBounds
	}

	rosegment, err := newReadOnlySegment(r.basePath, baseOffset)
	if err != nil {
		return nil, err
	}

	rc := common.NewRefCount(rosegment)
	res := rc.Acquire()

	r.openSegments.Put(rosegment.BaseOffset(), rc)
	if err := r.cleanSegmentsCache(); err != nil {
		return nil, err
	}
	return res, nil
}

func (r *readOnlySegmentsGroup) TrimSegments(offset int64) error {
	r.Lock()
	defer r.Unlock()

	// Find the segment that ends before the trim offset
	segmentToKeep, found := r.allSegments.Floor(offset)
	if !found {
		segmentToKeep = offset
	}

	cutoffSegment, found := r.allSegments.Floor(segmentToKeep - 1)
	if !found {
		return nil
	}

	var err error
	for _, s := range r.allSegments.Keys() {
		if s > cutoffSegment {
			break
		}

		r.allSegments.Remove(s)
		if segment, ok := r.openSegments.Get(s); ok {
			err = multierr.Append(err, segment.Get().Delete())
			r.openSegments.Remove(s)
		} else {
			if segment, err2 := newReadOnlySegment(r.basePath, s); err != nil {
				err = multierr.Append(err, err2)
			} else {
				err = multierr.Append(err, segment.Delete())
			}
		}
	}

	return err
}

func (r *readOnlySegmentsGroup) PollHighestSegment() (common.RefCount[ReadOnlySegment], error) {
	r.Lock()
	defer r.Unlock()

	if r.allSegments.Empty() {
		return nil, nil // nolint: nilnil
	}

	offset, _ := r.allSegments.Max()
	r.allSegments.Remove(offset)
	segment, found := r.openSegments.Get(offset)
	if found {
		return segment.Acquire(), nil
	}

	roSegment, err := newReadOnlySegment(r.basePath, offset)
	if err != nil {
		return nil, err
	}

	return common.NewRefCount(roSegment), err
}

func (r *readOnlySegmentsGroup) cleanSegmentsCache() error {
	var err error

	// Delete based on open-timestamp
	r.openSegments.Each(func(k int64, v common.RefCount[ReadOnlySegment]) bool {
		ts := v.Get().OpenTimestamp()
		if time.Since(ts) > maxReadOnlySegmentsInCacheTime {
			err = multierr.Append(err, v.Close())
			r.openSegments.Remove(k)
		}

		return true
	})

	// Delete based on max-count
	r.openSegments.Each(func(k int64, v common.RefCount[ReadOnlySegment]) bool {
		if r.openSegments.Size() > maxReadOnlySegmentsInCacheCount {
			err = multierr.Append(err, v.Close())
			r.openSegments.Remove(k)
			return true
		}

		return false
	})

	return err
}
