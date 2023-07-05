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
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"io"
	"os"
	"oxia/common"
	"path/filepath"
	"sync"
	"time"
)

const (
	txnExtension = ".txn"
	idxExtension = ".idx"
)

func segmentPath(basePath string, firstOffset int64) string {
	return filepath.Join(basePath, fmt.Sprintf("%d", firstOffset))
}

func readInt(b []byte, offset uint32) uint32 {
	return binary.BigEndian.Uint32(b[offset : offset+4])
}

func fileOffset(idx []byte, firstOffset, offset int64) uint32 {
	return readInt(idx, uint32((offset-firstOffset)*4))
}

type ReadOnlySegment interface {
	io.Closer

	BaseOffset() int64
	LastOffset() int64

	Read(offset int64) ([]byte, error)

	Delete() error

	OpenTimestamp() time.Time
}

type readonlySegment struct {
	txnPath    string
	idxPath    string
	baseOffset int64
	lastOffset int64
	closed     bool

	txnFile       *os.File
	txnMappedFile mmap.MMap

	// Index file maps a logical "offset" to a physical file offset within the wal segment
	idxFile       *os.File
	idxMappedFile mmap.MMap
	openTimestamp time.Time
}

func newReadOnlySegment(basePath string, baseOffset int64) (ReadOnlySegment, error) {
	ms := &readonlySegment{
		txnPath:       segmentPath(basePath, baseOffset) + txnExtension,
		idxPath:       segmentPath(basePath, baseOffset) + idxExtension,
		baseOffset:    baseOffset,
		openTimestamp: time.Now(),
	}

	var err error
	if ms.txnFile, err = os.OpenFile(ms.txnPath, os.O_RDONLY, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment txn file %s", ms.txnPath)
	}

	if ms.txnMappedFile, err = mmap.MapRegion(ms.txnFile, -1, mmap.RDONLY, 0, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to map segment txn file %s", ms.txnPath)
	}

	if ms.idxFile, err = os.OpenFile(ms.idxPath, os.O_RDONLY, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment index file %s", ms.idxPath)
	}

	if ms.idxMappedFile, err = mmap.MapRegion(ms.idxFile, -1, mmap.RDONLY, 0, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to map segment index file %s", ms.idxPath)
	}

	ms.lastOffset = ms.baseOffset + int64(len(ms.idxMappedFile)/4-1)
	return ms, nil
}

func (ms *readonlySegment) BaseOffset() int64 {
	return ms.baseOffset
}

func (ms *readonlySegment) LastOffset() int64 {
	return ms.lastOffset
}

func (ms *readonlySegment) Read(offset int64) ([]byte, error) {
	if offset < ms.baseOffset || offset > ms.lastOffset {
		return nil, ErrorOffsetOutOfBounds
	}

	fileOffset := fileOffset(ms.idxMappedFile, ms.baseOffset, offset)
	entryLen := readInt(ms.txnMappedFile, fileOffset)
	entry := make([]byte, entryLen)
	copy(entry, ms.txnMappedFile[fileOffset+4:fileOffset+4+entryLen])

	return entry, nil
}

func (ms *readonlySegment) Close() error {
	if ms.closed {
		return nil
	}

	ms.closed = true
	return multierr.Combine(
		ms.txnMappedFile.Unmap(),
		ms.txnFile.Close(),
		ms.idxMappedFile.Unmap(),
		ms.idxFile.Close(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const (
	maxReadOnlySegmentsInCacheCount = 5
	maxReadOnlySegmentsInCacheTime  = 5 * time.Minute
)

type ReadOnlySegmentsGroup interface {
	io.Closer

	Get(offset int64) (common.RefCount[ReadOnlySegment], error)

	TrimSegments(offset int64) error

	AddedNewSegment(baseOffset int64)

	PollHighestSegment() (common.RefCount[ReadOnlySegment], error)
}

type readOnlySegmentsGroup struct {
	sync.Mutex

	basePath     string
	allSegments  *treemap.Map
	openSegments *treemap.Map
}

func newReadOnlySegmentsGroup(basePath string) (ReadOnlySegmentsGroup, error) {
	g := &readOnlySegmentsGroup{
		basePath:     basePath,
		allSegments:  treemap.NewWith(utils.Int64Comparator),
		openSegments: treemap.NewWith(utils.Int64Comparator),
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
	r.openSegments.Each(func(id any, segment any) {
		err = multierr.Append(err, segment.(io.Closer).Close())
	})

	r.openSegments.Clear()
	r.allSegments.Clear()
	return err
}

func (r *readOnlySegmentsGroup) Get(offset int64) (common.RefCount[ReadOnlySegment], error) {
	r.Lock()
	defer r.Unlock()

	_, segment := r.openSegments.Floor(offset)
	if segment != nil && offset <= segment.(common.RefCount[ReadOnlySegment]).Get().LastOffset() {
		return segment.(common.RefCount[ReadOnlySegment]).Acquire(), nil
	}

	// Check if we have a segment file on disk
	baseOffset, _ := r.allSegments.Floor(offset)
	if baseOffset != nil {
		segment, err := newReadOnlySegment(r.basePath, baseOffset.(int64))
		if err != nil {
			return nil, err
		}

		rc := common.NewRefCount(segment)
		res := rc.Acquire()

		r.openSegments.Put(segment.BaseOffset(), rc)
		if err := r.cleanSegmentsCache(); err != nil {
			return nil, err
		}
		return res, nil
	}

	return nil, ErrorOffsetOutOfBounds
}

func (r *readOnlySegmentsGroup) TrimSegments(offset int64) error {
	r.Lock()
	defer r.Unlock()

	// Find the segment that ends before the trim offset
	segmentToKeep, _ := r.allSegments.Floor(offset)
	if segmentToKeep == nil {
		segmentToKeep = offset
	}

	cutoffSegment, _ := r.allSegments.Floor(segmentToKeep.(int64) - 1)
	if cutoffSegment == nil {
		return nil
	}

	var err error
	for _, s := range r.allSegments.Keys() {
		if s.(int64) > cutoffSegment.(int64) {
			break
		}

		r.allSegments.Remove(s)
		if segment, ok := r.openSegments.Get(s); ok {
			err = multierr.Append(err, segment.(common.RefCount[ReadOnlySegment]).Get().Delete())
			r.openSegments.Remove(s)
		} else {
			if segment, err2 := newReadOnlySegment(r.basePath, s.(int64)); err != nil {
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
		return nil, nil
	}

	offset, _ := r.allSegments.Max()
	r.allSegments.Remove(offset)
	segment, found := r.openSegments.Get(offset)
	if found {
		return segment.(common.RefCount[ReadOnlySegment]).Acquire(), nil
	}

	roSegment, err := newReadOnlySegment(r.basePath, offset.(int64))
	if err != nil {
		return nil, err
	}

	return common.NewRefCount(roSegment), err
}

func (r *readOnlySegmentsGroup) cleanSegmentsCache() error {
	var err error

	// Delete based on open-timestamp
	it := r.openSegments.Iterator()
	for it.Next() {
		ts := it.Value().(common.RefCount[ReadOnlySegment]).Get().OpenTimestamp()
		if time.Now().Sub(ts) > maxReadOnlySegmentsInCacheTime {
			err = multierr.Append(err, it.Value().(common.RefCount[ReadOnlySegment]).Close())
			r.openSegments.Remove(it.Key())
		}
	}

	// Delete based on max-count
	it = r.openSegments.Iterator()
	for it.Next() && r.openSegments.Size() > maxReadOnlySegmentsInCacheCount {
		err = multierr.Append(err, it.Value().(common.RefCount[ReadOnlySegment]).Close())
		r.openSegments.Remove(it.Key())
	}

	return err
}
