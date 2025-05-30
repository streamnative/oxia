// Copyright 2025 StreamNative, Inc.
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
	"io"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common/object"

	"github.com/streamnative/oxia/server/util"
	"github.com/streamnative/oxia/server/wal/codec"
)

type ReadOnlySegmentsGroup interface {
	io.Closer

	Get(offset int64) (object.RefCount[ReadOnlySegment], error)

	TrimSegments(offset int64) error

	GetLastCrc(baseOffset int64) (uint32, error)

	AddedNewSegment(baseOffset int64)

	PollHighestSegment() (object.RefCount[ReadOnlySegment], error)
}

type readOnlySegmentsGroup struct {
	sync.Mutex

	basePath     string
	allSegments  *treeMap[int64, bool]
	openSegments *treeMap[int64, object.RefCount[ReadOnlySegment]]
}

func newReadOnlySegmentsGroup(basePath string) (ReadOnlySegmentsGroup, error) {
	g := &readOnlySegmentsGroup{
		basePath:     basePath,
		allSegments:  newInt64TreeMap[bool](),
		openSegments: newInt64TreeMap[object.RefCount[ReadOnlySegment]](),
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
	r.openSegments.Each(func(_ int64, segment object.RefCount[ReadOnlySegment]) bool {
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

func (r *readOnlySegmentsGroup) Get(offset int64) (object.RefCount[ReadOnlySegment], error) {
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

	rc := object.NewRefCount(rosegment)
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
			continue
		}

		c, err2 := newSegmentConfig(r.basePath, s)
		if err2 != nil {
			err = multierr.Append(err, err2)
			continue
		}

		err2 = multierr.Combine(
			util.RemoveFileIfExists(c.idxPath),
			util.RemoveFileIfExists(c.txnPath),
		)
		if err2 != nil {
			err = multierr.Append(err, err2)
		}
	}

	return err
}

func (r *readOnlySegmentsGroup) PollHighestSegment() (object.RefCount[ReadOnlySegment], error) {
	r.Lock()
	defer r.Unlock()

	if r.allSegments.Empty() {
		return nil, nil //nolint:nilnil
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

	return object.NewRefCount(roSegment), err
}

func (r *readOnlySegmentsGroup) cleanSegmentsCache() error {
	var err error

	// Delete based on open-timestamp
	r.openSegments.Each(func(k int64, v object.RefCount[ReadOnlySegment]) bool {
		ts := v.Get().OpenTimestamp()
		if time.Since(ts) > maxReadOnlySegmentsInCacheTime {
			err = multierr.Append(err, v.Close())
			r.openSegments.Remove(k)
		}

		return true
	})

	// Delete based on max-count
	r.openSegments.Each(func(k int64, v object.RefCount[ReadOnlySegment]) bool {
		if r.openSegments.Size() > maxReadOnlySegmentsInCacheCount {
			err = multierr.Append(err, v.Close())
			r.openSegments.Remove(k)
			return true
		}

		return false
	})

	return err
}
