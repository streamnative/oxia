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
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type ReadWriteSegment interface {
	ReadOnlySegment

	Append(offset int64, data []byte) error

	Truncate(lastSafeOffset int64) error

	HasSpace(len int) bool

	Flush() error
}

type readWriteSegment struct {
	sync.RWMutex

	path          string
	baseOffset    int64
	lastOffset    int64
	txnFile       *os.File
	txnMappedFile mmap.MMap

	currentFileOffset uint32
	writingIdx        []byte

	segmentSize uint32
}

func newReadWriteSegment(basePath string, baseOffset int64, segmentSize uint32) (ReadWriteSegment, error) {
	var err error
	if _, err = os.Stat(basePath); os.IsNotExist(err) {
		if err = os.MkdirAll(basePath, 0755); err != nil {
			return nil, errors.Wrapf(err, "failed to create wal directory %s", basePath)
		}
	}

	ms := &readWriteSegment{
		path:        segmentPath(basePath, baseOffset),
		baseOffset:  baseOffset,
		segmentSize: segmentSize,
	}

	txnPath := ms.path + txnExtension

	var segmentExists bool

	if _, err = os.Stat(txnPath); os.IsNotExist(err) {
		// The segment file does not exist yet, create file and initialize it
		segmentExists = false
	} else {
		segmentExists = true
	}

	if ms.txnFile, err = os.OpenFile(txnPath, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment file %s", txnPath)
	}

	if !segmentExists {
		if err = initFileWithZeroes(ms.txnFile, segmentSize); err != nil {
			return nil, err
		}
	}

	if ms.txnMappedFile, err = mmap.MapRegion(ms.txnFile, int(segmentSize), mmap.RDWR, 0, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to map segment file %s", txnPath)
	}

	if err = ms.rebuildIdx(); err != nil {
		return nil, errors.Wrapf(err, "failed to rebuild index for segment file %s", txnPath)
	}

	return ms, nil
}

func (ms *readWriteSegment) BaseOffset() int64 {
	return ms.baseOffset
}

func (ms *readWriteSegment) LastOffset() int64 {
	ms.RLock()
	defer ms.RUnlock()

	return ms.lastOffset
}

func (ms *readWriteSegment) Read(offset int64) ([]byte, error) {
	ms.Lock()
	defer ms.Unlock()

	fileOffset := fileOffset(ms.writingIdx, ms.baseOffset, offset)
	entryLen := readInt(ms.txnMappedFile, fileOffset)
	entry := make([]byte, entryLen)
	copy(entry, ms.txnMappedFile[fileOffset+4:fileOffset+4+entryLen])

	return entry, nil
}

func (ms *readWriteSegment) HasSpace(len int) bool {
	return ms.currentFileOffset+4+uint32(len) <= ms.segmentSize
}

func (ms *readWriteSegment) Append(offset int64, data []byte) error {
	ms.Lock()
	defer ms.Unlock()

	if offset != ms.lastOffset+1 {
		return ErrorInvalidNextOffset
	}

	entryOffset := ms.currentFileOffset
	entrySize := uint32(len(data))
	binary.BigEndian.PutUint32(ms.txnMappedFile[ms.currentFileOffset:], entrySize)
	copy(ms.txnMappedFile[ms.currentFileOffset+4:], data)
	ms.currentFileOffset += 4 + entrySize
	ms.lastOffset = offset

	ms.writingIdx = binary.BigEndian.AppendUint32(ms.writingIdx, entryOffset)
	return nil
}

func (ms *readWriteSegment) Flush() error {
	return ms.txnMappedFile.Flush()
}

func (ms *readWriteSegment) rebuildIdx() error {
	// Scan the mapped file and rebuild the index

	entryOffset := ms.baseOffset

	for ms.currentFileOffset < ms.segmentSize {
		size := readInt(ms.txnMappedFile, ms.currentFileOffset)
		if size == 0 || size > (ms.segmentSize-ms.currentFileOffset) {
			break
		}

		ms.writingIdx = binary.BigEndian.AppendUint32(ms.writingIdx, ms.currentFileOffset)
		ms.currentFileOffset += 4 + size
		entryOffset += 1
	}

	ms.lastOffset = entryOffset - 1
	return nil
}

func (ms *readWriteSegment) OpenTimestamp() time.Time {
	return time.Now()
}

func (ms *readWriteSegment) Close() error {
	ms.Lock()
	defer ms.Unlock()

	return multierr.Combine(
		ms.txnMappedFile.Unmap(),
		ms.txnFile.Close(),

		// Write index file
		ms.writeIndex(),
	)
}

func (ms *readWriteSegment) Delete() error {
	return multierr.Combine(
		ms.Close(),
		os.Remove(ms.path+txnExtension),
		os.Remove(ms.path+idxExtension),
	)
}

func (ms *readWriteSegment) writeIndex() error {
	idxPath := ms.path + idxExtension

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open index file %s", idxPath)
	}

	if _, err = idxFile.Write(ms.writingIdx); err != nil {
		return errors.Wrapf(err, "failed write index file %s", idxPath)
	}

	return idxFile.Close()
}

func (ms *readWriteSegment) Truncate(lastSafeOffset int64) error {
	if lastSafeOffset < ms.baseOffset || lastSafeOffset > ms.lastOffset {
		return ErrorOffsetOutOfBounds
	}

	// Write zeroes in the section to clear
	fileLastSafeOffset := fileOffset(ms.writingIdx, ms.baseOffset, lastSafeOffset)
	entryLen := readInt(ms.txnMappedFile, fileLastSafeOffset)
	fileEndOffset := fileLastSafeOffset + 4 + entryLen
	for i := ms.currentFileOffset; i < fileEndOffset; i++ {
		ms.txnMappedFile[i] = 0
	}

	// Truncate the index
	ms.writingIdx = ms.writingIdx[:4*(lastSafeOffset-ms.baseOffset)]

	ms.currentFileOffset = fileEndOffset
	ms.lastOffset = lastSafeOffset
	return ms.Flush()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func initFileWithZeroes(f *os.File, size uint32) error {
	if _, err := f.Seek(int64(size), 0); err != nil {
		return err
	}

	if _, err := f.Write([]byte{0x00}); err != nil {
		return err
	}

	return f.Sync()
}
