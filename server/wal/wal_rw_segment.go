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
	"github.com/streamnative/oxia/server/util/crc"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

var bufferPool = sync.Pool{}

const initialIndexBufferCapacity = 16 * 1024

type ReadWriteSegment interface {
	ReadOnlySegment

	Append(offset int64, data []byte) error

	Truncate(lastSafeOffset int64) error

	HasSpace(l int) bool

	Flush() error
}

type readWriteSegment struct {
	sync.RWMutex
	formatVersion FormatVersion

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
		path:          segmentPath(basePath, baseOffset),
		baseOffset:    baseOffset,
		segmentSize:   segmentSize,
		formatVersion: TxnFormatVersion2,
	}

	if pooledBuffer, ok := bufferPool.Get().(*[]byte); ok {
		ms.writingIdx = (*pooledBuffer)[:0]
	} else {
		// Start with empty slice, though with some initial capacity
		ms.writingIdx = make([]byte, 0, initialIndexBufferCapacity)
	}

	txnPath := ms.path + txnExtension

	var segmentExists bool

	if _, err = os.Stat(txnPath); os.IsNotExist(err) {
		// The segment file does not exist yet, create file and initialize it
		segmentExists = false
		// use extension 2 by default
		txnPath = ms.path + txnExtension2
	} else {
		segmentExists = true
		ms.formatVersion = TxnFormatVersion1
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
	var entryCrc *uint32
	var headerOffset uint32 = 0
	if ms.formatVersion == TxnFormatVersion2 {
		v := readInt(ms.txnMappedFile, fileOffset)
		entryCrc = &v
		headerOffset += CrcLen
	}
	entryLen := readInt(ms.txnMappedFile, fileOffset+headerOffset)
	headerOffset += PayloadSizeLen
	entry := make([]byte, entryLen)
	copy(entry, ms.txnMappedFile[fileOffset+headerOffset:fileOffset+headerOffset+entryLen])
	if entryCrc != nil {
		if crc.New(entry).Value() != *entryCrc {
			// todo: introduce a new error
			return nil, errors.New("data corrupted")
		}
	}
	return entry, nil
}

func (ms *readWriteSegment) HasSpace(l int) bool {
	return ms.currentFileOffset+4+uint32(l) <= ms.segmentSize
}

func (ms *readWriteSegment) Append(offset int64, data []byte) error {
	ms.Lock()
	defer ms.Unlock()

	if offset != ms.lastOffset+1 {
		return ErrInvalidNextOffset
	}

	fileOffset := ms.currentFileOffset
	headerOffset := uint32(0)
	entrySize := uint32(len(data))
	if ms.formatVersion == TxnFormatVersion2 {
		checksum := crc.New(data).Value()
		binary.BigEndian.PutUint32(ms.txnMappedFile[ms.currentFileOffset:], checksum)
		headerOffset += CrcLen
	}

	binary.BigEndian.PutUint32(ms.txnMappedFile[ms.currentFileOffset+headerOffset:], entrySize)
	headerOffset += PayloadSizeLen

	copy(ms.txnMappedFile[ms.currentFileOffset+headerOffset:], data)
	ms.currentFileOffset += headerOffset + entrySize
	ms.lastOffset = offset

	ms.writingIdx = binary.BigEndian.AppendUint32(ms.writingIdx, fileOffset)
	return nil
}

func (ms *readWriteSegment) Flush() error {
	return ms.txnMappedFile.Flush()
}

//nolint:unparam
func (ms *readWriteSegment) rebuildIdx() error {
	// Scan the mapped file and rebuild the index

	entryOffset := ms.baseOffset

	for ms.currentFileOffset < ms.segmentSize {
		headerOffset := uint32(0)
		var entryCrc *uint32
		if ms.formatVersion == TxnFormatVersion2 {
			v := readInt(ms.txnMappedFile, ms.currentFileOffset)
			entryCrc = &v
			headerOffset += CrcLen
		}
		size := readInt(ms.txnMappedFile, ms.currentFileOffset+headerOffset)
		headerOffset += PayloadSizeLen
		if size == 0 || size > (ms.segmentSize-ms.currentFileOffset) {
			break
		}
		if entryCrc != nil {
			checksum := crc.New(ms.txnMappedFile[ms.currentFileOffset+headerOffset : ms.currentFileOffset+headerOffset+size]).Value()
			if *entryCrc != checksum {
				// todo: introduce a new error
				// todo: introduce auto-truncate logic
				return errors.New("data corrupt")
			}
		}

		ms.writingIdx = binary.BigEndian.AppendUint32(ms.writingIdx, ms.currentFileOffset)
		ms.currentFileOffset += headerOffset + size
		entryOffset++
	}

	ms.lastOffset = entryOffset - 1
	return nil
}

func (*readWriteSegment) OpenTimestamp() time.Time {
	return time.Now()
}

func (ms *readWriteSegment) Close() error {
	ms.Lock()
	defer ms.Unlock()

	err := multierr.Combine(
		ms.txnMappedFile.Unmap(),
		ms.txnFile.Close(),

		// Write index file
		ms.writeIndex(),
	)

	bufferPool.Put(&ms.writingIdx)
	return err
}

func (ms *readWriteSegment) Delete() error {
	var extension string
	if ms.formatVersion == TxnFormatVersion2 {
		extension = txnExtension2
	} else {
		extension = txnExtension
	}
	return multierr.Combine(
		ms.Close(),
		os.Remove(ms.path+extension),
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
		return ErrOffsetOutOfBounds
	}

	// Write zeroes in the section to clear
	fileLastSafeOffset := fileOffset(ms.writingIdx, ms.baseOffset, lastSafeOffset)
	headerOffset := uint32(0)
	if ms.formatVersion == TxnFormatVersion2 {
		headerOffset += CrcLen
	}
	entryLen := readInt(ms.txnMappedFile, fileLastSafeOffset+headerOffset)
	headerOffset += PayloadSizeLen
	fileEndOffset := fileLastSafeOffset + headerOffset + entryLen
	for i := fileEndOffset; i < ms.currentFileOffset; i++ {
		ms.txnMappedFile[i] = 0
	}

	// Truncate the index
	ms.writingIdx = ms.writingIdx[:4*(lastSafeOffset-ms.baseOffset+1)]

	ms.currentFileOffset = fileEndOffset
	ms.lastOffset = lastSafeOffset
	return ms.Flush()
}

func initFileWithZeroes(f *os.File, size uint32) error {
	if _, err := f.Seek(int64(size), 0); err != nil {
		return err
	}

	if _, err := f.Write([]byte{0x00}); err != nil {
		return err
	}

	return f.Sync()
}
