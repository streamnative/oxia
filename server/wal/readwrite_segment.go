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
	"hash/crc64"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common/crc"
)

type ReadWriteSegment interface {
	ReadOnlySegment

	Append(offset int64, data []byte) error

	Truncate(lastSafeOffset int64) error

	HasSpace(l int) bool

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

	e *encoder
}

func newReadWriteSegment(basePath string, baseOffset int64, segmentSize uint32, e *encoder) (ReadWriteSegment, error) {
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
		e:           e,
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

	// Recover the crc from existing wal
	if ms.currentFileOffset > 0 {
		prevCrc := binary.BigEndian.Uint64(ms.txnMappedFile[ms.currentFileOffset-8:])
		e.crc = crc.New(prevCrc, crc64.MakeTable(crc64.ISO))
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
	entryLen := readInt(ms.txnMappedFile, uint32(fileOffset))
	// entry := make([]byte, uint32(((entryLen>>48)&0xFF))+uint32(entryLen))
	entry := make([]byte, uint32(entryLen))
	copy(entry, ms.txnMappedFile[fileOffset+8:uint32(fileOffset)+8+uint32(entryLen)])

	return entry, nil
}

func (ms *readWriteSegment) HasSpace(l int) bool {
	return ms.currentFileOffset+16+uint32(l) <= ms.segmentSize
}

func paddingBytes(dataBytes int) (padBytes uint64) {
	padBytes = uint64((8 - (dataBytes % 8)) % 8)
	return padBytes
}

// Write implement io.Writer for encoder.
func (ms *readWriteSegment) Write(p []byte) (n int, err error) {
	copy(ms.txnMappedFile[ms.currentFileOffset:], p)
	ms.currentFileOffset += uint32(len(p))
	return len(p), nil
}

func (ms *readWriteSegment) Append(offset int64, data []byte) error {
	ms.Lock()
	defer ms.Unlock()

	if offset != ms.lastOffset+1 {
		return ErrInvalidNextOffset
	}
	if ms.currentFileOffset == 0 {
		// If this is the first entry in current segment, we initialize it's crc header
		err := ms.e.encodeCrc(ms)
		if err != nil {
			return err
		}
	}

	entryOffset := ms.currentFileOffset

	err := ms.e.encodeLog(data, ms)
	if err != nil {
		return err
	}
	ms.lastOffset = offset

	ms.writingIdx = binary.BigEndian.AppendUint64(ms.writingIdx, uint64(entryOffset))
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
		size := readInt(ms.txnMappedFile, ms.currentFileOffset)
		if size == 0 || uint32(size) > (ms.segmentSize-ms.currentFileOffset) {
			break
		}
		if size>>56 == CrcType {
			ms.currentFileOffset += 16
			continue
		}

		ms.writingIdx = binary.BigEndian.AppendUint64(ms.writingIdx, uint64(ms.currentFileOffset))
		ms.currentFileOffset += 16 + uint32(size) + uint32(size>>48&0xFF)
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
		return ErrOffsetOutOfBounds
	}

	// Write zeroes in the section to clear
	fileLastSafeOffset := fileOffset(ms.writingIdx, ms.baseOffset, lastSafeOffset)
	entryLen := readInt(ms.txnMappedFile, uint32(fileLastSafeOffset))
	fileEndOffset := uint32(fileLastSafeOffset) + 8 + uint32(entryLen) + uint32(entryLen>>48&0xFF)
	for i := ms.currentFileOffset; i < fileEndOffset; i++ {
		ms.txnMappedFile[i] = 0
	}

	// Truncate the index
	ms.writingIdx = ms.writingIdx[:8*(lastSafeOffset-ms.baseOffset+1)]

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
