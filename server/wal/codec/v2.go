// Copyright 2024 StreamNative, Inc.
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

package codec

import (
	"encoding/binary"
	"github.com/streamnative/oxia/server/wal"
	"log/slog"
	"os"

	"github.com/pkg/errors"

	"github.com/streamnative/oxia/server/util/crc"
)

// +--------------+-------------------+-------------+--------------+
// | Size(4Bytes) | PreviousCRC(4Bytes) | CRC(4Bytes) | Payload(...) |
// +--------------+-------------------+-------------+--------------+
// Size: 			Length of the payload data
// PreviousCRC: 	32bit hash computed over the previous payload using CRC.
// CRC:				32bit hash computed over the previous and the current payload using CRC. CRC(n) = CRC( DATAn, CRCn-1 )
// Payload: 		Byte stream as long as specified by the payload size.
var _ Codec = V2{}

const v2PayloadSizeLen uint32 = 4
const v2PreviousCrcLen uint32 = 4
const v2PayloadCrcLen uint32 = 4

const v2IndexCrcLen uint32 = 4

const v2TxnExtension = ".txnx"
const v2IdxExtension = ".idxx"

var v2 = &V2{
	Metadata{
		TxnExtension: v2TxnExtension,
		IdxExtension: v2IdxExtension,
		HeaderSize:   v2PayloadSizeLen + v2PreviousCrcLen + v2PayloadCrcLen,
	},
}

type V2 struct {
	Metadata
}

func (v V2) GetIdxExtension() string {
	return v.IdxExtension
}

func (v V2) GetTxnExtension() string {
	return v.TxnExtension
}

func (v V2) GetHeaderSize() uint32 {
	return v.HeaderSize
}

func (v V2) GetRecordSize(buf []byte, startFileOffset uint32) (uint32, error) {
	var payloadSize uint32
	var err error
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return 0, err
	}
	return v.HeaderSize + payloadSize, nil
}

func (v V2) ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error) {
	var payloadSize uint32
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return nil, err
	}
	payload = make([]byte, payloadSize)
	payloadStartFileOffset := startFileOffset + v.HeaderSize
	copy(payload, buf[payloadStartFileOffset:payloadStartFileOffset+payloadSize])
	return payload, nil
}

func (v V2) ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(len(buf))
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ",
				startFileOffset+v2PayloadSizeLen, bufSize)
	}

	var headerOffset uint32
	payloadSize = ReadInt(buf, startFileOffset)
	headerOffset += v2PayloadSizeLen

	// It shouldn't happen when normal reading
	if payloadSize == 0 {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrEmptyPayload, "unexpected empty payload")
	}

	expectSize := payloadSize + v.HeaderSize
	// overflow checking
	actualBufSize := bufSize - (startFileOffset + headerOffset)
	if expectSize > actualBufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ", expectSize, bufSize)
	}

	previousCrc = ReadInt(buf, startFileOffset+headerOffset)
	headerOffset += v2PreviousCrcLen
	payloadCrc = ReadInt(buf, startFileOffset+headerOffset)
	headerOffset += v2PayloadCrcLen

	payloadStartFileOffset := startFileOffset + headerOffset
	payloadSlice := buf[payloadStartFileOffset : payloadStartFileOffset+payloadSize]

	if expectedCrc := crc.Checksum(previousCrc).Update(payloadSlice).Value(); expectedCrc != payloadCrc {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrDataCorrupted,
			" expected crc: %d; actual crc: %d", expectedCrc, payloadCrc)
	}

	return payloadSize, previousCrc, payloadCrc, nil
}

func (V2) WriteRecord(buf []byte, startOffset uint32, previousCrc uint32, payload []byte) (recordSize uint32, payloadCrc uint32) {
	payloadSize := uint32(len(payload))

	var headerOffset uint32
	binary.BigEndian.PutUint32(buf[startOffset:], payloadSize)
	headerOffset += v2PayloadSizeLen

	binary.BigEndian.PutUint32(buf[startOffset+headerOffset:], previousCrc)
	headerOffset += v2PreviousCrcLen
	payloadCrc = crc.Checksum(previousCrc).Update(payload).Value()
	binary.BigEndian.PutUint32(buf[startOffset+headerOffset:], payloadCrc)
	headerOffset += v2PayloadCrcLen

	copy(buf[startOffset+headerOffset:], payload)
	return headerOffset + payloadSize, payloadCrc
}

func (V2) WriteIndex(file *os.File, index []byte) error {
	buf := make([]byte, uint32(len(index))+v2IndexCrcLen)
	indexCrc := crc.Checksum(0).Update(index).Value()
	binary.BigEndian.PutUint32(buf[0:], indexCrc)
	copy(buf[v2IndexCrcLen:], index)
	_, err := file.Write(buf)
	return err
}

func (V2) ReadIndex(buf []byte) ([]byte, error) {
	expectedCrc := ReadInt(buf, 0)
	actualCrc := crc.Checksum(0).Update(buf[v2IndexCrcLen:]).Value()
	if expectedCrc != actualCrc {
		return nil, errors.Wrapf(ErrDataCorrupted,
			" expected crc: %d; actual crc: %d", expectedCrc, actualCrc)
	}
	index := make([]byte, uint32(len(buf))-v2IndexCrcLen)
	copy(index, buf[v2IndexCrcLen:])
	return index, nil
}

func (v V2) RecoverIndex(buf []byte, startFileOffset uint32, baseEntryOffset int64,
	commitOffsetProvider wal.CommitOffsetProvider) (index []byte, lastCrc uint32,
	newFileOffset uint32, newEntryOffset int64, err error) {
	maxSize := uint32(len(buf))
	newFileOffset = startFileOffset
	newEntryOffset = baseEntryOffset

	index = BorrowEmptyIndexBuf()

	for newFileOffset < maxSize {
		var payloadSize uint32
		var payloadCrc uint32
		var err error
		if payloadSize, _, payloadCrc, err = v.ReadHeaderWithValidation(buf, newFileOffset); err != nil {
			if errors.Is(err, ErrEmptyPayload) {
				// we might read the end of the segment.
				break
			}
			// data corruption
			if errors.Is(err, ErrOffsetOutOfBounds) || errors.Is(err, ErrDataCorrupted) {
				if commitOffsetProvider != nil && newEntryOffset > commitOffsetProvider.CommitOffset() {
					// uncommited data corruption, simply discard it
					slog.Warn("discard the corrupted uncommited data.",
						slog.Int64("entryId", newEntryOffset), slog.Any("error", err))
					break
				}
				return nil, 0, 0, 0, errors.Wrapf(err, "entryOffset: %d", newEntryOffset)
			}
			return nil, 0, 0, 0, err
		}
		lastCrc = payloadCrc
		index = binary.BigEndian.AppendUint32(index, newFileOffset)
		newFileOffset += v.GetHeaderSize() + payloadSize
		newEntryOffset++
	}
	return index, lastCrc, newFileOffset, newEntryOffset, nil
}
