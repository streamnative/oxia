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

package wal

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/server/util/crc"
)

const (
	SizeLen    = 4
	CrcLen     = 4
	HeaderSize = SizeLen + CrcLen + CrcLen
)

type FormatVersion int

const TxnFormatVersion1 FormatVersion = 1
const TxnFormatVersion2 FormatVersion = 2

const TxnExtension = ".txn"
const TxnExtensionV2 = ".txnx"
const TdxExtension = ".idx"

func ReadRecordWithValidation(buf []byte, startFileOffset uint32, version FormatVersion) (payload []byte, err error) {
	if payloadSize, _, _, err := ReadHeaderWithValidation(buf, startFileOffset, version); err != nil {
		return nil, err
	} else {
		payload = make([]byte, payloadSize)
		payloadStartFileOffset := startFileOffset + HeaderSize
		copy(payload, buf[payloadStartFileOffset:payloadStartFileOffset+payloadSize])
		return payload, nil
	}
}

func ReadHeaderWithValidation(buf []byte, startFileOffset uint32, version FormatVersion) (
	payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(len(buf))
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ",
			startFileOffset+SizeLen, bufSize)
	}

	var headerOffset uint32
	payloadSize = readInt(buf, startFileOffset)
	headerOffset += SizeLen

	// It shouldn't happen when normal reading
	if payloadSize == 0 {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrEmptyPayload, "unexpected empty payload")
	}

	expectSize := payloadSize + HeaderSize
	// overflow checking
	actualBufSize := bufSize - (startFileOffset + headerOffset)
	if expectSize > actualBufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ", expectSize, bufSize)
	}

	if version == TxnFormatVersion2 {
		previousCrc = readInt(buf, startFileOffset+headerOffset)
		headerOffset += CrcLen
		payloadCrc = readInt(buf, startFileOffset+headerOffset)
		headerOffset += CrcLen
	}

	payloadStartFileOffset := startFileOffset + headerOffset
	payloadSlice := buf[payloadStartFileOffset : payloadStartFileOffset+payloadSize]
	if version == TxnFormatVersion2 {
		if expectedCrc := crc.Checksum(previousCrc).Update(payloadSlice).Value(); expectedCrc != payloadCrc {
			return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrDataCorrupted,
				" expected crc: %d; actual crc: %d", expectedCrc, payloadCrc)
		}
	}

	return payloadSize, previousCrc, payloadCrc, nil
}

func WriteRecord(buf []byte, startOffset uint32, previousCrc uint32, version FormatVersion, payload []byte) (recordSize uint32, payloadCrc uint32) {
	payloadSize := uint32(len(payload))

	var headerOffset uint32
	binary.BigEndian.PutUint32(buf[startOffset:], payloadSize)
	headerOffset += SizeLen

	if version == TxnFormatVersion2 {
		binary.BigEndian.PutUint32(buf[startOffset+headerOffset:], previousCrc)
		headerOffset += CrcLen
		payloadCrc = crc.Checksum(previousCrc).Update(payload).Value()
		binary.BigEndian.PutUint32(buf[startOffset+headerOffset:], payloadCrc)
		headerOffset += CrcLen
	}

	copy(buf[startOffset+headerOffset:], payload)
	return headerOffset + payloadSize, payloadCrc
}
