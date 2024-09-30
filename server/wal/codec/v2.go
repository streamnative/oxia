package codec

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/server/util/crc"
	"github.com/streamnative/oxia/server/wal"
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

type V2 struct {
	Metadata
}

func (v V2) GetHeaderSize() uint32 {
	return v.HeaderSize
}

func (v V2) ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error) {
	if payloadSize, _, _, err := v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return nil, err
	} else {
		payload = make([]byte, payloadSize)
		payloadStartFileOffset := startFileOffset + v.HeaderSize
		copy(payload, buf[payloadStartFileOffset:payloadStartFileOffset+payloadSize])
		return payload, nil
	}
}

func (v V2) ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(len(buf))
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(wal.ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ",
				startFileOffset+v2PayloadSizeLen, bufSize)
	}

	var headerOffset uint32
	payloadSize = readInt(buf, startFileOffset)
	headerOffset += v2PayloadSizeLen

	// It shouldn't happen when normal reading
	if payloadSize == 0 {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(wal.ErrEmptyPayload, "unexpected empty payload")
	}

	expectSize := payloadSize + v.HeaderSize
	// overflow checking
	actualBufSize := bufSize - (startFileOffset + headerOffset)
	if expectSize > actualBufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(wal.ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ", expectSize, bufSize)
	}

	previousCrc = readInt(buf, startFileOffset+headerOffset)
	headerOffset += v2PreviousCrcLen
	payloadCrc = readInt(buf, startFileOffset+headerOffset)
	headerOffset += v2PayloadCrcLen

	payloadStartFileOffset := startFileOffset + headerOffset
	payloadSlice := buf[payloadStartFileOffset : payloadStartFileOffset+payloadSize]

	if expectedCrc := crc.Checksum(previousCrc).Update(payloadSlice).Value(); expectedCrc != payloadCrc {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(wal.ErrDataCorrupted,
			" expected crc: %d; actual crc: %d", expectedCrc, payloadCrc)
	}

	return payloadSize, previousCrc, payloadCrc, nil
}

func (v V2) WriteRecord(buf []byte, startOffset uint32, previousCrc uint32, payload []byte) (recordSize uint32, payloadCrc uint32) {
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
