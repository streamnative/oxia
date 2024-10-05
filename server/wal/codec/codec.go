package codec

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"slices"
)

var (
	ErrOffsetOutOfBounds = errors.New("oxia: offset out of bounds")
	ErrEmptyPayload      = errors.New("oxia: empty payload")
	ErrDataCorrupted     = errors.New("oxia: data corrupted")
)

const TxnExtension = ".txn"
const TxnExtensionV2 = ".txnx"
const IdxExtension = ".idx"

var v1 = V1{
	Metadata{
		TxnExtension: TxnExtension,
		IdxExtension: IdxExtension,
		HeaderSize:   v1PayloadSizeLen,
	},
}
var v2 = V2{
	Metadata{
		TxnExtension: TxnExtensionV2,
		IdxExtension: IdxExtension,
		HeaderSize:   v2PayloadSizeLen + v2PreviousCrcLen + v2PayloadCrcLen,
	},
}

type Metadata struct {
	TxnExtension string
	IdxExtension string
	HeaderSize   uint32
}

type Codec interface {

	// GetHeaderSize returns the fixed size of the header in bytes
	// for each record. This value is used to understand where the
	// payload starts after the header.
	GetHeaderSize() uint32

	GetRecordSize(buf []byte, startFileOffset uint32) (uint32, error)

	// ReadRecordWithValidation reads a record starting at the specified
	// file offset in the buffer. It also validates the record's integrity
	// (e.g., CRC checks) before returning the payload.
	//
	// Parameters:
	// - buf: The buffer containing the data to read from.
	// - startFileOffset: The file offset to start reading from.
	//
	// Returns:
	// - payload: The actual data (payload) of the record.
	// - err: Error if any issues occur during reading or validation.
	ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error)

	// ReadHeaderWithValidation reads the header of a record at the specified
	// offset and validates the integrity of the header data (e.g., CRC checks).
	//
	// Parameters:
	// - buf: The buffer containing the data to read from.
	// - startFileOffset: The file offset to start reading from.
	//
	// Returns:
	// - payloadSize: The size of the payload.
	// - previousCrc: The CRC value of the previous record.
	// - payloadCrc: The CRC value of the current payload.
	// - err: Error if any issues occur during reading or validation.
	ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error)

	// WriteRecord writes a record to the buffer, starting at the specified
	// offset, and includes a header with metadata like CRC.
	//
	// Parameters:
	// - buf: The buffer where the record will be written.
	// - startFileOffset: The file offset to start reading from.
	// - previousCrc: The CRC value of the previous record to maintain consistency.
	// - payload: The actual data (payload) to write as part of the record.
	//
	// Returns:
	// - recordSize: The total size of the written record, including the header.
	// - payloadCrc: The CRC value of the written payload.
	WriteRecord(buf []byte, startFileOffset uint32, previousCrc uint32, payload []byte) (recordSize uint32, payloadCrc uint32)
}

func GetSegmentContext(segmentTxnBasePath string) (codec Codec, segmentTxnFullPath string, segmentExist bool) {
	segmentTxnFullPath = segmentTxnBasePath + TxnExtensionV2
	if _, err := os.Stat(segmentTxnFullPath); os.IsNotExist(err) {
		// fallback to v1 and check again.
		segmentTxnFullPath = segmentTxnBasePath + TxnExtension
		// check the v1 file exist?
		if _, err = os.Stat(segmentTxnFullPath); os.IsNotExist(err) {
			// go back to v2
			segmentTxnFullPath = segmentTxnBasePath + TxnExtensionV2
			return v2, segmentTxnFullPath, false
		} else {
			return v1, segmentTxnFullPath, true
		}
	} else {
		return v2, segmentTxnFullPath, true
	}
}

func ListAllSegments(walPath string) (segments []int64, err error) {
	dir, err := os.ReadDir(walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to list files in wal directory %s", walPath)
	}

	for _, entry := range dir {
		if matched, _ := filepath.Match("*"+TxnExtension, entry.Name()); matched {
			var id int64
			if _, err := fmt.Sscanf(entry.Name(), "%d"+TxnExtension, &id); err != nil {
				return nil, err
			}

			segments = append(segments, id)
		}
		if matched, _ := filepath.Match("*"+TxnExtensionV2, entry.Name()); matched {
			var id int64
			if _, err := fmt.Sscanf(entry.Name(), "%d"+TxnExtensionV2, &id); err != nil {
				return nil, err
			}

			segments = append(segments, id)
		}
	}

	slices.Sort(segments)
	return segments, nil
}

// ReadInt read unsigned int from buf with big endian.
func ReadInt(b []byte, offset uint32) uint32 {
	return binary.BigEndian.Uint32(b[offset : offset+4])
}
