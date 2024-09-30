package codec

import "encoding/binary"

const TxnExtension = ".txn"
const TxnExtensionV2 = ".txnx"
const TdxExtension = ".idx"

type Metadata struct {
	TxnExtension string
	IdxExtension string
	HeaderSize   uint32
}

type Codec interface {
	//GetHeaderSize get the current version of wal entry format header size
	GetHeaderSize() uint32

	// ReadRecordWithValidation read a record
	ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error)

	ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error)

	WriteRecord(buf []byte, startOffset uint32, previousCrc uint32, payload []byte) (recordSize uint32, payloadCrc uint32)
}

func readInt(b []byte, offset uint32) uint32 {
	return binary.BigEndian.Uint32(b[offset : offset+4])
}
