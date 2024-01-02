package wal

import (
	"encoding/binary"
	"hash"
	"hash/crc64"
	"io"

	"github.com/streamnative/oxia/common/crc"
)

type encoder struct {
	crc hash.Hash64

	uint64buf []byte
}

func newEncoder(prevCrc uint64) *encoder {
	return &encoder{
		crc:       crc.New(prevCrc, crc64.MakeTable(crc64.ISO)),
		uint64buf: make([]byte, 8),
	}
}

func (e *encoder) encodeCrc(w io.Writer) error {
	dataHeader := CrcType << 56
	err := e.writeUint64(dataHeader, w)
	if err != nil {
		return err
	}

	err = e.writeUint64(e.crc.Sum64(), w)
	return err
}

func (e *encoder) encodeLog(data []byte, w io.Writer) error {
	e.crc.Write(data)
	crc := e.crc.Sum64()

	padBytes := paddingBytes(len(data))
	dataHeader := LogType<<56 | padBytes<<48 | uint64(uint32(len(data)))

	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}

	err := e.writeUint64(dataHeader, w)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	err = e.writeUint64(crc, w)
	return err
}

func (e *encoder) writeUint64(n uint64, w io.Writer) error {
	binary.BigEndian.PutUint64(e.uint64buf, n)
	_, err := w.Write(e.uint64buf)
	return err
}
