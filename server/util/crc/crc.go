package crc

import (
	"hash/crc32"
)

var table = crc32.MakeTable(crc32.Castagnoli)

type Checksum uint32

func New(b []byte) Checksum {
	return Checksum(0).Update(b)
}
func (c Checksum) Update(b []byte) Checksum {
	return Checksum(crc32.Update(uint32(c), table, b))
}
func (c Checksum) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}
