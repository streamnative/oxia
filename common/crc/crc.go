package crc

import (
	"hash"
	"hash/crc64"
)

type digest struct {
	// crc is the previous crc value
	crc uint64

	table *crc64.Table
}

func New(prev uint64, table *crc64.Table) hash.Hash64 {
	return &digest{
		crc:   prev,
		table: table,
	}
}

func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = crc64.Update(d.crc, d.table, p)
	return len(p), nil
}

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum64()
	return append(in, byte(s>>48), byte(s>>40), byte(s>>32), byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

func (d *digest) Reset() {
	d.crc = 0
}

func (*digest) Size() int {
	return crc64.Size
}

func (*digest) BlockSize() int {
	return 1
}

func (d *digest) Sum64() uint64 {
	return d.crc
}
