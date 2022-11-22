package util

import (
	"fmt"
	"math/bits"
)

const MaxBitSetSize = 16

// BitSet
// Simplified and compact bitset
type BitSet struct {
	bits uint16
}

func (bs *BitSet) Count() int {
	return bits.OnesCount16(bs.bits)
}

func (bs *BitSet) Set(idx int) {
	if idx < 0 || idx >= MaxBitSetSize {
		panic(fmt.Sprintf("invalid index: %d", idx))
	}
	bs.bits |= 1 << idx
}
