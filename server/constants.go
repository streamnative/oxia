package server

import (
	"math"
	"oxia/proto"
	"oxia/server/wal"
)

const (
	MaxEpoch = math.MaxInt64
)

var InvalidEntryId = &proto.EntryId{
	Epoch:  wal.InvalidEpoch,
	Offset: wal.InvalidOffset,
}
