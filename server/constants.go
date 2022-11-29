package server

import (
	"oxia/proto"
	"oxia/server/wal"
)

var InvalidEntryId = &proto.EntryId{
	Epoch:  wal.InvalidEpoch,
	Offset: wal.InvalidOffset,
}
