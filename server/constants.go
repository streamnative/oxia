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

type Status int16

const (
	NotMember Status = iota
	Fenced
	Follower
	Leader
)

func (s Status) String() string {
	switch s {
	case NotMember:
		return "NotMember"
	case Fenced:
		return "Fenced"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}
