package server

import (
	"oxia/proto"
)

func newWalWithEntries(payloads ...string) *inMemoryWal {
	wal := NewInMemoryWal(shard)
	return initWalWithEntries(wal, payloads)
}

func initWalWithEntries(wal Wal, payloads []string) *inMemoryWal {
	epoch := uint64(1)
	offset := uint64(1)
	for _, p := range payloads {
		if p == "" {
			epoch++
		} else {
			entry := &proto.LogEntry{
				EntryId: &proto.EntryId{
					Epoch:  epoch,
					Offset: offset,
				},
				Value:     []byte(p),
				Timestamp: offset,
			}
			wal.Append(entry)
			offset++
		}
	}
	return wal.(*inMemoryWal)
}
