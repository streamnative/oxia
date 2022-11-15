package server

import (
	"oxia/proto"
	"testing"
)

func assertEquals[X comparable](t *testing.T, expected X, actual X, message string) {
	if expected != actual {
		t.Fatalf("Equality check failed for %s. Got %v, expected %v", message, actual, expected)
	}
}

func failOnErr(t *testing.T, err error, op string) {
	if err != nil {
		t.Log(err)
		t.Fatalf("Error while %s", op)
	}
}

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
			_ = wal.Append(entry)
			offset++
		}
	}
	return wal.(*inMemoryWal)
}
