package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const shard = 1

func TestNewShardManager(t *testing.T) {
	wal := NewInMemoryWal(shard)
	sm, err := NewShardManager(shard, "augustus:31", nil, wal, nil)
	if err != nil {
		t.Log(err)
		t.Fatalf("Unable to create shard manager")
	}
	impl := sm.(*shardManager)
	assertEquals[uint64](t, 0, impl.epoch, "Epochs")
	assertEquals[EntryId](t, EntryId{}, impl.commitIndex, "CommitIndexes")
	assertEquals[EntryId](t, EntryId{}, impl.headIndex, "HeadIndexes")
	assertEquals[Status](t, NotMember, impl.status, "Statuses")
}

func TestNewShardManagerInitializesFromWal(t *testing.T) {
	wal, err := newWalWithEntries("Ticinus", "Cannae", "", "Cartagena", "Baecula", "Ilipa", "", "Utica", "Utica", "Zama")
	assert.NoError(t, err)
	sm, err := NewShardManager(shard, "scipio:202", nil, wal, nil)
	if err != nil {
		t.Log(err)
		t.Fatalf("Unable to create shard manager")
	}
	impl := sm.(*shardManager)
	assertEquals[EntryId](t, wal.EntryIdAt(wal.LogLength()-1), impl.headIndex, "HeadIndexes")

}
