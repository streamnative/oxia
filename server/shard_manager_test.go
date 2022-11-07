package server

import (
	"testing"
)

const shard = "0000-000F"

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
	wal := newWalWithEntries("Ticinus", "Cannae", "", "Cartagena", "Baecula", "Ilipa", "", "Utica", "Utica", "Zama")
	sm, err := NewShardManager(shard, "scipio:202", nil, wal, nil)
	if err != nil {
		t.Log(err)
		t.Fatalf("Unable to create shard manager")
	}
	impl := sm.(*shardManager)
	assertEquals[EntryId](t, wal.EntryIdAt(wal.LogLength()-1), impl.headIndex, "HeadIndexes")

}
