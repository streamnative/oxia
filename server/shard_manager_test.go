package server

const shard = 1

//func TestNewShardManager(t *testing.T) {
//	wal := wal2.NewInMemoryWal(shard)
//	sm, err := NewShardManager(shard, "augustus:31", nil, wal, nil)
//	if err != nil {
//		t.Log(err)
//		t.Fatalf("Unable to create shard manager")
//	}
//	impl := sm.(*shardManager)
//	assert.Equal(t, 0, impl.epoch, "Epochs")
//	assert.Equal(t, EntryId{}, impl.commitIndex, "CommitIndexes")
//	assert.Equal(t, EntryId{}, impl.headIndex, "HeadIndexes")
//	assert.Equal(t, NotMember, impl.status, "Statuses")
//}
//
//func TestNewShardManagerInitializesFromWal(t *testing.T) {
//	wal := newWalWithEntries("Ticinus", "Cannae", "", "Cartagena", "Baecula", "Ilipa", "", "Utica", "Utica", "Zama")
//	sm, err := NewShardManager(shard, "scipio:202", nil, wal, nil)
//	if err != nil {
//		t.Log(err)
//		t.Fatalf("Unable to create shard manager")
//	}
//	impl := sm.(*shardManager)
//	assert.Equal(t, nil, impl.headIndex, "HeadIndexes")
//
//}
