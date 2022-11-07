package server

import "testing"

// This serves as a compatibility test suite for WAL implementations

func newWal() Wal {
	// Change this to test another implementation
	return new(inMemoryWal)
}

func TestNewWal(t *testing.T) {
	wal := newWal()
	assertEquals(t, 0, wal.LogLength(), "Log length")
	entry, err := wal.GetHighestEntryOfEpoch(1)
	failOnErr(t, err, "Getting highest entry")
	assertEquals(t, EntryId{}, entry, "Highest entry of epoch")
}
