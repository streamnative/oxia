package server

import (
	"testing"
)
import "github.com/stretchr/testify/assert"

// This serves as a compatibility test suite for WAL implementations

func newWal() Wal {
	// Change this to test another implementation
	return NewInMemoryWal(uint32(0))
}

func TestNewWal(t *testing.T) {
	wal := newWal()
	assert.Equal(t, uint64(0), wal.LogLength(), "Log length")
	entry, err := wal.GetHighestEntryOfEpoch(1)
	assert.NoError(t, err, "Getting highest entry")
	assert.Equal(t, EntryId{}, entry, "Highest entry of epoch")
}
