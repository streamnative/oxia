package wal

import (
	"github.com/pkg/errors"
	"io"
	"oxia/proto"
)

var (
	ErrorEntryNotFound       = errors.New("oxia: entry not found")
	ErrorReaderClosed        = errors.New("oxia: reader already closed")
	InvalidEpoch       int64 = -1
	InvalidOffset      int64 = -1
)

type WalFactoryOptions struct {
	LogDir string
}

var DefaultWalFactoryOptions = &WalFactoryOptions{
	LogDir: "wal",
}

type WalFactory interface {
	io.Closer
	NewWal(shard uint32) (Wal, error)
}

// WalReader reads the Wal sequentially. It is not synchronized itself.
type WalReader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	// If a reverse WalReader has passed the beginning of the log, it returns [ErrorEntryNotFound]. To avoid this error, use HasNext.
	// If a forward WalReader has passed the log end, it will wait for new entries appended. To avoid waiting, you can use HasNext.
	ReadNext() (*proto.LogEntry, error)
	// HasNext returns true if there is an entry to read.
	// For a reverse WalReader this means the reader has not yet reached the beginning of the log.
	// For a forward WalReader this means that we have not yet reached the end of the wal
	HasNext() bool
}

type Wal interface {
	io.Closer
	// Append writes an entry to the end of the log
	Append(entry *proto.LogEntry) error
	// TruncateLog removes entries from the end of the log that have an ID greater than lastSafeEntry.
	TruncateLog(lastSafeEntry int64) (int64, error)
	// NewReader returns a new WalReader to traverse the log from the entry after `after` towards the log end
	NewReader(after int64) (WalReader, error)
	// NewReverseReader returns a new WalReader to traverse the log from the last entry towards the beginning
	NewReverseReader() (WalReader, error)

	// LastOffset Return the offset of the last entry committed to the WAL
	// Return InvalidOffset if the WAL is empty
	LastOffset() int64
}
