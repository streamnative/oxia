package wal

import (
	"github.com/pkg/errors"
	"io"
	"oxia/proto"
)

var (
	ErrorEntryNotFound = errors.New("oxia: entry not found")
	ErrorReaderClosed  = errors.New("oxia: reader already closed")
)

type EntryId struct {
	Epoch  uint64
	Offset uint64
}

func EntryIdFromProto(id *proto.EntryId) EntryId {
	return EntryId{
		Epoch:  id.Epoch,
		Offset: id.Offset,
	}
}

func (id EntryId) ToProto() *proto.EntryId {
	return &proto.EntryId{
		Epoch:  id.Epoch,
		Offset: id.Offset,
	}
}

type WalFactory interface {
	io.Closer
	NewWal(shard uint32) (Wal, error)
}

type WalReader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	// If a reverse WalReader has passed the beginning of the log, it returns [ErrorEntryNotFound]. To avoid this error, use HasNext.
	// If a forward WalReader has passed the log end, it will wait for new entries appended. To avoid waiting, you can use HasNext.
	ReadNext() (*proto.LogEntry, error)
	// HasNext returns true if there is an entry to read.
	// For a reverse WalReader this means the reader has not yet reached the beginning of the log.
	// For a forward WalReader this means that we have not yet reached the offset that was the log end when the reader was created.
	HasNext() bool
}

type Wal interface {
	io.Closer
	// Append writes an entry to the end of the log
	Append(entry *proto.LogEntry) error
	// TruncateLog removes entries from the end of the log that have an ID greater than headIndex.
	TruncateLog(headIndex EntryId) (EntryId, error)
	// NewReader returns a new WalReader to traverse the log from the specified offset towards the log end
	NewReader(startOffset uint64) (WalReader, error)
	// NewReverseReader returns a new WalReader to traverse the log from the end towards the beginning
	NewReverseReader() (WalReader, error)
}
