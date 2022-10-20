package server

import (
	"github.com/pkg/errors"
	"io"
	"oxia/coordination"
	"oxia/proto"
)

type Wal interface {
	io.Closer
	Append(epoch uint64, payload []byte) (entryId uint64, err error)

	Read(entryId uint64) (logEntry *proto.LogEntry, err error)
	LastEntryIdUptoEpoch(epoch uint64) (*coordination.EntryId, error)
}

type wal struct {
	shard uint32

	// Dummy in-memory implementation
	log []proto.LogEntry
}

func NewWal(shard uint32) Wal {
	return &wal{
		shard: shard,
		log:   make([]proto.LogEntry, 0),
	}
}

func (w *wal) Close() error {
	return nil
}

func (w *wal) Append(epoch uint64, payload []byte) (entryId uint64, err error) {
	panic("implement me")
}

func (w *wal) Read(entryId uint64) (logEntry *proto.LogEntry, err error) {
	return nil, errors.New("WAL.read not implemented")
}

func (w *wal) LastEntryIdUptoEpoch(epoch uint64) (*coordination.EntryId, error) {
	//TODO implement me as GetHighestEntryOfEpoch
	panic("implement me")
}
