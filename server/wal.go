package server

import (
	"github.com/pkg/errors"
	"io"
	"oxia/coordination"
)

type Wal interface {
	io.Closer
	Append(entry *coordination.LogEntry) (err error)

	Read(id *coordination.EntryId) (logEntry *coordination.LogEntry, err error)
	LastEntryIdUptoEpoch(epoch uint64) (*coordination.EntryId, error)
	TruncateLog(headIndex *coordination.EntryId) (*coordination.EntryId, error)
}

type wal struct {
	shard uint32

	// TODO Provide persistent implementation
	log []*coordination.LogEntry
}

func NewWal(shard uint32) Wal {
	return &wal{
		shard: shard,
		log:   make([]*coordination.LogEntry, 0, 10000),
	}
}

func (w *wal) Close() error {
	return nil
}

func (w *wal) Append(entry *coordination.LogEntry) (err error) {
	w.log = append(w.log, entry)
	return nil
}

func (w *wal) Read(id *coordination.EntryId) (logEntry *coordination.LogEntry, err error) {
	// TODO read. Maybe create an iterator because the reader does not necessarily know which id's next
	return nil, errors.New("WAL.read not implemented")
}

func (w *wal) LastEntryIdUptoEpoch(epoch uint64) (*coordination.EntryId, error) {
	//TODO implement as GetHighestEntryOfEpoch
	panic("implement me")
}

func (w *wal) TruncateLog(lastSafeEntryId *coordination.EntryId) (*coordination.EntryId, error) {
	//TODO implement as TruncateLog
	//TODO Return with HeadEntry
	panic("implement me")
}
