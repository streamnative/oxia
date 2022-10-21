package server

import (
	"io"
	"oxia/coordination"
	"sort"
)

type Wal interface {
	io.Closer
	Append(entry *coordination.LogEntry) (err error)

	Read(lastPushedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) (err error)
	LastEntryIdUptoEpoch(epoch uint64) (*coordination.EntryId, error)
	TruncateLog(headIndex *coordination.EntryId) (*coordination.EntryId, error)
	StopReaders()
	ReadOne(id *coordination.EntryId) (*coordination.LogEntry, error)
}

type wal struct {
	shard uint32

	// TODO Provide persistent implementation
	log       []*coordination.LogEntry
	callbacks []func(*coordination.LogEntry) error
}

func NewWal(shard uint32) Wal {
	return &wal{
		shard:     shard,
		log:       make([]*coordination.LogEntry, 0, 10000),
		callbacks: make([]func(*coordination.LogEntry) error, 0, 10000),
	}
}

func (w *wal) Close() error {
	return nil
}

func (w *wal) Append(entry *coordination.LogEntry) error {
	w.log = append(w.log, entry)
	for _, callback := range w.callbacks {
		err := callback(entry)
		if err != nil {
			// TODO remove callback instead
			return err
		}
	}
	return nil
}

func (w *wal) ReadOne(id *coordination.EntryId) (*coordination.LogEntry, error) {
	index := w.findId(id)
	return w.log[index], nil
}

func (w *wal) Read(lastPushedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error {
	// TODO this is a bottleneck in case of lagging followers
	index := w.findId(lastPushedEntryId) + 1
	for index < len(w.log) {
		index++
		err := callback(w.log[index])
		if err != nil {
			return err
		}
	}
	w.callbacks = append(w.callbacks, callback)
	return nil
}

func (w *wal) StopReaders() {
	w.callbacks = make([]func(*coordination.LogEntry) error, 0, 10000)
}

func (w *wal) LastEntryIdUptoEpoch(epoch uint64) (*coordination.EntryId, error) {
	// TODO implement as GetHighestEntryOfEpoch
	panic("implement me")
}

func (w *wal) TruncateLog(lastSafeEntryId *coordination.EntryId) (*coordination.EntryId, error) {
	// TODO implement as TruncateLog
	// TODO Return with HeadEntry
	panic("implement me")
}

func (w *wal) findId(id *coordination.EntryId) int {
	// TODO what if we don't fnd it?
	return sort.Search(len(w.log), func(index int) bool {
		return !(id.Epoch < w.log[index].EntryId.Epoch ||
			(id.Epoch == w.log[index].EntryId.Epoch &&
				id.Offset < w.log[index].EntryId.Offset))
	})
}
