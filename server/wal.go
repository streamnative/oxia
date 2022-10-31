package server

import (
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"oxia/coordination"
	"sort"
)

type Wal interface {
	io.Closer
	Append(entry *coordination.LogEntry) error

	Read(lastPushedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error
	ReadSync(previousCommittedEntryId *coordination.EntryId, lastCommittedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error
	GetHighestEntryOfEpoch(epoch uint64) (*coordination.EntryId, error)
	TruncateLog(headIndex *coordination.EntryId) (*coordination.EntryId, error)
	StopReaders()
	ReadOne(id *coordination.EntryId) (*coordination.LogEntry, error)

	LogLength() uint64
	EntryIdAt(index uint64) *coordination.EntryId
}

type inMemoryWal struct {
	shard     ShardId
	log       []*coordination.LogEntry
	callbacks []func(*coordination.LogEntry) error
}

func NewInMemoryWal(shard ShardId) Wal {
	return &inMemoryWal{
		shard:     shard,
		log:       make([]*coordination.LogEntry, 0, 10000),
		callbacks: make([]func(*coordination.LogEntry) error, 0, 10000),
	}
}

func (w *inMemoryWal) Close() error {
	return nil
}

func (w *inMemoryWal) Append(entry *coordination.LogEntry) error {
	w.log = append(w.log, entry)
	index := 0
	for index < len(w.callbacks) {
		callback := w.callbacks[index]
		err := callback(entry)
		if err != nil {
			// TODO retry
			log.Error().Err(err).Msg("Encountered error. Removing callback")
			w.callbacks[index] = w.callbacks[len(w.callbacks)-1]
			w.callbacks = w.callbacks[:len(w.callbacks)-1]
		} else {
			index++
		}
	}
	return nil
}

func (w *inMemoryWal) ReadOne(id *coordination.EntryId) (*coordination.LogEntry, error) {
	index, err := findId(w, id, true)
	if err != nil {
		return nil, err
	}
	return w.log[index], nil
}

func (w *inMemoryWal) ReadSync(previousCommittedEntryId *coordination.EntryId, lastCommittedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error {
	index, err := findId(w, previousCommittedEntryId, true)
	if err != nil {
		return err
	}
	for index+1 < len(w.log) {
		index++
		entry := w.log[index]
		if entry.EntryId.Epoch > lastCommittedEntryId.Epoch || (entry.EntryId.Epoch == lastCommittedEntryId.Epoch && entry.EntryId.Offset > lastCommittedEntryId.Offset) {
			break
		}
		err2 := callback(entry)
		if err2 != nil {
			return err2
		}
	}
	return nil
}

func (w *inMemoryWal) Read(lastPushedEntryId *coordination.EntryId, callback func(*coordination.LogEntry) error) error {
	index, err := findId(w, lastPushedEntryId, true)
	if err != nil {
		return err
	}
	for index+1 < len(w.log) {
		index++
		err2 := callback(w.log[index])
		if err2 != nil {
			return err2
		}
	}
	w.callbacks = append(w.callbacks, callback)
	return nil
}

func (w *inMemoryWal) StopReaders() {
	w.callbacks = make([]func(*coordination.LogEntry) error, 0, 10000)
}

func (w *inMemoryWal) GetHighestEntryOfEpoch(epoch uint64) (*coordination.EntryId, error) {
	index, err := findId(w, &coordination.EntryId{
		Epoch:  epoch + 1,
		Offset: 0,
	}, false)
	if err != nil {
		return nil, err
	}
	if index == 0 {
		return nil, status.Error(codes.Unimplemented, "Snapshotting is not yet implemented")
	}
	return w.log[index-1].EntryId, nil
}

func (w *inMemoryWal) TruncateLog(lastSafeEntryId *coordination.EntryId) (*coordination.EntryId, error) {
	nextIndex, err := findId(w, &coordination.EntryId{
		Epoch:  lastSafeEntryId.Epoch,
		Offset: lastSafeEntryId.Offset + 1,
	}, false)
	if err != nil {
		return nil, err
	}
	w.log = w.log[:nextIndex]
	if len(w.log) == 0 {
		return nil, nil
	}
	return w.log[len(w.log)-1].EntryId, nil
}

func (w *inMemoryWal) LogLength() uint64 {
	return uint64(len(w.log))
}

func (w *inMemoryWal) EntryIdAt(index uint64) *coordination.EntryId {
	return w.log[index].EntryId
}

func findId(w Wal, id *coordination.EntryId, failIfNotFound bool) (int, error) {
	index := sort.Search(int(w.LogLength()), func(index int) bool {
		iD := w.EntryIdAt(uint64(index))
		return !(id.Epoch < iD.Epoch ||
			(id.Epoch == iD.Epoch &&
				id.Offset < iD.Offset))
	})
	if failIfNotFound && index == int(w.LogLength()) {
		return -1, status.Error(codes.Unimplemented, "Snapshotting is not yet implemented")
	}
	return index, nil
}
