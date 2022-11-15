package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/proto"
)

type Wal interface {
	io.Closer
	Append(entry *proto.LogEntry) error

	Read(lastPushedEntryId EntryId, callback func(*proto.LogEntry) error) error
	ReadSync(previousCommittedEntryId EntryId, lastCommittedEntryId EntryId, callback func(*proto.LogEntry) error) error
	GetHighestEntryOfEpoch(epoch uint64) (EntryId, error)
	TruncateLog(headIndex EntryId) (EntryId, error)
	StopReaders()
	ReadOne(id EntryId) (*proto.LogEntry, error)

	LogLength() uint64
	EntryIdAt(index uint64) EntryId
}

type inMemoryWal struct {
	shard     uint32
	log       []*proto.LogEntry
	index     map[EntryId]int
	callbacks []func(*proto.LogEntry) error
}

func NewInMemoryWal(shard uint32) Wal {
	return &inMemoryWal{
		shard:     shard,
		log:       make([]*proto.LogEntry, 0, 10000),
		index:     make(map[EntryId]int),
		callbacks: make([]func(*proto.LogEntry) error, 0, 10000),
	}
}

func (w *inMemoryWal) Close() error {
	return nil
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.log = append(w.log, entry)
	w.index[EntryIdFromProto(entry.EntryId)] = len(w.log) - 1
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

func (w *inMemoryWal) ReadOne(id EntryId) (*proto.LogEntry, error) {
	return w.log[w.index[id]], nil
}

func (w *inMemoryWal) ReadSync(previousCommittedEntryId EntryId, lastCommittedEntryId EntryId, callback func(*proto.LogEntry) error) error {
	index := w.index[previousCommittedEntryId]
	for index < len(w.log) {
		entry := w.log[index]
		if entry.EntryId.Epoch > lastCommittedEntryId.epoch ||
			(entry.EntryId.Epoch == lastCommittedEntryId.epoch &&
				entry.EntryId.Offset > lastCommittedEntryId.offset) {
			break
		}
		err2 := callback(entry)
		if err2 != nil {
			return err2
		}

		index++
	}
	return nil
}

func (w *inMemoryWal) Read(lastPushedEntryId EntryId, callback func(*proto.LogEntry) error) error {
	index := w.index[lastPushedEntryId]
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
	w.callbacks = make([]func(*proto.LogEntry) error, 0, 10000)
}

func (w *inMemoryWal) GetHighestEntryOfEpoch(epoch uint64) (EntryId, error) {
	if len(w.log) == 0 {
		return EntryId{}, nil
	}
	if w.log[0].EntryId.Epoch > epoch {
		return EntryId{}, errors.Errorf("Snapshotting is not yet implemented")
	}
	if w.log[len(w.log)-1].EntryId.Epoch <= epoch {
		return EntryIdFromProto(w.log[len(w.log)-1].EntryId), nil
	}
	for i := len(w.log) - 1; i >= 0; i-- {
		if w.log[i].EntryId.Epoch == epoch {
			return EntryIdFromProto(w.log[i].EntryId), nil
		}
	}
	// Should not happen
	return EntryId{}, nil

}

func (w *inMemoryWal) TruncateLog(lastSafeEntryId EntryId) (EntryId, error) {
	index, ok := w.index[lastSafeEntryId]
	if ok {
		for i := index + 1; i < len(w.log); i++ {
			delete(w.index, EntryIdFromProto(w.log[i].EntryId))
		}
		w.log = w.log[:index]
	}
	if len(w.log) == 0 {
		return EntryId{}, nil
	}
	return EntryIdFromProto(w.log[len(w.log)-1].EntryId), nil

}

func (w *inMemoryWal) LogLength() uint64 {
	return uint64(len(w.log))
}

func (w *inMemoryWal) EntryIdAt(index uint64) EntryId {
	return EntryIdFromProto(w.log[index].EntryId)
}
