package server

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/proto"
)

var (
	ErrorEntryNotFound = errors.New("oxia: entry not found")
	ErrorReaderClosed  = errors.New("oxia: reader already closed")
)

type Reader interface {
	io.Closer
	ReadNext() (*proto.LogEntry, error)
}

type Wal interface {
	io.Closer
	Append(entry *proto.LogEntry) error
	GetHighestEntryOfEpoch(epoch uint64) (EntryId, error)
	TruncateLog(headIndex EntryId) (EntryId, error)
	CloseReaders() error
	Reader(firstOffset uint64) (Reader, error)

	LogLength() uint64
}

type inMemoryWal struct {
	shard        uint32
	log          []*proto.LogEntry
	index        map[EntryId]int
	readers      map[int]*inMemoryWalReader
	nextReaderId int
}

type inMemoryWalReader struct {
	wal        *inMemoryWal
	nextOffset uint64
	maxOffset  uint64
	channel    chan uint64
	closed     bool
	id         int
}

func (r *inMemoryWalReader) Close() error {
	delete(r.wal.readers, r.id)
	return nil
}

func (r *inMemoryWalReader) ReadNext() (*proto.LogEntry, error) {
	if r.closed {
		return nil, ErrorReaderClosed
	}
	if r.nextOffset >= r.maxOffset {
		r.maxOffset = <-r.channel
	}
	entry := r.wal.log[r.nextOffset]
	r.nextOffset++
	return entry, nil
}

func NewInMemoryWal(shard uint32) Wal {
	return &inMemoryWal{
		shard:   shard,
		log:     make([]*proto.LogEntry, 10, 10000),
		index:   make(map[EntryId]int),
		readers: make(map[int]*inMemoryWalReader, 10000),
	}
}

func (w *inMemoryWal) Close() error {
	return w.CloseReaders()
}

func (w *inMemoryWal) Reader(firstOffset uint64) (Reader, error) {
	r := &inMemoryWalReader{
		wal:        w,
		nextOffset: firstOffset,
		maxOffset:  w.log[w.LogLength()-1].EntryId.Offset,
		channel:    make(chan uint64, 1),
		closed:     false,
		id:         w.nextReaderId,
	}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.log = append(w.log, entry)
	w.index[EntryIdFromProto(entry.EntryId)] = len(w.log) - 1
	for _, reader := range w.readers {
		reader.channel <- entry.EntryId.Offset
	}
	return nil
}

func (w *inMemoryWal) CloseReaders() error {
	for _, r := range w.readers {
		err := r.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing reader")
		}
	}
	w.readers = make(map[int]*inMemoryWalReader, 10000)
	return nil
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
