package server

import (
	"github.com/rs/zerolog/log"
	"oxia/proto"
	"oxia/server/wal"
	"sync"
)

type inMemoryWalFactory struct{}

func NewInMemoryWalFactory() wal.WalFactory {
	return &inMemoryWalFactory{}
}

func (f *inMemoryWalFactory) NewWal(shard uint32) (wal.Wal, error) {
	return &inMemoryWal{
		shard:   shard,
		log:     make([]*proto.LogEntry, 0, 100),
		index:   make(map[wal.EntryId]int),
		readers: make(map[int]*inMemoryWalReader, 10),
	}, nil
}

func (f *inMemoryWalFactory) Close() error {
	return nil
}

type inMemoryWal struct {
	sync.Mutex
	shard        uint32
	log          []*proto.LogEntry
	index        map[wal.EntryId]int
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
	forward    bool
}

type emptyReader struct{}

func (r *emptyReader) Close() error {
	return nil
}

func (r *emptyReader) ReadNext() (*proto.LogEntry, error) {
	return nil, wal.ErrorEntryNotFound
}

func (r *emptyReader) HasNext() (bool, error) {
	return false, nil
}

func (r *inMemoryWalReader) Close() error {
	r.wal.Lock()
	defer r.wal.Unlock()
	delete(r.wal.readers, r.id)
	return nil
}

func (r *inMemoryWalReader) ReadNext() (*proto.LogEntry, error) {
	r.wal.Lock()
	defer r.wal.Unlock()
	if r.closed {
		return nil, wal.ErrorReaderClosed
	}
	if !r.forward && r.nextOffset < 0 {
		return nil, wal.ErrorEntryNotFound
	}
	if r.forward && r.nextOffset >= r.maxOffset {
		r.maxOffset = <-r.channel
	}
	entry := r.wal.log[r.nextOffset]
	if r.forward {
		r.nextOffset++
	} else {
		r.nextOffset--
	}
	return entry, nil
}

func (r *inMemoryWalReader) HasNext() (bool, error) {
	r.wal.Lock()
	defer r.wal.Unlock()
	if r.closed {
		return false, wal.ErrorReaderClosed
	}
	if r.forward {
		return r.nextOffset <= r.maxOffset, nil
	}
	return r.nextOffset >= 0, nil
}

func (w *inMemoryWal) Close() error {
	return w.closeReaders()
}

func (w *inMemoryWal) NewReader(firstOffset uint64) (wal.WalReader, error) {
	w.Lock()
	defer w.Unlock()
	r := &inMemoryWalReader{
		wal:        w,
		nextOffset: firstOffset,
		maxOffset:  w.log[w.logLength()-1].EntryId.Offset,
		channel:    make(chan uint64, 1),
		closed:     false,
		id:         w.nextReaderId,
		forward:    true,
	}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) NewReverseReader() (wal.WalReader, error) {
	w.Lock()
	defer w.Unlock()
	if w.logLength() == 0 {
		return &emptyReader{}, nil
	}
	r := &inMemoryWalReader{
		wal:        w,
		nextOffset: w.log[len(w.log)-1].EntryId.Offset,
		maxOffset:  0,
		channel:    make(chan uint64, 1),
		closed:     false,
		id:         w.nextReaderId,
		forward:    false,
	}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.Lock()
	defer w.Unlock()
	w.log = append(w.log, entry)
	w.index[wal.EntryIdFromProto(entry.EntryId)] = len(w.log) - 1
	for _, reader := range w.readers {
		reader.channel <- entry.EntryId.Offset
	}
	return nil
}

func (w *inMemoryWal) closeReaders() error {
	w.Lock()
	defer w.Unlock()
	for _, r := range w.readers {
		err := r.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing reader")
		}
	}
	w.readers = make(map[int]*inMemoryWalReader, 10000)
	return nil
}

func (w *inMemoryWal) TruncateLog(lastSafeEntryId wal.EntryId) (wal.EntryId, error) {
	w.Lock()
	defer w.Unlock()
	index, ok := w.index[lastSafeEntryId]
	if ok {
		for i := index + 1; i < len(w.log); i++ {
			delete(w.index, wal.EntryIdFromProto(w.log[i].EntryId))
		}
		w.log = w.log[:index]
	}
	if len(w.log) == 0 {
		return wal.EntryId{}, nil
	}
	return wal.EntryIdFromProto(w.log[len(w.log)-1].EntryId), nil

}

func (w *inMemoryWal) logLength() uint64 {
	return uint64(len(w.log))
}
