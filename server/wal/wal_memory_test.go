package wal

import (
	"github.com/rs/zerolog/log"
	"oxia/proto"
)

type InMemoryWalFactory struct{}

func (f *InMemoryWalFactory) NewWal(shard uint32) (Wal, error) {
	return &inMemoryWal{
		shard:   shard,
		log:     make([]*proto.LogEntry, 10, 10000),
		index:   make(map[EntryId]int),
		readers: make(map[int]*inMemoryWalReader, 10000),
	}, nil
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
	forward    bool
}

func (r *inMemoryWalReader) Close() error {
	delete(r.wal.readers, r.id)
	return nil
}

func (r *inMemoryWalReader) ReadNext() (*proto.LogEntry, error) {
	if r.closed {
		return nil, ErrorReaderClosed
	}
	if !r.forward && r.nextOffset < 0 {
		return nil, ErrorEntryNotFound
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
	if r.closed {
		return false, ErrorReaderClosed
	}
	if r.forward {
		return r.nextOffset <= r.maxOffset, nil
	}
	return r.nextOffset >= 0, nil
}

func (w *inMemoryWal) Close() error {
	return w.closeReaders()
}

func (w *inMemoryWal) NewReader(firstOffset uint64) (WalReader, error) {
	r := &inMemoryWalReader{
		wal:        w,
		nextOffset: firstOffset,
		maxOffset:  w.log[w.LogLength()-1].EntryId.Offset,
		channel:    make(chan uint64, 1),
		closed:     false,
		id:         w.nextReaderId,
		forward:    true,
	}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) NewReverseReader() (WalReader, error) {
	// TODO
	r := &inMemoryWalReader{
		wal:        w,
		nextOffset: w.log[w.LogLength()-1].EntryId.Offset,
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
	w.log = append(w.log, entry)
	w.index[EntryIdFromProto(entry.EntryId)] = len(w.log) - 1
	for _, reader := range w.readers {
		reader.channel <- entry.EntryId.Offset
	}
	return nil
}

func (w *inMemoryWal) closeReaders() error {
	for _, r := range w.readers {
		err := r.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing reader")
		}
	}
	w.readers = make(map[int]*inMemoryWalReader, 10000)
	return nil
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
