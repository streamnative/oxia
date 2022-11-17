package server

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"math"
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
		readers: make(map[int]updatableReader, 10),
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
	readers      map[int]updatableReader
	nextReaderId int
}

type reader struct {
	// wal the log to iterate
	wal *inMemoryWal
	// nextOffset the offset of the entry to read next
	nextOffset uint64
	// channel chan to get updates of log progression
	channel chan uint64
	closed  bool
	id      int
}

type forwardReader struct {
	reader
	// offsetCeiling the offset that must not be read (because it does not yet exist
	offsetCeiling uint64
}
type reverseReader struct {
	reader
}

type updatableReader interface {
	wal.WalReader
	Channel() chan uint64
}

func (r *forwardReader) Close() error {
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	close(r.Channel())
	delete(r.wal.readers, r.id)
	return nil
}

func (r *reverseReader) Close() error {
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	close(r.Channel())
	delete(r.wal.readers, r.id)
	return nil
}

func (r *forwardReader) Channel() chan uint64 {
	return r.channel
}

func (r *reverseReader) Channel() chan uint64 {
	return make(chan uint64, 1)
}

func (r *forwardReader) ReadNext() (*proto.LogEntry, error) {
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return nil, wal.ErrorReaderClosed
	}
	for r.nextOffset >= r.offsetCeiling {
		update, more := <-r.channel
		if !more {
			return nil, wal.ErrorReaderClosed
		} else {
			r.offsetCeiling = update + 1
		}
	}
	entry := r.wal.log[r.nextOffset]
	r.nextOffset++
	return entry, nil
}

func (r *forwardReader) HasNext() bool {
	r.wal.Lock()
	defer r.wal.Unlock()
	if r.closed {
		return false
	}
	return r.nextOffset < r.offsetCeiling-1
}

func (r *reverseReader) ReadNext() (*proto.LogEntry, error) {
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return nil, wal.ErrorReaderClosed
	}

	entry := r.wal.log[r.nextOffset]
	r.nextOffset--
	return entry, nil
}

func (r *reverseReader) HasNext() bool {
	if r.closed {
		return false
	}
	return r.nextOffset != math.MaxUint64
}

type emptyReader struct{}

func (r *emptyReader) Close() error {
	return nil
}

func (r *emptyReader) ReadNext() (*proto.LogEntry, error) {
	fmt.Printf("Reading empty reader")

	return nil, wal.ErrorEntryNotFound
}

func (r *emptyReader) HasNext() bool {
	return false
}

func (w *inMemoryWal) Close() error {
	return w.closeReaders()
}

func (w *inMemoryWal) NewReader(firstOffset uint64) (wal.WalReader, error) {
	w.Lock()
	defer w.Unlock()
	if len(w.log) == 0 {
		return &emptyReader{}, nil
	}
	r := &forwardReader{
		reader: reader{
			wal:        w,
			nextOffset: firstOffset,
			channel:    make(chan uint64, 1),
			closed:     false,
			id:         w.nextReaderId,
		},
		offsetCeiling: w.log[w.logLength()-1].EntryId.Offset + 1,
	}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) NewReverseReader() (wal.WalReader, error) {
	w.Lock()
	defer w.Unlock()
	if len(w.log) == 0 {
		return &emptyReader{}, nil
	}
	r := &reverseReader{reader{
		wal:        w,
		nextOffset: w.log[len(w.log)-1].EntryId.Offset,
		channel:    make(chan uint64, 1),
		closed:     false,
		id:         w.nextReaderId,
	}}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.Lock()
	defer w.Unlock()
	fmt.Printf("Append (%d,%d)\n", entry.EntryId.Epoch, entry.EntryId.Offset)

	w.log = append(w.log, entry)
	w.index[wal.EntryIdFromProto(entry.EntryId)] = len(w.log) - 1
	for _, reader := range w.readers {
		reader.Channel() <- entry.EntryId.Offset
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
	w.readers = make(map[int]updatableReader, 10000)
	return nil
}

func (w *inMemoryWal) TruncateLog(lastSafeEntryId wal.EntryId) (wal.EntryId, error) {
	w.Lock()
	defer w.Unlock()
	fmt.Printf("Truncate (%d,%d)\n", lastSafeEntryId.Epoch, lastSafeEntryId.Offset)
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
