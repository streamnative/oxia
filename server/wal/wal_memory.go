package wal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"math"
	"oxia/proto"
	"sync"
)

type inMemoryWalFactory struct{}

func NewInMemoryWalFactory() WalFactory {
	return &inMemoryWalFactory{}
}

func (f *inMemoryWalFactory) NewWal(shard uint32) (Wal, error) {
	return &inMemoryWal{
		shard:   shard,
		log:     make([]*proto.LogEntry, 0, 100),
		index:   make(map[EntryId]int),
		readers: make(map[int]inMemUpdatableReader, 10),
	}, nil
}

func (f *inMemoryWalFactory) Close() error {
	return nil
}

type inMemoryWal struct {
	sync.Mutex
	shard        uint32
	log          []*proto.LogEntry
	index        map[EntryId]int
	readers      map[int]inMemUpdatableReader
	nextReaderId int
}

type inMemReader struct {
	// wal the log to iterate
	wal *inMemoryWal
	// nextOffset the offset of the entry to read next
	nextOffset uint64
	// channel chan to get updates of log progression
	channel chan uint64
	closed  bool
	id      int
}

type inMemForwardReader struct {
	inMemReader
	// maxOffsetExclusive the offset that must not be read (because e.g. it does not yet exist)
	maxOffsetExclusive uint64
}
type inMemReverseReader struct {
	inMemReader
}

type inMemUpdatableReader interface {
	WalReader
	Channel() chan uint64
}

func (r *inMemForwardReader) Close() error {
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	if r.channel != nil {
		close(r.Channel())
		r.channel = nil
	}
	delete(r.wal.readers, r.id)
	return nil
}

func (r *inMemReverseReader) Close() error {
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	close(r.Channel())
	delete(r.wal.readers, r.id)
	return nil
}

func (r *inMemForwardReader) Channel() chan uint64 {
	return r.channel
}

func (r *inMemReverseReader) Channel() chan uint64 {
	return make(chan uint64, 1)
}

func (r *inMemForwardReader) ReadNext() (*proto.LogEntry, error) {
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return nil, ErrorReaderClosed
	}
	for r.nextOffset >= r.maxOffsetExclusive {

		r.wal.Unlock()
		update, more := <-r.channel
		r.wal.Lock()
		if !more {
			return nil, ErrorReaderClosed
		} else {
			r.maxOffsetExclusive = update + 1
		}
	}
	entry := r.wal.log[r.nextOffset]
	r.nextOffset++
	return entry, nil
}

func (r *inMemForwardReader) HasNext() bool {
	r.wal.Lock()
	defer r.wal.Unlock()
	if r.closed {
		return false
	}
	return r.nextOffset < r.maxOffsetExclusive
}

func (r *inMemReverseReader) ReadNext() (*proto.LogEntry, error) {
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return nil, ErrorReaderClosed
	}

	entry := r.wal.log[r.nextOffset]
	r.nextOffset--
	return entry, nil
}

func (r *inMemReverseReader) HasNext() bool {
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
	return nil, ErrorEntryNotFound
}

func (r *emptyReader) HasNext() bool {
	return false
}

func (w *inMemoryWal) Close() error {
	return w.closeReaders()
}

func (w *inMemoryWal) NewReader(after EntryId) (WalReader, error) {
	w.Lock()
	defer w.Unlock()
	firstOffset := after.Offset + 1
	if after == (EntryId{}) {
		firstOffset = 0
	}
	maxOffsetExclusive := uint64(0)
	if len(w.log) != 0 {
		maxOffsetExclusive = w.log[len(w.log)-1].EntryId.Offset + 1
	}
	r := &inMemForwardReader{
		inMemReader: inMemReader{
			wal:        w,
			nextOffset: firstOffset,
			channel:    make(chan uint64, 1),
			closed:     false,
			id:         w.nextReaderId,
		},
		maxOffsetExclusive: maxOffsetExclusive,
	}
	w.readers[w.nextReaderId] = r
	w.nextReaderId++
	return r, nil
}

func (w *inMemoryWal) NewReverseReader() (WalReader, error) {
	w.Lock()
	defer w.Unlock()
	if len(w.log) == 0 {
		return &emptyReader{}, nil
	}
	r := &inMemReverseReader{inMemReader{
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

func (w *inMemoryWal) LastEntry() EntryId {
	if len(w.log) == 0 {
		return EntryId{}
	}

	return EntryIdFromProto(w.log[len(w.log)-1].EntryId)
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.Lock()
	defer w.Unlock()

	entryId := entry.EntryId
	lastEntryId := &proto.EntryId{}
	if len(w.log) != 0 {
		lastEntryId = w.log[len(w.log)-1].EntryId
	}

	lastEpoch := lastEntryId.Epoch
	lastOffset := lastEntryId.Offset
	nextEpoch := entryId.Epoch
	nextOffset := entryId.Offset
	if (lastOffset == 0 && lastEpoch == 0 && !(nextEpoch == 1 && nextOffset == 0)) ||
		(lastEpoch > 0 && ((nextOffset != lastOffset+1) || (nextEpoch < lastEpoch))) {
		return errors.New(fmt.Sprintf("Invalid next entry. EntryId{%d,%d} can not immediately follow EntryId{%d,%d}",
			nextEpoch, nextOffset, lastEpoch, lastOffset))
	}

	w.log = append(w.log, entry)
	w.index[EntryIdFromProto(entry.EntryId)] = len(w.log) - 1
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
	w.readers = make(map[int]inMemUpdatableReader, 10000)
	return nil
}

func (w *inMemoryWal) TruncateLog(lastSafeEntryId EntryId) (EntryId, error) {
	w.Lock()
	defer w.Unlock()
	if lastSafeEntryId == EntryIdFromProto(NonExistentEntryId) {
		w.log = make([]*proto.LogEntry, 0, 100)
		w.index = make(map[EntryId]int)
	} else {
		index, ok := w.index[lastSafeEntryId]
		if ok {
			for i := index + 1; i < len(w.log); i++ {
				delete(w.index, EntryIdFromProto(w.log[i].EntryId))
			}
			w.log = w.log[:index+1]
		}
	}
	if len(w.log) == 0 {
		return EntryId{}, nil
	}
	return EntryIdFromProto(w.log[len(w.log)-1].EntryId), nil

}
