package wal

import (
	"github.com/pkg/errors"
	"oxia/proto"
	"sync"
)

type inMemoryWalFactory struct{}

func NewInMemoryWalFactory() WalFactory {
	return &inMemoryWalFactory{}
}

func (f *inMemoryWalFactory) NewWal(shard uint32) (Wal, error) {
	return &inMemoryWal{
		shard:      shard,
		log:        make(map[int64]*proto.LogEntry),
		lastOffset: InvalidOffset,
	}, nil
}

func (f *inMemoryWalFactory) Close() error {
	return nil
}

type inMemoryWal struct {
	sync.Mutex
	shard       uint32
	log         map[int64]*proto.LogEntry
	firstOffset int64
	lastOffset  int64
}

type inMemReader struct {
	// wal the log to iterate
	wal *inMemoryWal
	// nextOffset the offset of the entry to read next
	nextOffset int64
	closed     bool
}

type inMemForwardReader struct {
	inMemReader
}
type inMemReverseReader struct {
	inMemReader
}

func (r *inMemForwardReader) Close() error {
	r.closed = true
	return nil
}

func (r *inMemReverseReader) Close() error {
	r.closed = true
	return nil
}

func (r *inMemForwardReader) ReadNext() (*proto.LogEntry, error) {
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return nil, ErrorReaderClosed
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

	return r.nextOffset <= r.wal.lastOffset
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
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return false
	}

	return r.nextOffset >= r.wal.firstOffset
}

func (w *inMemoryWal) Clear() error {
	w.log = make(map[int64]*proto.LogEntry)
	w.lastOffset = InvalidOffset
	return nil
}

func (w *inMemoryWal) Close() error {
	return nil
}

func (w *inMemoryWal) NewReader(after int64) (WalReader, error) {
	w.Lock()
	defer w.Unlock()
	firstOffset := after + 1
	r := &inMemForwardReader{
		inMemReader: inMemReader{
			wal:        w,
			nextOffset: firstOffset,
			closed:     false,
		},
	}
	return r, nil
}

func (w *inMemoryWal) NewReverseReader() (WalReader, error) {
	w.Lock()
	defer w.Unlock()

	r := &inMemReverseReader{inMemReader{
		wal:        w,
		nextOffset: w.lastOffset,
		closed:     false,
	}}
	return r, nil
}

func (w *inMemoryWal) LastOffset() int64 {
	w.Lock()
	defer w.Unlock()

	return w.lastOffset
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.Lock()
	defer w.Unlock()

	if w.lastOffset != InvalidOffset && entry.Offset != w.lastOffset+1 {
		return errors.Wrapf(ErrorInvalidNextOffset,
			"%d can not immediately follow %d", entry.Offset, w.lastOffset)
	}

	w.lastOffset = entry.Offset
	if len(w.log) == 0 {
		w.firstOffset = entry.Offset
	}
	w.log[entry.Offset] = entry
	return nil
}

func (w *inMemoryWal) TruncateLog(lastSafeOffset int64) (int64, error) {
	w.Lock()
	defer w.Unlock()

	if lastSafeOffset == InvalidOffset {
		w.log = make(map[int64]*proto.LogEntry)
	} else {
		for offset := range w.log {
			if offset > lastSafeOffset {
				delete(w.log, offset)
			}
		}
	}
	if len(w.log) == 0 {
		return InvalidOffset, nil
	}

	if lastSafeOffset < w.lastOffset {
		w.lastOffset = lastSafeOffset
	}

	if lastSafeOffset < w.firstOffset {
		w.firstOffset = lastSafeOffset
	}

	return w.lastOffset, nil
}
