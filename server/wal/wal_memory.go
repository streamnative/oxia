package wal

import (
	"fmt"
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
		shard: shard,
		log:   make([]*proto.LogEntry, 0, 100),
		index: make(map[int64]int),
	}, nil
}

func (f *inMemoryWalFactory) Close() error {
	return nil
}

type inMemoryWal struct {
	sync.Mutex
	shard uint32
	log   []*proto.LogEntry
	index map[int64]int
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

	return r.nextOffset <= r.wal.lastOffset()
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
	res := r.nextOffset != InvalidOffset

	return res
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
		nextOffset: w.lastOffset(),
		closed:     false,
	}}
	return r, nil
}

func (w *inMemoryWal) lastOffset() int64 {
	if len(w.log) == 0 {
		return InvalidOffset
	}

	return w.log[len(w.log)-1].Offset
}

func (w *inMemoryWal) LastOffset() int64 {
	w.Lock()
	defer w.Unlock()

	return w.lastOffset()
}

func (w *inMemoryWal) Append(entry *proto.LogEntry) error {
	w.Lock()
	defer w.Unlock()

	lastOffset := w.lastOffset()

	if entry.Offset != lastOffset+1 {
		return errors.New(fmt.Sprintf("Invalid next offset. Offset %d can not immediately follow %d",
			entry.Offset, lastOffset))
	}

	w.log = append(w.log, entry)
	w.index[entry.Offset] = len(w.log) - 1
	return nil
}

func (w *inMemoryWal) TruncateLog(lastSafeOffset int64) (int64, error) {
	w.Lock()
	defer w.Unlock()
	if lastSafeOffset == InvalidOffset {
		w.log = make([]*proto.LogEntry, 0, 100)
		w.index = make(map[int64]int)
	} else {
		index, ok := w.index[lastSafeOffset]
		if ok {
			for i := index + 1; i < len(w.log); i++ {
				delete(w.index, w.log[i].Offset)
			}
			w.log = w.log[:index+1]
		}
	}
	if len(w.log) == 0 {
		return InvalidOffset, nil
	}
	return w.log[len(w.log)-1].Offset, nil

}
