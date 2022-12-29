package wal

import (
	"fmt"
	"github.com/pkg/errors"
	pb "google.golang.org/protobuf/proto"
	"oxia/proto"
	"path/filepath"
	"sync"
)

type factory struct {
	options *WalFactoryOptions
}

func NewWalFactory(options *WalFactoryOptions) WalFactory {
	return &factory{
		options: options,
	}
}

func NewInMemoryWalFactory() WalFactory {
	return &factory{
		options: &WalFactoryOptions{
			LogDir:   "/",
			InMemory: true,
		},
	}
}

func (f *factory) NewWal(shard uint32) (Wal, error) {
	impl, err := newPersistentWal(shard, f.options)
	return impl, err
}

func (f *factory) Close() error {
	return nil
}

type persistentWal struct {
	sync.RWMutex
	shard       uint32
	log         *Log
	firstOffset int64
	lastOffset  int64
}

func newPersistentWal(shard uint32, options *WalFactoryOptions) (Wal, error) {
	opts := DefaultOptions()
	opts.InMemory = options.InMemory
	walPath := filepath.Join(options.LogDir, fmt.Sprint("shard-", shard))
	log, err := Open(walPath, opts)
	if err != nil {
		return nil, err
	}
	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, err
	}

	var firstOffset, lastOffset int64
	if lastIndex == -1 {
		lastOffset = InvalidOffset
		firstOffset = InvalidOffset
	} else {
		lastEntry, err := readAtIndex(log, lastIndex)
		if err != nil {
			return nil, err
		}

		lastOffset = lastEntry.Offset
		firstOffset = log.firstOffset
	}
	w := &persistentWal{
		shard:       shard,
		log:         log,
		lastOffset:  lastOffset,
		firstOffset: firstOffset,
	}
	return w, nil
}

func readAtIndex(log *Log, index int64) (*proto.LogEntry, error) {
	val, err := log.Read(index)
	if err != nil {
		return nil, err
	}

	entry := &proto.LogEntry{}
	if err = pb.Unmarshal(val, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (t *persistentWal) LastOffset() int64 {
	t.Lock()
	defer t.Unlock()
	return t.lastOffset
}

func (t *persistentWal) FirstOffset() int64 {
	t.Lock()
	defer t.Unlock()
	return t.firstOffset
}

func (t *persistentWal) Trim(firstOffset int64) error {
	t.Lock()
	defer t.Unlock()

	if err := t.log.TruncateFront(firstOffset); err != nil {
		return err
	}

	t.firstOffset = t.log.firstOffset
	return nil
}

func (t *persistentWal) Close() error {
	t.Lock()
	defer t.Unlock()
	return t.log.Close()
}

func (t *persistentWal) Append(entry *proto.LogEntry) error {
	t.Lock()
	defer t.Unlock()

	if err := t.checkNextOffset(entry.Offset); err != nil {
		return err
	}

	val, err := pb.Marshal(entry)
	if err != nil {
		return err
	}

	err = t.log.Write(entry.Offset, val)
	if err != nil {
		return err
	}
	t.lastOffset = entry.Offset
	if t.firstOffset == InvalidOffset {
		t.firstOffset = t.log.firstOffset
	}
	return err
}

func (t *persistentWal) checkNextOffset(nextOffset int64) error {
	if nextOffset < 0 {
		return errors.New(fmt.Sprintf("Invalid next offset. %d should be > 0", nextOffset))
	}
	if t.lastOffset != InvalidOffset && nextOffset != t.lastOffset+1 {
		return errors.Wrapf(ErrorInvalidNextOffset,
			"%d can not immediately follow %d", nextOffset, t.lastOffset)
	}
	return nil
}

func (t *persistentWal) Clear() error {
	if err := t.log.Clear(); err != nil {
		return err
	}

	t.lastOffset = InvalidOffset
	return nil
}

func (t *persistentWal) TruncateLog(lastSafeOffset int64) (int64, error) {
	t.Lock()
	defer t.Unlock()

	lastIndex, err := t.log.LastIndex()
	if err != nil {
		return InvalidOffset, err
	}

	if lastIndex == -1 {
		// The WAL is empty
		return InvalidOffset, nil
	}

	lastSafeIndex := lastSafeOffset
	if err := t.log.TruncateBack(lastSafeIndex); err != nil {
		return InvalidOffset, err
	}

	if lastIndex, err = t.log.LastIndex(); err != nil {
		return InvalidOffset, err
	}
	val, err := t.log.Read(lastIndex)
	if err != nil {
		return InvalidOffset, err
	}
	lastEntry := &proto.LogEntry{}
	err = pb.Unmarshal(val, lastEntry)
	if err != nil {
		return InvalidOffset, err
	}

	if lastEntry.Offset != lastSafeOffset {
		return InvalidOffset, errors.New(fmt.Sprintf("Truncating to %+v resulted in last entry %+v",
			lastSafeOffset, lastEntry.Offset))
	}
	t.lastOffset = lastEntry.Offset
	return lastEntry.Offset, nil
}

func (t *persistentWal) NewReader(after int64) (WalReader, error) {
	t.Lock()
	defer t.Unlock()

	firstOffset := after + 1

	if firstOffset < t.firstOffset {
		return nil, ErrorEntryNotFound
	}

	r := &forwardReader{
		reader: reader{
			wal:        t,
			nextOffset: firstOffset,
			closed:     false,
		},
	}

	return r, nil
}

func (t *persistentWal) NewReverseReader() (WalReader, error) {
	t.RLock()
	defer t.RUnlock()

	r := &reverseReader{reader{
		wal:        t,
		nextOffset: t.lastOffset,
		closed:     false,
	}}
	return r, nil
}

type reader struct {
	// wal the log to iterate
	wal *persistentWal

	nextOffset int64

	closed bool
}

type forwardReader struct {
	reader
	sync.Mutex
}

type reverseReader struct {
	reader
}

func (r *forwardReader) Close() error {
	r.Lock()
	defer r.Unlock()
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	return nil
}

func (r *reverseReader) Close() error {
	r.closed = true
	return nil
}

func (r *forwardReader) ReadNext() (*proto.LogEntry, error) {
	r.Lock()
	defer r.Unlock()

	if r.closed {
		return nil, ErrorReaderClosed
	}

	index := r.nextOffset
	r.wal.RLock()
	defer r.wal.RUnlock()
	entry, err := readAtIndex(r.wal.log, index)
	if err != nil {
		return nil, err
	}

	r.nextOffset++
	return entry, nil
}

func (r *forwardReader) HasNext() bool {
	r.Lock()
	defer r.Unlock()

	if r.closed {
		return false
	}

	r.wal.Lock()
	defer r.wal.Unlock()

	return r.nextOffset <= r.wal.lastOffset
}

func (r *reverseReader) ReadNext() (*proto.LogEntry, error) {

	if r.closed {
		return nil, ErrorReaderClosed
	}

	index := r.nextOffset
	r.wal.RLock()
	defer r.wal.RUnlock()

	entry, err := readAtIndex(r.wal.log, index)
	if err != nil {
		return nil, err
	}
	// If we read the first entry, this overflows to MaxUint64
	r.nextOffset--
	return entry, nil
}

func (r *reverseReader) HasNext() bool {
	r.wal.Lock()
	defer r.wal.Unlock()

	if r.closed {
		return false
	}
	return r.nextOffset != (r.wal.log.firstOffset - 1)
}
