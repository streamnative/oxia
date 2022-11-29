package wal

import (
	"fmt"
	"github.com/pkg/errors"
	pb "google.golang.org/protobuf/proto"
	"os"
	"oxia/proto"
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

func (f *factory) NewWal(shard uint32) (Wal, error) {
	impl, err := newTidwallWal(shard, f.options.LogDir)
	return impl, err
}

func (f *factory) Close() error {
	return nil
}

type tidwallWal struct {
	sync.RWMutex
	shard      uint32
	log        *Log
	lastOffset int64
}

func newTidwallWal(shard uint32, dir string) (Wal, error) {
	opts := DefaultOptions
	log, err := Open(fmt.Sprintf("%s%c%06d", dir, os.PathSeparator, shard), opts)
	if err != nil {
		return nil, err
	}
	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, err
	}

	var lastOffset int64
	if lastIndex == 0 {
		lastOffset = InvalidOffset
	} else {
		lastEntry, err := readAtIndex(log, lastIndex)
		if err != nil {
			return nil, err
		}

		lastOffset = lastEntry.Offset
	}
	w := &tidwallWal{
		shard:      shard,
		log:        log,
		lastOffset: lastOffset,
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

func (t *tidwallWal) LastOffset() int64 {
	t.Lock()
	defer t.Unlock()
	return t.lastOffset
}

func (t *tidwallWal) Close() error {
	t.Lock()
	defer t.Unlock()
	return t.log.Close()
}

func (t *tidwallWal) Append(entry *proto.LogEntry) error {
	t.Lock()
	defer t.Unlock()

	if err := t.checkNextOffset(entry.Offset); err != nil {
		return err
	}

	val, err := pb.Marshal(entry)
	if err != nil {
		return err
	}

	err = t.log.Write(val)
	if err != nil {
		return err
	}
	t.lastOffset = entry.Offset
	return err
}

func (t *tidwallWal) checkNextOffset(nextOffset int64) error {
	if nextOffset < 0 {
		return errors.New(fmt.Sprintf("Invalid next offset. %d should be > 0", nextOffset))
	}
	if nextOffset != t.lastOffset+1 {
		return errors.New(fmt.Sprintf("Invalid next offset. %d can not immediately follow %d",
			nextOffset, t.lastOffset))
	}
	return nil
}

// Convert between oxia offset and tidwall index
// Oxia offsets go from 0 -> N
// Tidwall offsets go from 1 -> N+1
func offsetToTidwallIdx(offset int64) int64 {
	return offset + 1
}

func (t *tidwallWal) TruncateLog(lastSafeOffset int64) (int64, error) {
	t.Lock()
	defer t.Unlock()

	lastIndex, err := t.log.LastIndex()
	if err != nil {
		return InvalidOffset, err
	}

	if lastIndex == 0 {
		// The WAL is empty
		return InvalidOffset, nil
	}

	lastSafeIndex := offsetToTidwallIdx(lastSafeOffset)
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

func (t *tidwallWal) NewReader(after int64) (WalReader, error) {
	t.Lock()
	defer t.Unlock()

	firstOffset := after + 1
	r := &forwardReader{
		reader: reader{
			wal:        t,
			nextOffset: firstOffset,
			closed:     false,
		},
	}

	return r, nil
}

func (t *tidwallWal) NewReverseReader() (WalReader, error) {
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
	wal *tidwallWal

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

	index := offsetToTidwallIdx(r.nextOffset)
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

	index := offsetToTidwallIdx(r.nextOffset)
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
	if r.closed {
		return false
	}
	res := r.nextOffset != InvalidOffset

	return res
}
