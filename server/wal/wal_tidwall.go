package wal

import (
	"fmt"
	"github.com/pkg/errors"
	tidwall "github.com/tidwall/wal"
	pb "google.golang.org/protobuf/proto"
	"math"
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
	shard        uint32
	log          *tidwall.Log
	lastEntryId  *proto.EntryId
	readers      map[int]updatableReader
	nextReaderId int
}

func newTidwallWal(shard uint32, dir string) (Wal, error) {
	opts := tidwall.DefaultOptions
	log, err := tidwall.Open(fmt.Sprintf("%s%c%06d", dir, os.PathSeparator, shard), opts)
	if err != nil {
		return nil, err
	}
	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, err
	}
	var lastEntry *proto.LogEntry
	if lastIndex == 0 {
		// If this is a new log, we need to add an entry to it, because a full truncation is not allowed
		entry := &proto.LogEntry{
			EntryId: &proto.EntryId{},
			Value:   []byte("oxia"),
		}
		val, err := pb.Marshal(entry)
		if err != nil {
			return nil, err
		}
		if err = log.Write(1, val); err != nil {
			return nil, err
		}
		lastEntry = entry
	} else {
		lastEntry, err = readAtIndex(log, lastIndex)
		if err != nil {
			return nil, err
		}
	}
	w := &tidwallWal{
		shard:        shard,
		log:          log,
		lastEntryId:  lastEntry.EntryId,
		readers:      make(map[int]updatableReader),
		nextReaderId: 0,
	}
	return w, nil
}

func readAtIndex(log *tidwall.Log, index uint64) (*proto.LogEntry, error) {
	entry := &proto.LogEntry{}
	val, err := log.Read(index)
	if err != nil {
		return nil, err
	}
	err = pb.Unmarshal(val, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (t *tidwallWal) LastEntry() EntryId {
	return EntryIdFromProto(t.lastEntryId)
}

func (t *tidwallWal) Close() error {
	return t.log.Close()
}

func (t *tidwallWal) Append(entry *proto.LogEntry) error {
	t.Lock()
	defer t.Unlock()
	err := t.checkNextEntryId(entry.EntryId)
	if err != nil {
		return err
	}
	index := entryIdToIndex(entry.EntryId)
	val, err := pb.Marshal(entry)
	if err != nil {
		return err
	}
	err = t.log.Write(index, val)
	if err != nil {
		return err
	}
	t.lastEntryId = entry.EntryId
	for _, reader := range t.readers {
		reader.updateMaxIdInclusive(entry.EntryId)
	}
	return err
}

func (t *tidwallWal) checkNextEntryId(entryId *proto.EntryId) error {
	lastEpoch := t.lastEntryId.Epoch
	lastOffset := t.lastEntryId.Offset
	nextEpoch := entryId.Epoch
	nextOffset := entryId.Offset
	if (lastOffset == 0 && lastEpoch == 0 && !(nextEpoch == 1 && nextOffset == 0)) ||
		(lastEpoch > 0 && ((nextOffset != lastOffset+1) || (nextEpoch < lastEpoch))) {
		return errors.New(fmt.Sprintf("Invalid next entry. EntryId{%d,%d} can not immediately follow EntryId{%d,%d}",
			nextEpoch, nextOffset, lastEpoch, lastOffset))
	}
	return nil
}

// we need to convert between oxia offset and tidwall index, because the index starts with 1, and we added a placeholder when opening
// Note, this method gives the place the entry id is supposed to be _if_ it is in the log
func entryIdToIndex(id *proto.EntryId) uint64 {
	if id.Epoch == 0 && id.Offset == 0 {
		return 1
	}
	return id.Offset + 2
}

func offsetToIndex(offset uint64) uint64 {
	return offset + 2
}

func (t *tidwallWal) TruncateLog(lastSafeEntry EntryId) (EntryId, error) {
	t.Lock()
	defer t.Unlock()
	zero := EntryId{}
	lastSafeIndex := entryIdToIndex(lastSafeEntry.ToProto())
	err := t.log.TruncateBack(lastSafeIndex)
	if err != nil {
		return zero, err
	}
	lastIndex, err := t.log.LastIndex()
	if err != nil {
		return zero, err
	}
	val, err := t.log.Read(lastIndex)
	if err != nil {
		return zero, err
	}
	entry := &proto.LogEntry{}
	err = pb.Unmarshal(val, entry)
	if err != nil {
		return zero, err
	}
	entryId := entry.EntryId
	lastEntryId := EntryIdFromProto(entryId)
	if entryId.Epoch != lastSafeEntry.Epoch || entryId.Offset != lastSafeEntry.Offset {
		return zero, errors.New(fmt.Sprintf("Truncating to %+v resulted in last entry %+v",
			lastSafeEntry, lastEntryId))
	}
	t.lastEntryId = entryId
	return lastEntryId, nil
}

func (t *tidwallWal) NewReader(after EntryId) (WalReader, error) {
	t.Lock()
	defer t.Unlock()
	firstOffset := uint64(0)
	if after != (EntryId{}) {
		firstOffset = after.Offset + 1
	}
	r := &forwardReader{
		reader: reader{
			wal:        t,
			nextOffset: firstOffset,
			closed:     false,
		},
		maxIdInclusive: t.lastEntryId,
		channel:        make(chan *proto.EntryId, 1),
		id:             t.nextReaderId,
	}
	t.readers[t.nextReaderId] = r
	t.nextReaderId++

	return r, nil
}

func (t *tidwallWal) NewReverseReader() (WalReader, error) {
	t.RLock()
	defer t.RUnlock()
	nextOffset := uint64(math.MaxUint64)
	if EntryIdFromProto(t.lastEntryId) != (EntryId{}) {
		nextOffset = t.lastEntryId.Offset
	}
	r := &reverseReader{reader{
		wal:        t,
		nextOffset: nextOffset,
		closed:     false,
	}}
	return r, nil

}

type reader struct {
	// wal the log to iterate
	wal *tidwallWal

	nextOffset uint64

	closed bool
}

type forwardReader struct {
	reader
	sync.Mutex
	// maxOffsetExclusive the offset that must not be read (because e.g. it does not yet exist)
	maxIdInclusive *proto.EntryId
	// channel chan to get updates of log progression
	channel chan *proto.EntryId
	id      int
}

type reverseReader struct {
	reader
}

type updatableReader interface {
	WalReader
	updateMaxIdInclusive(maxIndexInclusive *proto.EntryId)
}

func (r *forwardReader) Close() error {
	r.Lock()
	defer r.Unlock()
	r.wal.Lock()
	defer r.wal.Unlock()
	r.closed = true
	close(r.channel)
	delete(r.wal.readers, r.id)
	return nil
}

func (r *reverseReader) Close() error {
	r.closed = true
	return nil
}

func (r *forwardReader) updateMaxIdInclusive(maxIndexInclusive *proto.EntryId) {
	r.channel <- maxIndexInclusive
}

func (r *forwardReader) ReadNext() (*proto.LogEntry, error) {
	r.Lock()
	if r.closed {
		r.Unlock()
		return nil, ErrorReaderClosed
	}

	for r.nextOffset > r.maxIdInclusive.Offset {
		r.Unlock()
		update, more := <-r.channel
		if !more {
			return nil, ErrorReaderClosed
		} else {
			r.Lock()
			r.maxIdInclusive = update
		}
	}
	defer r.Unlock()
	index := offsetToIndex(r.nextOffset)
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
	closed := r.closed
	r.Unlock()
	if closed {
		return false
	}
	select {
	case update, more := <-r.channel:
		if !more {
			return false
		}
		r.Lock()
		r.maxIdInclusive = update
		r.Unlock()
	default:
	}
	r.Lock()
	r.wal.Lock()
	defer r.wal.Unlock()
	defer r.Unlock()
	return r.nextOffset <= r.wal.lastEntryId.Offset && EntryIdFromProto(r.maxIdInclusive) != EntryId{}
}

func (r *reverseReader) ReadNext() (*proto.LogEntry, error) {

	if r.closed {
		return nil, ErrorReaderClosed
	}

	index := offsetToIndex(r.nextOffset)
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
	return r.nextOffset != math.MaxUint64
}
