// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"fmt"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric/unit"
	pb "google.golang.org/protobuf/proto"
	"oxia/common/metrics"
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

	appendLatency metrics.LatencyHistogram
	appendBytes   metrics.Counter
	readLatency   metrics.LatencyHistogram
	readBytes     metrics.Counter
	trimOps       metrics.Counter
	readErrors    metrics.Counter
	writeErrors   metrics.Counter
	activeEntries metrics.Gauge
}

func newPersistentWal(shard uint32, options *WalFactoryOptions) (Wal, error) {
	opts := DefaultOptions()
	opts.InMemory = options.InMemory
	walPath := filepath.Join(options.LogDir, fmt.Sprint("shard-", shard))
	log, err := OpenWithShard(walPath, shard, opts)
	if err != nil {
		return nil, err
	}
	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, err
	}

	labels := metrics.LabelsForShard(shard)
	w := &persistentWal{
		shard: shard,
		log:   log,

		appendLatency: metrics.NewLatencyHistogram("oxia_server_wal_append_latency",
			"The time it takes to append entries to the WAL", labels),
		appendBytes: metrics.NewCounter("oxia_server_wal_append",
			"Bytes appended to the WAL", unit.Bytes, labels),
		readLatency: metrics.NewLatencyHistogram("oxia_server_wal_read_latency",
			"The time it takes to read an entry from the WAL", labels),
		readBytes: metrics.NewCounter("oxia_server_wal_read",
			"Bytes read from the WAL", unit.Bytes, labels),
		trimOps: metrics.NewCounter("oxia_server_wal_trim",
			"The number of trim operations happening on the WAL", "count", labels),
		readErrors: metrics.NewCounter("oxia_server_wal_read_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
		writeErrors: metrics.NewCounter("oxia_server_wal_write_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
	}

	w.activeEntries = metrics.NewGauge("oxia_server_wal_entries",
		"The number of active entries in the wal", "count", labels, func() int64 {
			return w.lastOffset - w.firstOffset
		})

	if lastIndex == -1 {
		w.lastOffset = InvalidOffset
		w.firstOffset = InvalidOffset
	} else {
		lastEntry, err := w.readAtIndex(lastIndex)
		if err != nil {
			return nil, err
		}

		w.lastOffset = lastEntry.Offset
		w.firstOffset = log.firstOffset
	}
	return w, nil
}

func (t *persistentWal) readAtIndex(index int64) (*proto.LogEntry, error) {
	timer := t.readLatency.Timer()
	defer timer.Done()

	val, err := t.log.Read(index)
	if err != nil {
		t.readErrors.Inc()
		return nil, err
	}

	entry := &proto.LogEntry{}
	if err = pb.Unmarshal(val, entry); err != nil {
		t.readErrors.Inc()
		return nil, err
	}
	t.readBytes.Add(len(val))
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
		t.writeErrors.Inc()
		return err
	}

	t.firstOffset = t.log.firstOffset
	t.trimOps.Inc()
	return nil
}

func (t *persistentWal) Close() error {
	t.Lock()
	defer t.Unlock()

	t.activeEntries.Unregister()
	return t.log.Close()
}

func (t *persistentWal) Append(entry *proto.LogEntry) error {
	timer := t.appendLatency.Timer()
	defer timer.Done()

	t.Lock()
	defer t.Unlock()

	if err := t.checkNextOffset(entry.Offset); err != nil {
		t.writeErrors.Inc()
		return err
	}

	val, err := pb.Marshal(entry)
	if err != nil {
		t.writeErrors.Inc()
		return err
	}

	err = t.log.Write(entry.Offset, val)
	if err != nil {
		t.writeErrors.Inc()
		return err
	}
	t.lastOffset = entry.Offset
	if t.firstOffset == InvalidOffset {
		t.firstOffset = t.log.firstOffset
	}

	t.appendBytes.Add(len(val))
	return nil
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
		t.writeErrors.Inc()
		return err
	}

	t.lastOffset = InvalidOffset
	t.firstOffset = InvalidOffset
	return nil
}

func (t *persistentWal) TruncateLog(lastSafeOffset int64) (int64, error) {
	t.Lock()
	defer t.Unlock()

	lastIndex, err := t.log.LastIndex()
	if err != nil {
		t.writeErrors.Inc()
		return InvalidOffset, err
	}

	if lastIndex == -1 {
		// The WAL is empty
		return InvalidOffset, nil
	}

	lastSafeIndex := lastSafeOffset
	if err := t.log.TruncateBack(lastSafeIndex); err != nil {
		t.writeErrors.Inc()
		return InvalidOffset, err
	}

	if lastIndex, err = t.log.LastIndex(); err != nil {
		t.writeErrors.Inc()
		return InvalidOffset, err
	}
	val, err := t.log.Read(lastIndex)
	if err != nil {
		t.readErrors.Inc()
		return InvalidOffset, err
	}
	lastEntry := &proto.LogEntry{}
	err = pb.Unmarshal(val, lastEntry)
	if err != nil {
		t.readErrors.Inc()
		return InvalidOffset, err
	}

	if lastEntry.Offset != lastSafeOffset {
		t.writeErrors.Inc()
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
	timer := r.wal.readLatency.Timer()
	defer timer.Done()

	r.Lock()
	defer r.Unlock()

	if r.closed {
		return nil, ErrorReaderClosed
	}

	index := r.nextOffset
	r.wal.RLock()
	defer r.wal.RUnlock()
	entry, err := r.wal.readAtIndex(index)
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

	entry, err := r.wal.readAtIndex(index)
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
