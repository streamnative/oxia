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
	"context"
	"fmt"
	"github.com/pkg/errors"
	pb "google.golang.org/protobuf/proto"
	"os"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/proto"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func (f *factory) NewWal(namespace string, shard int64) (Wal, error) {
	impl, err := newPersistentWal(namespace, shard, f.options)
	return impl, err
}

func (f *factory) Close() error {
	return nil
}

type persistentWal struct {
	sync.RWMutex
	shard       int64
	log         *Log
	firstOffset atomic.Int64
	options     *WalFactoryOptions

	// The last offset appended to the Wal. It might not yet be synced
	lastAppendedOffset atomic.Int64

	// The last offset synced in the Wal.
	lastSyncedOffset atomic.Int64

	ctx         context.Context
	cancel      context.CancelFunc
	syncRequest common.ConditionContext
	syncDone    common.ConditionContext
	lastSyncErr atomic.Pointer[error] // The error from the last sync operation, if any

	appendLatency metrics.LatencyHistogram
	appendBytes   metrics.Counter
	readLatency   metrics.LatencyHistogram
	readBytes     metrics.Counter
	trimOps       metrics.Counter
	readErrors    metrics.Counter
	writeErrors   metrics.Counter
	activeEntries metrics.Gauge
}

func walPath(logDir string, shard int64) string {
	return filepath.Join(logDir, fmt.Sprint("shard-", shard))
}

func newPersistentWal(namespace string, shard int64, options *WalFactoryOptions) (Wal, error) {
	opts := DefaultOptions()
	opts.InMemory = options.InMemory
	opts.NoSync = true // We always sync explicitly

	log, err := OpenWithShard(walPath(options.LogDir, shard), namespace, shard, opts)
	if err != nil {
		return nil, err
	}
	lastIndex, err := log.LastIndex()
	if err != nil {
		return nil, err
	}

	labels := metrics.LabelsForShard(namespace, shard)
	w := &persistentWal{
		shard:   shard,
		log:     log,
		options: options,

		appendLatency: metrics.NewLatencyHistogram("oxia_server_wal_append_latency",
			"The time it takes to append entries to the WAL", labels),
		appendBytes: metrics.NewCounter("oxia_server_wal_append",
			"Bytes appended to the WAL", metrics.Bytes, labels),
		readLatency: metrics.NewLatencyHistogram("oxia_server_wal_read_latency",
			"The time it takes to read an entry from the WAL", labels),
		readBytes: metrics.NewCounter("oxia_server_wal_read",
			"Bytes read from the WAL", metrics.Bytes, labels),
		trimOps: metrics.NewCounter("oxia_server_wal_trim",
			"The number of trim operations happening on the WAL", "count", labels),
		readErrors: metrics.NewCounter("oxia_server_wal_read_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
		writeErrors: metrics.NewCounter("oxia_server_wal_write_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.syncRequest = common.NewConditionContext(w)
	w.syncDone = common.NewConditionContext(w)

	w.activeEntries = metrics.NewGauge("oxia_server_wal_entries",
		"The number of active entries in the wal", "count", labels, func() int64 {
			return w.lastSyncedOffset.Load() - w.firstOffset.Load()
		})

	if lastIndex == -1 {
		w.lastAppendedOffset.Store(InvalidOffset)
		w.lastSyncedOffset.Store(InvalidOffset)
		w.firstOffset.Store(InvalidOffset)
	} else {
		lastEntry, err := w.readAtIndex(lastIndex)
		if err != nil {
			return nil, err
		}

		w.lastAppendedOffset.Store(lastEntry.Offset)
		w.lastSyncedOffset.Store(lastEntry.Offset)
		w.firstOffset.Store(log.firstOffset)
	}

	go common.DoWithLabels(map[string]string{
		"oxia":      "wal-sync",
		"namespace": namespace,
		"shard":     fmt.Sprintf("%d", shard),
	}, w.runSync)

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
	return t.lastSyncedOffset.Load()
}

func (t *persistentWal) FirstOffset() int64 {
	return t.firstOffset.Load()
}

func (t *persistentWal) Trim(firstOffset int64) error {
	t.Lock()
	defer t.Unlock()

	if err := t.log.TruncateFront(firstOffset); err != nil {
		t.writeErrors.Inc()
		return err
	}

	t.firstOffset.Store(t.log.firstOffset)
	t.trimOps.Inc()
	return nil
}

func (t *persistentWal) Close() error {
	t.Lock()
	defer t.Unlock()

	return t.close()
}

func (t *persistentWal) close() error {
	t.cancel()
	t.activeEntries.Unregister()
	return t.log.Close()
}

func (t *persistentWal) Append(entry *proto.LogEntry) error {
	if err := t.AppendAsync(entry); err != nil {
		return err
	}

	return t.Sync(context.Background())
}

func (t *persistentWal) AppendAsync(entry *proto.LogEntry) error {
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
	t.lastAppendedOffset.Store(entry.Offset)
	t.firstOffset.CompareAndSwap(InvalidOffset, t.log.firstOffset)

	t.appendBytes.Add(len(val))
	return nil
}

func (t *persistentWal) runSync() {
	for {
		t.Lock()

		if err := t.syncRequest.Wait(t.ctx); err != nil {
			// Wal is closing, exit the go routine
			t.Unlock()
			return
		}

		t.Unlock()

		lastAppendedOffset := t.lastAppendedOffset.Load()

		if t.lastSyncedOffset.Load() == lastAppendedOffset {
			// We are already at the end, no need to sync
			t.syncDone.Broadcast()
			continue
		}

		if err := t.log.Sync(); err != nil {
			t.lastSyncErr.Store(&err)
			t.writeErrors.Inc()
		} else {
			t.lastSyncedOffset.Store(lastAppendedOffset)
			t.lastSyncErr.Store(nil)
		}

		t.syncDone.Broadcast()
	}
}

func (t *persistentWal) Sync(ctx context.Context) error {
	t.Lock()
	defer t.Unlock()

	// Wait until the currently last appended offset is synced
	lastOffset := t.lastAppendedOffset.Load()

	for lastOffset > t.lastSyncedOffset.Load() {
		t.syncRequest.Signal()

		if err := t.syncDone.Wait(ctx); err != nil {
			return err
		}
	}

	if lastErr := t.lastSyncErr.Load(); lastErr != nil {
		return *lastErr
	}

	return nil
}

func (t *persistentWal) checkNextOffset(nextOffset int64) error {
	if nextOffset < 0 {
		return errors.New(fmt.Sprintf("Invalid next offset. %d should be > 0", nextOffset))
	}

	lastAppendedOffset := t.lastAppendedOffset.Load()
	expectedOffset := lastAppendedOffset + 1

	if lastAppendedOffset != InvalidOffset && nextOffset != expectedOffset {
		return errors.Wrapf(ErrorInvalidNextOffset,
			"%d can not immediately follow %d", nextOffset, lastAppendedOffset)
	}
	return nil
}

func (t *persistentWal) Clear() error {
	t.Lock()
	defer t.Unlock()

	if err := t.log.Clear(); err != nil {
		t.writeErrors.Inc()
		return err
	}

	t.lastAppendedOffset.Store(InvalidOffset)
	t.lastSyncedOffset.Store(InvalidOffset)
	t.firstOffset.Store(InvalidOffset)
	return nil
}

func (t *persistentWal) Delete() error {
	t.Lock()
	defer t.Unlock()

	t.close()
	return os.RemoveAll(walPath(t.options.LogDir, t.shard))
}

func (t *persistentWal) TruncateLog(lastSafeOffset int64) (int64, error) {
	if lastSafeOffset == InvalidOffset {
		if err := t.Clear(); err != nil {
			return InvalidOffset, err
		}
		return t.LastOffset(), nil
	}

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
	t.lastSyncedOffset.Store(lastEntry.Offset)
	return lastEntry.Offset, nil
}

func (t *persistentWal) NewReader(after int64) (WalReader, error) {
	firstOffset := after + 1

	if firstOffset < t.FirstOffset() {
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
	r := &reverseReader{reader{
		wal:        t,
		nextOffset: t.LastOffset(),
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

	return r.nextOffset <= r.wal.LastOffset()
}

func (r *reverseReader) ReadNext() (*proto.LogEntry, error) {
	if r.closed {
		return nil, ErrorReaderClosed
	}

	entry, err := r.wal.readAtIndex(r.nextOffset)
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

	firstOffset := r.wal.FirstOffset()
	return firstOffset != InvalidOffset && r.nextOffset != (firstOffset-1)
}
