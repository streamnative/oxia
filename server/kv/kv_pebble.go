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

package kv

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/metric/unit"
	"go.uber.org/multierr"
	"io"
	"os"
	"oxia/common"
	"oxia/common/metrics"
	"path/filepath"
	"sync/atomic"
	"time"
)

type PebbleFactory struct {
	dataDir string
	cache   *pebble.Cache
	options *KVFactoryOptions

	gaugeCacheSize metrics.Gauge
}

func NewPebbleKVFactory(options *KVFactoryOptions) (KVFactory, error) {
	if options == nil {
		options = DefaultKVFactoryOptions
	}
	cacheSize := options.CacheSize
	if cacheSize == 0 {
		cacheSize = DefaultKVFactoryOptions.CacheSize
	}

	dataDir := options.DataDir
	if dataDir == "" {
		dataDir = DefaultKVFactoryOptions.DataDir
	}

	cache := pebble.NewCache(cacheSize)

	pf := &PebbleFactory{
		dataDir: dataDir,
		options: options,

		// Share a single cache instance across the databases for all the shards
		cache: cache,

		gaugeCacheSize: metrics.NewGauge("oxia_server_kv_pebble_max_cache_size",
			"The max size configured for the Pebble block cache in bytes",
			unit.Bytes, map[string]any{}, func() int64 {
				return options.CacheSize
			}),
	}

	// Cleanup leftover snapshots from previous runs
	if err := pf.cleanupSnapshots(); err != nil {
		return nil, errors.Wrap(err, "failed to delete database snapshots")
	}

	return pf, nil
}

func (p *PebbleFactory) cleanupSnapshots() error {
	snapshotsPath := filepath.Join(p.dataDir, "snapshots")
	_, err := os.Stat(snapshotsPath)

	if err == nil {
		return os.RemoveAll(snapshotsPath)
	} else if os.IsNotExist(err) {
		// Snapshot directory does not exist, nothing to do
		return nil
	}

	return err
}

func (p *PebbleFactory) Close() error {
	p.gaugeCacheSize.Unregister()
	p.cache.Unref()
	return nil
}

func (p *PebbleFactory) NewKV(shardId uint32) (KV, error) {
	return newKVPebble(p, shardId)
}

func (p *PebbleFactory) NewSnapshotLoader(shardId uint32) (SnapshotLoader, error) {
	return newPebbleSnapshotLoader(p, shardId)
}

func (p *PebbleFactory) getKVPath(shard uint32) string {
	return filepath.Join(p.dataDir, fmt.Sprint("shard-", shard))
}

////////////////////

type Pebble struct {
	shardId         uint32
	dataDir         string
	db              *pebble.DB
	snapshotCounter atomic.Int64

	dbMetrics          func() *pebble.Metrics
	gauges             []metrics.Gauge
	batchCommitLatency metrics.LatencyHistogram

	writeBytes  metrics.Counter
	writeCount  metrics.Counter
	readBytes   metrics.Counter
	readCount   metrics.Counter
	readLatency metrics.LatencyHistogram
	writeErrors metrics.Counter
	readErrors  metrics.Counter

	batchSizeHisto  metrics.Histogram
	batchCountHisto metrics.Histogram
}

func newKVPebble(factory *PebbleFactory, shardId uint32) (KV, error) {
	labels := metrics.LabelsForShard(shardId)
	pb := &Pebble{
		shardId: shardId,
		dataDir: factory.dataDir,

		batchCommitLatency: metrics.NewLatencyHistogram("oxia_server_kv_batch_commit_latency",
			"The latency for committing a batch into the database", labels),
		readLatency: metrics.NewLatencyHistogram("oxia_server_kv_read_latency",
			"The latency for reading a value from the database", labels),
		writeBytes: metrics.NewCounter("oxia_server_kv_write",
			"The amount of bytes written into the database", unit.Bytes, labels),
		writeCount: metrics.NewCounter("oxia_server_kv_write_ops",
			"The amount of write operations", "count", labels),
		readBytes: metrics.NewCounter("oxia_server_kv_read",
			"The amount of bytes read from the database", unit.Bytes, labels),
		readCount: metrics.NewCounter("oxia_server_kv_write_ops",
			"The amount of write operations", "count", labels),
		writeErrors: metrics.NewCounter("oxia_server_kv_write_errors",
			"The count of write operations errors", "count", labels),
		readErrors: metrics.NewCounter("oxia_server_kv_read_errors",
			"The count of read operations errors", "count", labels),

		batchSizeHisto: metrics.NewBytesHistogram("oxia_server_kv_batch_size",
			"The size in bytes for a given batch", labels),
		batchCountHisto: metrics.NewCountHistogram("oxia_server_kv_batch_count",
			"The number of operations in a given batch", labels),
	}

	pbOptions := &pebble.Options{
		Cache: factory.cache,
		Comparer: &pebble.Comparer{
			Compare:            CompareWithSlash,
			Equal:              pebble.DefaultComparer.Equal,
			AbbreviatedKey:     pebble.DefaultComparer.AbbreviatedKey,
			FormatKey:          pebble.DefaultComparer.FormatKey,
			FormatValue:        pebble.DefaultComparer.FormatValue,
			Separator:          pebble.DefaultComparer.Separator,
			Split:              pebble.DefaultComparer.Split,
			Successor:          pebble.DefaultComparer.Successor,
			ImmediateSuccessor: pebble.DefaultComparer.ImmediateSuccessor,
			Name:               "oxia-slash-spans",
		},
		FS:         vfs.Default,
		DisableWAL: true,
		Logger: &PebbleLogger{
			log.With().
				Str("component", "pebble").
				Uint32("shard", shardId).
				Logger(),
		},
	}

	if factory.options.InMemory {
		pbOptions.FS = vfs.NewMem()
	}

	dbPath := factory.getKVPath(shardId)
	db, err := pebble.Open(dbPath, pbOptions)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open database at %s", dbPath)
	}

	pb.db = db

	// Cache the calls to db.Metrics() which are common to all the gauges
	pb.dbMetrics = common.Memoize(func() *pebble.Metrics {
		return pb.db.Metrics()
	}, 5*time.Second)

	pb.gauges = []metrics.Gauge{
		metrics.NewGauge("oxia_server_kv_pebble_block_cache_used",
			"The size of the block cache used by a given db shard",
			unit.Bytes, labels, func() int64 {
				return pb.dbMetrics().BlockCache.Size
			}),
		metrics.NewGauge("oxia_server_kv_pebble_block_cache_hits",
			"The number of hits in the block cache",
			"count", labels, func() int64 {
				return pb.dbMetrics().BlockCache.Hits
			}),
		metrics.NewGauge("oxia_server_kv_pebble_block_cache_misses",
			"The number of misses in the block cache",
			"count", labels, func() int64 {
				return pb.dbMetrics().BlockCache.Misses
			}),
		metrics.NewGauge("oxia_server_kv_pebble_read_iterators",
			"The number of iterators open",
			"value", labels, func() int64 {
				return pb.dbMetrics().TableIters
			}),

		metrics.NewGauge("oxia_server_kv_pebble_compactions_total",
			"The number of compactions operations",
			"count", labels, func() int64 {
				return pb.dbMetrics().Compact.Count
			}),
		metrics.NewGauge("oxia_server_kv_pebble_compaction_debt",
			"The estimated number of bytes that need to be compacted",
			unit.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().Compact.EstimatedDebt)
			}),
		metrics.NewGauge("oxia_server_kv_pebble_flush_total",
			"The total number of db flushes",
			"count", labels, func() int64 {
				return pb.dbMetrics().Flush.Count
			}),
		metrics.NewGauge("oxia_server_kv_pebble_flush",
			"The total amount of bytes flushed into the db",
			unit.Bytes, labels, func() int64 {
				return pb.dbMetrics().Flush.WriteThroughput.Bytes
			}),
		metrics.NewGauge("oxia_server_kv_pebble_memtable_size",
			"The size of the memtable",
			unit.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().MemTable.Size)
			}),

		metrics.NewGauge("oxia_server_kv_pebble_disk_space",
			"The total size of all the db files",
			unit.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().DiskSpaceUsage())
			}),
		metrics.NewGauge("oxia_server_kv_pebble_num_files_total",
			"The total number of files for the db",
			"count", labels, func() int64 {
				return pb.dbMetrics().Total().NumFiles
			}),
		metrics.NewGauge("oxia_server_kv_pebble_read",
			"The total amount of bytes read at this db level",
			unit.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().Total().BytesRead)
			}),
		metrics.NewGauge("oxia_server_kv_pebble_write_amplification_percent",
			"The total amount of bytes read at this db level",
			"percent", labels, func() int64 {
				t := pb.dbMetrics().Total()
				return int64(t.WriteAmp() * 100)
			}),
	}

	// Add the per-LSM level metrics
	for i := 0; i < 7; i++ {
		level := i
		labels := map[string]any{
			"shard": shardId,
			"level": level,
		}

		pb.gauges = append(pb.gauges,
			metrics.NewGauge("oxia_server_kv_pebble_per_level_num_files",
				"The total number of files at this db level",
				"count", labels, func() int64 {
					return pb.dbMetrics().Levels[level].NumFiles
				}),
			metrics.NewGauge("oxia_server_kv_pebble_per_level_size",
				"The total size in bytes of the files at this db level",
				unit.Bytes, labels, func() int64 {
					return pb.dbMetrics().Levels[level].Size
				}),
			metrics.NewGauge("oxia_server_kv_pebble_per_level_read",
				"The total amount of bytes read at this db level",
				unit.Bytes, labels, func() int64 {
					return int64(pb.dbMetrics().Levels[level].BytesRead)
				}),
		)
	}

	return pb, nil
}

func (p *Pebble) Close() error {
	for _, g := range p.gauges {
		g.Unregister()
	}

	if err := p.db.Flush(); err != nil {
		return err
	}
	return p.db.Close()
}

func (p *Pebble) Flush() error {
	return p.db.Flush()
}

func (p *Pebble) NewWriteBatch() WriteBatch {
	return &PebbleBatch{p: p, b: p.db.NewIndexedBatch()}
}

func (p *Pebble) Get(key string) ([]byte, io.Closer, error) {
	value, closer, err := p.db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrorKeyNotFound
	} else {
		p.readErrors.Inc()
	}
	return value, closer, err
}

func (p *Pebble) KeyRangeScan(lowerBound, upperBound string) KeyIterator {
	pbit := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	pbit.SeekGE([]byte(lowerBound))
	return &PebbleIterator{p, pbit}
}

func (p *Pebble) KeyRangeScanReverse(lowerBound, upperBound string) ReverseKeyIterator {
	pbit := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	pbit.Last()
	return &PebbleReverseIterator{p, pbit}
}

func (p *Pebble) RangeScan(lowerBound, upperBound string) KeyValueIterator {
	pbit := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	pbit.SeekGE([]byte(lowerBound))
	return &PebbleIterator{p, pbit}
}

func (p *Pebble) Snapshot() (Snapshot, error) {
	return newPebbleSnapshot(p)
}

/// Batch wrapper methods

type PebbleBatch struct {
	p *Pebble
	b *pebble.Batch
}

func (b *PebbleBatch) Count() int {
	return int(b.b.Count())
}

func (b *PebbleBatch) Size() int {
	return b.b.Len()
}

func (b *PebbleBatch) DeleteRange(lowerBound, upperBound string) error {
	return b.b.DeleteRange([]byte(lowerBound), []byte(upperBound), pebble.NoSync)
}

func (b *PebbleBatch) KeyRangeScan(lowerBound, upperBound string) KeyIterator {
	pbit := b.b.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	pbit.SeekGE([]byte(lowerBound))
	return &PebbleIterator{b.p, pbit}
}

func (b *PebbleBatch) Close() error {
	return b.b.Close()
}

func (b *PebbleBatch) Put(key string, value []byte) error {
	err := b.b.Set([]byte(key), value, pebble.NoSync)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

func (b *PebbleBatch) Delete(key string) error {
	err := b.b.Delete([]byte(key), pebble.NoSync)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

func (b *PebbleBatch) Get(key string) ([]byte, io.Closer, error) {
	value, closer, err := b.b.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrorKeyNotFound
	} else if err != nil {
		b.p.readErrors.Inc()
	}
	return value, closer, err
}

func (b *PebbleBatch) Commit() error {
	b.p.writeCount.Add(b.Count())
	b.p.writeBytes.Add(b.Size())
	b.p.batchCountHisto.Record(b.Count())
	b.p.batchSizeHisto.Record(b.Size())

	timer := b.p.batchCommitLatency.Timer()
	defer timer.Done()

	err := b.b.Commit(pebble.NoSync)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

/// Iterator wrapper methods

type PebbleIterator struct {
	p  *Pebble
	pi *pebble.Iterator
}

func (p *PebbleIterator) Close() error {
	return p.pi.Close()
}

func (p *PebbleIterator) Valid() bool {
	return p.pi.Valid()
}

func (p *PebbleIterator) Key() string {
	return string(p.pi.Key())
}

func (p *PebbleIterator) Next() bool {
	return p.pi.Next()
}

func (p *PebbleIterator) Value() ([]byte, error) {
	res, err := p.pi.ValueAndErr()
	if err != nil {
		p.p.readErrors.Inc()
	}
	return res, err
}

/// Iterator wrapper methods

type PebbleReverseIterator struct {
	p  *Pebble
	pi *pebble.Iterator
}

func (p *PebbleReverseIterator) Close() error {
	return p.pi.Close()
}

func (p *PebbleReverseIterator) Valid() bool {
	return p.pi.Valid()
}

func (p *PebbleReverseIterator) Key() string {
	return string(p.pi.Key())
}

func (p *PebbleReverseIterator) Prev() bool {
	return p.pi.Prev()
}

func (p *PebbleReverseIterator) Value() ([]byte, error) {
	res, err := p.pi.ValueAndErr()
	if err != nil {
		p.p.readErrors.Inc()
	}
	return res, err
}

/// Pebble logger wrapper

type PebbleLogger struct {
	zl zerolog.Logger
}

func (pl *PebbleLogger) Infof(format string, args ...interface{}) {
	pl.zl.Info().Msgf(format, args...)
}

func (pl *PebbleLogger) Fatalf(format string, args ...interface{}) {
	pl.zl.Fatal().Msgf(format, args...)
}

/// Custom comparator function

func CompareWithSlash(a, b []byte) int {
	for len(a) > 0 && len(b) > 0 {
		idxA, idxB := bytes.IndexByte(a, '/'), bytes.IndexByte(b, '/')
		if idxA < 0 && idxB < 0 {
			return bytes.Compare(a, b)
		} else if idxA < 0 && idxB >= 0 {
			return -1
		} else if idxA >= 0 && idxB < 0 {
			return +1
		}

		// At this point, both slices have '/'
		spanA, spanB := a[:idxA], b[:idxB]

		spanRes := bytes.Compare(spanA, spanB)
		if spanRes != 0 {
			return spanRes
		}

		a, b = a[idxA+1:], b[idxB+1:]
	}

	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return +1
	} else {
		return 0
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///// Snapshots
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type pebbleSnapshot struct {
	path       string
	files      []string
	chunkCount int32
	chunkIndex int32
	file       *os.File
}

type pebbleSnapshotChunk struct {
	name       string
	index      int32
	totalCount int32
	content    []byte
}

func newPebbleSnapshot(p *Pebble) (Snapshot, error) {
	ps := &pebbleSnapshot{
		path: filepath.Join(p.dataDir, "snapshots",
			fmt.Sprintf("shard-%d", p.shardId),
			fmt.Sprintf("snapshot-%d", p.snapshotCounter.Add(1))),
	}

	// Flush the DB to ensure the snapshot content is as close as possible to
	// the last committed entry
	if err := p.db.Flush(); err != nil {
		return nil, err
	}

	if err := p.db.Checkpoint(ps.path); err != nil {
		return nil, err
	}

	dirEntries, err := os.ReadDir(ps.path)
	if err != nil {
		return nil, err
	}

	for _, de := range dirEntries {
		if !de.IsDir() {
			ps.files = append(ps.files, de.Name())
		}
	}

	return ps, nil
}

func (ps *pebbleSnapshot) Close() error {
	var err error
	if ps.file != nil {
		err = ps.file.Close()
	}
	return multierr.Combine(err, os.RemoveAll(ps.path))
}

func (ps *pebbleSnapshot) BasePath() string {
	return ps.path
}

func (ps *pebbleSnapshot) Valid() bool {
	return len(ps.files) > 0
}

func (ps *pebbleSnapshot) Next() bool {
	ps.chunkIndex += 1
	if ps.chunkIndex == ps.chunkCount {
		ps.chunkIndex = 0
		ps.files = ps.files[1:]
	}
	return ps.Valid()
}

func (ps *pebbleSnapshot) Chunk() (SnapshotChunk, error) {
	content, err := ps.NextChunkContent()
	if err != nil {
		return nil, err
	}
	return &pebbleSnapshotChunk{
		ps.files[0],
		int32(ps.chunkIndex),
		int32(ps.chunkCount),
		content}, nil
}

func (psf *pebbleSnapshotChunk) Name() string {
	return psf.name
}

func (psf *pebbleSnapshotChunk) Content() []byte {
	return psf.content
}

func (psf *pebbleSnapshotChunk) Index() int32 {
	return psf.index
}

func (psf *pebbleSnapshotChunk) TotalCount() int32 {
	return psf.totalCount
}

func (ps *pebbleSnapshot) NextChunkContent() ([]byte, error) {
	if ps.chunkIndex == 0 {
		var err error
		filePath := filepath.Join(ps.path, ps.files[0])
		stat, err := os.Stat(filePath)
		if err != nil {
			return nil, err
		}
		fileSize := stat.Size()
		ps.chunkCount = int32(fileSize / MaxSnapshotChunkSize)
		if fileSize%MaxSnapshotChunkSize != 0 {
			ps.chunkCount += 1
		}
		if ps.chunkCount == 0 {
			// empty file
			ps.chunkCount = 1
		}

		ps.file, err = os.Open(filePath)
		if err != nil {
			return nil, err
		}

	}

	_, err := ps.file.Seek(int64(ps.chunkIndex)*MaxSnapshotChunkSize, io.SeekStart)
	if err != nil {
		return nil, err
	}
	content := make([]byte, MaxSnapshotChunkSize)
	byteCount, err := io.ReadFull(ps.file, content)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	if int64(byteCount) < MaxSnapshotChunkSize {
		content = content[:byteCount]

		err = ps.file.Close()
		if err != nil {
			return nil, err
		}
		ps.file = nil
	}
	return content, nil
}

type pebbleSnapshotLoader struct {
	pf       *PebbleFactory
	shard    uint32
	dbPath   string
	complete bool
	file     *os.File
}

func newPebbleSnapshotLoader(pf *PebbleFactory, shard uint32) (SnapshotLoader, error) {
	sl := &pebbleSnapshotLoader{
		pf:     pf,
		shard:  shard,
		dbPath: pf.getKVPath(shard),
	}

	if err := os.RemoveAll(sl.dbPath); err != nil {
		return nil, errors.Wrap(err, "failed to remove existing database")
	}

	if err := os.MkdirAll(sl.dbPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create database dir")
	}

	return sl, nil
}

func (sl *pebbleSnapshotLoader) Close() error {
	if sl.complete {
		return nil
	}

	// If we failed to successfully load, remove all intermediate files
	return os.RemoveAll(sl.dbPath)
}

func (sl *pebbleSnapshotLoader) AddChunk(fileName string, chunkIndex int32, chunkCount int32, content []byte) error {
	var err error
	if chunkIndex == 0 {
		if sl.file != nil {
			return errors.Errorf("Inconsistent snapshot: previous file not finished")
		}
		sl.file, err = os.OpenFile(filepath.Join(sl.dbPath, fileName), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
	}
	for len(content) > 0 {
		w, err := sl.file.Write(content)
		if err != nil {
			return err
		}
		content = content[w:]
	}
	if chunkIndex == chunkCount-1 {
		err = sl.file.Close()
		sl.file = nil
		if err != nil {
			return err
		}
	}

	return nil
}

func (sl *pebbleSnapshotLoader) Load() (KV, error) {
	return newKVPebble(sl.pf, sl.shard)
}

func (sl *pebbleSnapshotLoader) Complete() {
	sl.complete = true
}
