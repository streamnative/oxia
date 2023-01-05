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
		writeCount: metrics.NewCounter("oxia_server_kv_write_ops_total",
			"The amount of write operations", "count", labels),
		readBytes: metrics.NewCounter("oxia_server_kv_read",
			"The amount of bytes read from the database", unit.Bytes, labels),
		readCount: metrics.NewCounter("oxia_server_kv_write_ops_total",
			"The amount of write operations", "count", labels),
		writeErrors: metrics.NewCounter("oxia_server_kv_write_errors_total",
			"The count of write operations errors", "count", labels),
		readErrors: metrics.NewCounter("oxia_server_kv_read_errors_total",
			"The count of read operations errors", "count", labels),

		batchSizeHisto: metrics.NewBytesHistogram("oxia_server_kv_batch_size",
			"The size in bytes for a given batch", labels),
		batchCountHisto: metrics.NewCountHistogram("oxia_server_kv_batch_count",
			"The number of operations in a given batch", labels),
	}

	levels := make([]pebble.LevelOptions, 1)
	levels[0].Compression = pebble.ZstdCompression

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
		Levels:     levels,
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

func (b *PebbleBatch) Put(key string, payload []byte) error {
	err := b.b.Set([]byte(key), payload, pebble.NoSync)
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
	} else {
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
	path  string
	files []string
}

type pebbleSnapshotChunk struct {
	ps       *pebbleSnapshot
	filePath string
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
	return os.RemoveAll(ps.path)
}

func (ps *pebbleSnapshot) BasePath() string {
	return ps.path
}

func (ps *pebbleSnapshot) Valid() bool {
	return len(ps.files) > 0
}

func (ps *pebbleSnapshot) Next() bool {
	ps.files = ps.files[1:]
	return ps.Valid()
}

func (ps *pebbleSnapshot) Chunk() SnapshotChunk {
	return &pebbleSnapshotChunk{ps, ps.files[0]}
}

func (psf *pebbleSnapshotChunk) Name() string {
	return psf.filePath
}

func (psf *pebbleSnapshotChunk) Content() ([]byte, error) {
	fp := filepath.Join(psf.ps.path, psf.filePath)
	return os.ReadFile(fp)
}

type pebbleSnapshotLoader struct {
	pf       *PebbleFactory
	shard    uint32
	dbPath   string
	complete bool
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

func (sl *pebbleSnapshotLoader) AddChunk(name string, content []byte) error {
	return os.WriteFile(filepath.Join(sl.dbPath, name), content, 0644)
}

func (sl *pebbleSnapshotLoader) Load() (KV, error) {
	return newKVPebble(sl.pf, sl.shard)
}

func (sl *pebbleSnapshotLoader) Complete() {
	sl.complete = true
}
