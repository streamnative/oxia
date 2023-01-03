package kv

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

type PebbleFactory struct {
	dataDir string
	cache   *pebble.Cache
	options *KVFactoryOptions
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
}

func newKVPebble(factory *PebbleFactory, shardId uint32) (KV, error) {
	pb := &Pebble{
		shardId: shardId,
		dataDir: factory.dataDir,
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
	return pb, nil
}

func (p *Pebble) Close() error {
	if err := p.db.Flush(); err != nil {
		return err
	}
	return p.db.Close()
}

func (p *Pebble) Flush() error {
	return p.db.Flush()
}

func (p *Pebble) NewWriteBatch() WriteBatch {
	return &PebbleBatch{p.db.NewIndexedBatch()}
}

func (p *Pebble) Get(key string) ([]byte, io.Closer, error) {
	value, closer, err := p.db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrorKeyNotFound
	}
	return value, closer, err
}

func (p *Pebble) KeyRangeScan(lowerBound, upperBound string) KeyIterator {
	pbit := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	pbit.SeekGE([]byte(lowerBound))
	return &PebbleIterator{pbit}
}

func (p *Pebble) RangeScan(lowerBound, upperBound string) KeyValueIterator {
	pbit := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	pbit.SeekGE([]byte(lowerBound))
	return &PebbleIterator{pbit}
}

func (p *Pebble) Snapshot() (Snapshot, error) {
	return newPebbleSnapshot(p)
}

/// Batch wrapper methods

type PebbleBatch struct {
	b *pebble.Batch
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
	return &PebbleIterator{pbit}
}

func (b *PebbleBatch) Close() error {
	return b.b.Close()
}

func (b *PebbleBatch) Put(key string, payload []byte) error {
	return b.b.Set([]byte(key), payload, pebble.NoSync)
}

func (b *PebbleBatch) Delete(key string) error {
	return b.b.Delete([]byte(key), pebble.NoSync)
}

func (b *PebbleBatch) Get(key string) ([]byte, io.Closer, error) {
	value, closer, err := b.b.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrorKeyNotFound
	}
	return value, closer, err
}

func (b *PebbleBatch) Commit() error {
	return b.b.Commit(pebble.NoSync)
}

/// Iterator wrapper methods

type PebbleIterator struct {
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
	return p.pi.ValueAndErr()
}

/// Snapshot Iterator wrapper methods

type PebbleSnapshotIterator struct {
	s  *pebble.Snapshot
	si *pebble.Iterator
}

func (p *PebbleSnapshotIterator) Close() error {
	err := p.si.Close()
	if err != nil {
		return err
	}
	return p.s.Close()
}

func (p *PebbleSnapshotIterator) Valid() bool {
	return p.si.Valid()
}

func (p *PebbleSnapshotIterator) Key() string {
	return string(p.si.Key())
}

func (p *PebbleSnapshotIterator) Next() bool {
	return p.si.Next()
}

func (p *PebbleSnapshotIterator) Value() ([]byte, error) {
	return p.si.ValueAndErr()
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
