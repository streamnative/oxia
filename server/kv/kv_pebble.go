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
	"path/filepath"
)

type PebbleFactory struct {
	dataDir string
	cache   *pebble.Cache
	options *KVFactoryOptions
}

func NewPebbleKVFactory(options *KVFactoryOptions) KVFactory {
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

	return &PebbleFactory{
		dataDir: dataDir,
		options: options,

		// Share a single cache instance across the databases for all the shards
		cache: cache,
	}
}

func (p *PebbleFactory) Close() error {
	p.cache.Unref()
	return nil
}

func (p *PebbleFactory) NewKV(shardId uint32) (KV, error) {
	return newKVPebble(p, shardId)
}

////////////////////

type Pebble struct {
	db *pebble.DB
}

func newKVPebble(factory *PebbleFactory, shardId uint32) (KV, error) {
	pb := &Pebble{}

	dbPath := filepath.Join(factory.dataDir, fmt.Sprint("shard-", shardId))

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
				Str("component", "pebble").Logger(),
		},
	}

	if factory.options.InMemory {
		pbOptions.FS = vfs.NewMem()
	}

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

func (p *Pebble) Snapshot() KeyValueIterator {
	s := p.db.NewSnapshot()
	si := s.NewIter(nil)
	si.First()
	return &PebbleSnapshotIterator{
		s:  s,
		si: si,
	}
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
