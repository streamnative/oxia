package kv

import (
	"bytes"
	"testing"
)
import "github.com/stretchr/testify/assert"

var testKVOptions = &KVFactoryOptions{
	InMemory:  true,
	CacheSize: 10 * 1024,
}

func TestPebbbleSimple(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	res, closer, err := kv.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	res, closer, err = kv.Get("b")
	assert.NoError(t, err)
	assert.Equal(t, "1", string(res))
	assert.NoError(t, closer.Close())

	res, closer, err = kv.Get("c")
	assert.NoError(t, err)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	res, closer, err = kv.Get("non-existing")
	assert.ErrorIs(t, err, ErrorKeyNotFound)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("00")))
	assert.NoError(t, wb.Put("b", []byte("11")))
	assert.NoError(t, wb.Put("d", []byte("22")))
	assert.NoError(t, wb.Delete("c"))

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	res, closer, err = kv.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "00", string(res))
	assert.NoError(t, closer.Close())

	res, closer, err = kv.Get("b")
	assert.NoError(t, err)
	assert.Equal(t, "11", string(res))
	assert.NoError(t, closer.Close())

	res, closer, err = kv.Get("c")
	assert.ErrorIs(t, err, ErrorKeyNotFound)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	res, closer, err = kv.Get("d")
	assert.NoError(t, err)
	assert.Equal(t, "22", string(res))
	assert.NoError(t, closer.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleRangeScan(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	rb := kv.NewReadBatch()

	it, err := rb.KeyRangeScan("/root/a", "/root/c")
	assert.NoError(t, err)
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = rb.KeyRangeScan("/xyz/a", "/xyz/c")
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, rb.Close())
	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleSnapshot(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it := kv.Snapshot()

	// Delete / modify some existing keys
	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("aa")))
	assert.NoError(t, wb.Delete("/root/b"))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	v, err := it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(v))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	v, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(v))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	v, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "c", string(v))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	v, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "d", string(v))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

var benchKeyA = []byte("/test/aaaaaaaaaaa/bbbbbbbbbbb/cccccccccccc/dddddddddddddd")
var benchKeyB = []byte("/test/aaaaaaaaaaa/bbbbbbbbbbb/ccccccccccccddddddddddddddd")

func BenchmarkStandardCompare(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes.Compare(benchKeyA, benchKeyB)
	}
}

func BenchmarkCompareWithSlash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CompareWithSlash(benchKeyA, benchKeyB)
	}
}

func TestCompareWithSlash(t *testing.T) {
	assert.Equal(t, 0, CompareWithSlash([]byte("aaaaa"), []byte("aaaaa")))
	assert.Equal(t, -1, CompareWithSlash([]byte("aaaaa"), []byte("zzzzz")))
	assert.Equal(t, +1, CompareWithSlash([]byte("bbbbb"), []byte("aaaaa")))

	assert.Equal(t, +1, CompareWithSlash([]byte("aaaaa"), []byte("")))
	assert.Equal(t, -1, CompareWithSlash([]byte(""), []byte("aaaaaa")))
	assert.Equal(t, 0, CompareWithSlash([]byte(""), []byte("")))

	assert.Equal(t, -1, CompareWithSlash([]byte("aaaaa"), []byte("aaaaaaaaaaa")))
	assert.Equal(t, +1, CompareWithSlash([]byte("aaaaaaaaaaa"), []byte("aaa")))

	assert.Equal(t, -1, CompareWithSlash([]byte("a"), []byte("/")))
	assert.Equal(t, +1, CompareWithSlash([]byte("/"), []byte("a")))

	assert.Equal(t, -1, CompareWithSlash([]byte("/aaaa"), []byte("/bbbbb")))
	assert.Equal(t, -1, CompareWithSlash([]byte("/aaaa"), []byte("/aa/a")))
	assert.Equal(t, -1, CompareWithSlash([]byte("/aaaa/a"), []byte("/aaaa/b")))
	assert.Equal(t, +1, CompareWithSlash([]byte("/aaaa/a/a"), []byte("/bbbbbbbbbb")))
	assert.Equal(t, +1, CompareWithSlash([]byte("/aaaa/a/a"), []byte("/aaaa/bbbbbbbbbb")))

	assert.Equal(t, +1, CompareWithSlash([]byte("/a/b/a/a/a"), []byte("/a/b/a/b")))
}

func TestPebbleRangeScanWithSlashOrder(t *testing.T) {
	keys := []string{
		"/a/a/a/zzzzzz",
		"/a/b/a/a/a/a",
		"/a/b/a/c",
		"/a/b/a/a",
		"/a/b/a/a/a",
		"/a/b/a/b",
	}

	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()

	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte(k)))
	}

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	rb := kv.NewReadBatch()

	it, err := rb.KeyRangeScan("/a/b/a/", "/a/b/a//")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/b", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/c", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())
	assert.NoError(t, rb.Close())
	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleGetWithinBatch(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))

	payload, closer, err := wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(payload))
	assert.NoError(t, closer.Close())

	payload, closer, err = wb.Get("non-existent")
	assert.ErrorIs(t, err, ErrorKeyNotFound)
	assert.Nil(t, payload)
	assert.Nil(t, closer)

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Second batch

	wb = kv.NewWriteBatch()
	payload, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(payload))
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Put("a", []byte("00")))

	payload, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "00", string(payload))
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Delete("a"))

	payload, closer, err = wb.Get("a")
	assert.ErrorIs(t, err, ErrorKeyNotFound)
	assert.Nil(t, payload)
	assert.Nil(t, closer)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDurability(t *testing.T) {
	options := &KVFactoryOptions{
		DataDir:   t.TempDir(),
		CacheSize: 10 * 1024,
		InMemory:  false,
	}

	// Open and write a key
	{
		factory := NewPebbleKVFactory(options)
		kv, err := factory.NewKV(1)
		assert.NoError(t, err)

		wb := kv.NewWriteBatch()
		assert.NoError(t, wb.Put("a", []byte("0")))
		assert.NoError(t, wb.Commit())
		assert.NoError(t, wb.Close())

		assert.NoError(t, kv.Close())
		assert.NoError(t, factory.Close())
	}

	// Open again and read it back
	{
		factory := NewPebbleKVFactory(options)
		kv, err := factory.NewKV(1)
		assert.NoError(t, err)

		res, closer, err := kv.Get("a")
		assert.NoError(t, err)
		assert.Equal(t, "0", string(res))
		assert.NoError(t, closer.Close())

		assert.NoError(t, kv.Close())
		assert.NoError(t, factory.Close())
	}
}

func TestPebbbleRangeScanInBatch(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))

	it := wb.KeyRangeScan("/root/a", "/root/c")
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, wb.Put("/root/d", []byte("d")))

	assert.NoError(t, wb.Delete("/root/a"))

	it = wb.KeyRangeScan("/root/a", "/root/c")
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Scan with empty result
	rb := kv.NewReadBatch()
	it, err = rb.KeyRangeScan("/xyz/a", "/xyz/c")
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, rb.Close())
	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDeleteRangeInBatch(t *testing.T) {
	keys := []string{
		"/a/a/a/zzzzzz",
		"/a/b/a/a/a/a",
		"/a/b/a/c",
		"/a/b/a/a",
		"/a/b/a/a/a",
		"/a/b/a/b",
	}

	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()

	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte(k)))
	}

	err = wb.DeleteRange("/a/b/a/", "/a/b/a//")
	assert.NoError(t, err)

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	res, closer, err := kv.Get("/a/b/a/a")
	assert.Nil(t, res)
	assert.Nil(t, closer)
	assert.ErrorIs(t, err, ErrorKeyNotFound)

	res, closer, err = kv.Get("/a/a/a/zzzzzz")
	assert.Equal(t, "/a/a/a/zzzzzz", string(res))
	assert.NoError(t, closer.Close())
	assert.Nil(t, err)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}
