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
	wb.Put("a", []byte("0"))
	wb.Put("b", []byte("1"))
	wb.Put("c", []byte("2"))
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
	wb.Put("a", []byte("00"))
	wb.Put("b", []byte("11"))
	wb.Put("d", []byte("22"))
	wb.Delete("c")

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
	wb.Put("/root/a", []byte("a"))
	wb.Put("/root/b", []byte("b"))
	wb.Put("/root/c", []byte("c"))
	wb.Put("/root/d", []byte("d"))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it := kv.KeyRangeScan("/root/a", "/root/c")

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it = kv.KeyRangeScan("/xyz/a", "/xyz/c")
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleSnapshot(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	kv, err := factory.NewKV(1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	wb.Put("/root/a", []byte("a"))
	wb.Put("/root/b", []byte("b"))
	wb.Put("/root/c", []byte("c"))
	wb.Put("/root/d", []byte("d"))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it := kv.Snapshot()

	// Delete / modify some existing keys
	wb = kv.NewWriteBatch()
	wb.Put("/root/a", []byte("aa"))
	wb.Delete("/root/b")
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
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
		wb.Put(k, []byte(k))
	}

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it := kv.KeyRangeScan("/a/b/a/", "/a/b/a//")

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
}
