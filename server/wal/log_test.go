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

/*
The MIT License (MIT)

Copyright (c) 2022 streamnative.io
Copyright (c) 2019 Joshua J Baker

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package wal

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func dataStr(index int64) string {
	if index%2 == 0 {
		return fmt.Sprintf("data-\"%d\"", index)
	}
	return fmt.Sprintf("data-'%d'", index)
}

func testLog(t *testing.T, path string, opts *Options, N int64) {
	logPath := path + strings.Join(strings.Split(t.Name(), "/")[1:], "/")
	l, err := open(logPath, opts)
	assert.NoError(t, err)
	defer l.Close()

	// FirstIndex - should be 0
	n, err := l.FirstIndex()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, n)

	// LastIndex - should be -1
	n, err = l.LastIndex()
	assert.NoError(t, err)
	assert.EqualValues(t, -1, n)

	for i := int64(0); i < N; i++ {
		// Write - append next item
		err = l.Write(i, []byte(dataStr(i)))
		assert.NoError(t, err)

		// Write - get next item
		data, err := l.Read(i)
		assert.NoError(t, err, "Could not read index %d", i)

		assert.Equal(t, dataStr(i), string(data))
	}

	// Read -- should fail, not found
	_, err = l.Read(-1)
	assert.ErrorIs(t, err, ErrNotFound)
	// Read -- read back all entries
	for i := int64(0); i < N; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, dataStr(i), string(data))
	}
	// Read -- read back first half of entries
	for i := int64(0); i < N/2; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, dataStr(i), string(data))
	}
	// Read -- read second third entries
	for i := N / 3; i < N/3+N/3; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, dataStr(i), string(data))
	}

	// Read -- random access
	for _, v := range rand.Perm(int(N)) {
		index := int64(v)
		data, err := l.Read(index)
		assert.NoError(t, err)
		assert.Equal(t, dataStr(index), string(data))
	}

	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, n)

	n, err = l.LastIndex()

	assert.NoError(t, err)
	assert.Equal(t, N-1, n)

	// Close -- close the log
	assert.NoError(t, l.Close())

	// Write - try while closed
	err = l.Write(0, nil)
	assert.ErrorIs(t, err, ErrClosed)
	// FirstIndex - try while closed
	_, err = l.FirstIndex()
	assert.ErrorIs(t, err, ErrClosed)

	// LastIndex - try while closed
	_, err = l.LastIndex()
	assert.ErrorIs(t, err, ErrClosed)

	// Get - try while closed
	_, err = l.Read(0)
	assert.ErrorIs(t, err, ErrClosed)

	// TruncateFront - try while closed
	err = l.TruncateFront(0)
	assert.ErrorIs(t, err, ErrClosed)

	// TruncateBack - try while closed
	err = l.TruncateBack(0)
	assert.ErrorIs(t, err, ErrClosed)

	// open -- reopen log
	l, err = open(logPath, opts)
	assert.NoError(t, err)

	defer l.Close()

	// Read -- read back all entries
	for i := int64(0); i < N; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err, "Could not read index %d", i)
		assert.Equal(t, dataStr(i), string(data))
	}
	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, n)

	n, err = l.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, N-1, n)
	// Write -- add 50 more items
	for i := N; i < N+50; i++ {
		err := l.Write(i, []byte(dataStr(i)))
		assert.NoError(t, err)

		data, err := l.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, dataStr(i), string(data))

	}
	N += 50
	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, n)

	n, err = l.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, N-1, n)

	// Batch -- test batch writes
	b := new(Batch)
	// WriteBatch -- should succeed
	err = l.WriteBatch(b)
	assert.NoError(t, err)

	// Write 100 entries in batches of 10
	for i := 0; i < 10; i++ {
		for i := N; i < N+10; i++ {
			offset := int64(i)
			b.Write(offset, []byte(dataStr(offset)))
		}
		err = l.WriteBatch(b)
		assert.NoError(t, err)
		N += 10
	}
	// Read -- read back all entries
	for i := int64(0); i < N; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)

		assert.Equal(t, dataStr(i), string(data))
	}

	// Write -- one entry, so the buffer might be activated
	err = l.Write(N, []byte(dataStr(N)))
	assert.NoError(t, err)

	N++
	// Read -- one random read, so there is an opened reader
	data, err := l.Read(N / 2)
	assert.NoError(t, err)
	assert.Equal(t, dataStr(N/2), string(data))

	// TruncateFront -- should fail, out of range
	for _, i := range []int64{-1, N} {
		err = l.TruncateFront(i)
		assert.ErrorIs(t, err, ErrOutOfRange)
		testFirstLast(t, l, 0, N-1, nil)
	}

	// TruncateBack -- should fail, out of range
	err = l.TruncateFront(-1)
	assert.ErrorIs(t, err, ErrOutOfRange)
	testFirstLast(t, l, 0, N-1, nil)

	// TruncateFront -- Remove no entries
	err = l.TruncateFront(0)
	assert.NoError(t, err)
	testFirstLast(t, l, 0, N-1, nil)

	// TruncateFront -- Remove first 80 entries
	err = l.TruncateFront(80)
	assert.NoError(t, err)
	testFirstLast(t, l, 80, N-1, nil)

	// Write -- one entry, so the buffer might be activated
	err = l.Write(N, []byte(dataStr(N)))
	assert.NoError(t, err)
	N++
	testFirstLast(t, l, 80, N-1, nil)

	// Read -- one random read, so there is an opened reader
	data, err = l.Read(N / 2)
	assert.NoError(t, err)

	assert.Equal(t, dataStr(N/2), string(data))

	// TruncateBack -- should fail, out of range
	for _, i := range []int{-1, 78} {
		index := int64(i)
		err = l.TruncateBack(index)
		assert.ErrorIs(t, err, ErrOutOfRange)
		testFirstLast(t, l, 80, N-1, nil)
	}

	// TruncateBack -- Remove no entries
	err = l.TruncateBack(N - 1)
	assert.NoError(t, err)
	testFirstLast(t, l, 80, N-1, nil)
	// TruncateBack -- Remove last 80 entries
	err = l.TruncateBack(N - 81)
	assert.NoError(t, err)
	N -= 80
	testFirstLast(t, l, 80, N-1, nil)

	// Read -- read back all entries
	for i := int64(80); i < N; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, string(data), dataStr(i))
	}

	// Close -- close log after truncating
	assert.NoError(t, l.Close())

	// open -- open log after truncating
	l, err = open(logPath, opts)
	assert.NoError(t, err)
	defer l.Close()

	testFirstLast(t, l, 80, N-1, nil)

	// Read -- read back all entries
	for i := int64(80); i < N; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, string(data), dataStr(i))
	}

	// TruncateFront -- truncate all entries but one
	err = l.TruncateFront(N - 1)
	assert.NoError(t, err)
	testFirstLast(t, l, N-1, N-1, nil)

	// Write -- write one entry
	err = l.Write(N, []byte(dataStr(N)))
	assert.NoError(t, err)
	N++
	testFirstLast(t, l, N-2, N-1, nil)

	// TruncateBack -- truncate all entries but one
	err = l.TruncateBack(N - 2)
	assert.NoError(t, err)
	N--
	testFirstLast(t, l, N-1, N-1, nil)

	err = l.Write(N, []byte(dataStr(N)))
	assert.NoError(t, err)
	N++

	assert.NoError(t, l.Sync())
	testFirstLast(t, l, N-2, N-1, nil)
}

func testFirstLast(t *testing.T, l *Log, expectFirst, expectLast int64, data func(index int64) []byte) {
	t.Helper()
	fi, err := l.FirstIndex()
	assert.NoError(t, err)

	li, err := l.LastIndex()
	assert.NoError(t, err)

	assert.Equal(t, expectFirst, fi)
	assert.Equal(t, expectLast, li)

	for i := fi; i <= li; i++ {
		dt1, err := l.Read(i)
		assert.NoError(t, err)

		if data != nil {
			dt2 := data(i)
			assert.Equal(t, string(dt2), string(dt1))
		}
	}

}

func TestLog(t *testing.T) {
	path := t.TempDir()

	t.Run("nil-opts", func(t *testing.T) {
		testLog(t, path, nil, 100)
	})
	t.Run("no-sync", func(t *testing.T) {
		testLog(t, path, makeOpts(512, true), 100)
	})
	t.Run("sync", func(t *testing.T) {
		testLog(t, path, makeOpts(512, false), 100)
	})
	t.Run("in-memory", func(t *testing.T) {
		opts := makeOpts(512, false)
		opts.InMemory = true
		testLog(t, path, makeOpts(512, true), 100)
	})
}

func TestOutliers(t *testing.T) {
	// Create some scenarios where the log has been corrupted, operations
	// fail, or various weirdness.
	t.Run("fail-not-a-directory", func(t *testing.T) {
		path := t.TempDir()
		if f, err := os.Create(path + "/file"); err != nil {
			t.Fatal(err)
		} else if err := f.Close(); err != nil {
			t.Fatal(err)
		} else if l, err := open(path+"/file", nil); err == nil {
			l.Close()
			t.Fatal("expected error")
		}
	})
	t.Run("load-with-junk-files", func(t *testing.T) {
		path := t.TempDir()
		// junk should be ignored
		err := os.MkdirAll(path+"/junk/other1", 0777)
		assert.NoError(t, err)
		f, err := os.Create(path + "/junk/other2")
		assert.NoError(t, err)

		assert.NoError(t, f.Close())
		f, err = os.Create(path + "/junk/" + strings.Repeat("A", 20))
		assert.NoError(t, err)

		assert.NoError(t, f.Close())
		l, err := open(path+"/junk", nil)
		assert.NoError(t, err)

		assert.NoError(t, l.Close())
	})

	t.Run("start-marker-file", func(t *testing.T) {
		lpath := t.TempDir() + "/start-marker"
		opts := makeOpts(512, true)
		l := must(open(lpath, opts)).(*Log)
		defer l.Close()
		for i := int64(0); i < 100; i++ {
			must(nil, l.Write(i, []byte(dataStr(i))))
		}
		path := l.segments[l.findSegment(35)].path
		firstOffset := l.segments[l.findSegment(35)].offset
		must(nil, l.Close())
		data := must(os.ReadFile(path)).([]byte)
		must(nil, os.WriteFile(path+".START", data, 0666))
		l = must(open(lpath, opts)).(*Log)
		defer l.Close()
		testFirstLast(t, l, firstOffset, 99, nil)

	})

}

func makeOpts(segSize int, noSync bool) *Options {
	opts := DefaultOptions()
	opts.SegmentSize = segSize
	opts.NoSync = noSync
	return opts
}

// https://github.com/tidwall/wal/issues/1
func TestIssue1(t *testing.T) {
	in := []byte{0, 0, 0, 0, 0, 0, 0, 1, 37, 108, 131, 178, 151, 17, 77, 32,
		27, 48, 23, 159, 63, 14, 240, 202, 206, 151, 131, 98, 45, 165, 151, 67,
		38, 180, 54, 23, 138, 238, 246, 16, 0, 0, 0, 0}
	l, err := open(t.TempDir(), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Write(0, in); err != nil {
		t.Fatal(err)
	}
	out, err := l.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		t.Fatal("data mismatch")
	}
}

func makeData(index int64) []byte {
	return []byte(fmt.Sprintf("data-%d", index))
}

func valid(t *testing.T, l *Log, first, last int64) {
	t.Helper()
	index, err := l.FirstIndex()
	assert.NoError(t, err)

	assert.Equal(t, first, index)
	index, err = l.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, last, index)

	for i := first; i <= last; i++ {
		data, err := l.Read(i)
		assert.NoError(t, err)

		assert.Equal(t, string(makeData(i)), string(data))
	}
}

func TestSimpleTruncateFront(t *testing.T) {
	opts := &Options{
		NoSync:      true,
		SegmentSize: 100,
	}
	path := t.TempDir()

	l, err := open(path, opts)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, l.Close())
	}()

	validReopen := func(t *testing.T, first, last int64) {
		t.Helper()
		valid(t, l, first, last)
		assert.NoError(t, l.Close())
		l, err = open(path, opts)
		assert.NoError(t, err)
		valid(t, l, first, last)
	}
	for i := int64(0); i < 100; i++ {
		err := l.Write(i, makeData(i))
		assert.NoError(t, err)
	}
	validReopen(t, 0, 99)

	err = l.TruncateFront(0)
	assert.NoError(t, err)

	validReopen(t, 0, 99)

	err = l.TruncateFront(1)
	assert.NoError(t, err)

	validReopen(t, 1, 99)

	err = l.TruncateFront(3)
	assert.NoError(t, err)
	validReopen(t, 3, 99)

	err = l.TruncateFront(5)
	assert.NoError(t, err)
	validReopen(t, 5, 99)

	err = l.TruncateFront(98)
	assert.NoError(t, err)
	validReopen(t, 98, 99)

	err = l.TruncateFront(99)
	assert.NoError(t, err)

	validReopen(t, 99, 99)

	// TODO test complete truncation

}

func TestSimpleTruncateBack(t *testing.T) {

	opts := &Options{
		NoSync:      true,
		SegmentSize: 100,
	}
	path := t.TempDir()
	l, err := open(path, opts)
	assert.NoError(t, err)

	defer func() {
		l.Close()
	}()

	validReopen := func(t *testing.T, first, last int64) {
		t.Helper()
		valid(t, l, first, last)
		assert.NoError(t, l.Close())
		l, err = open(path, opts)
		assert.NoError(t, err)
		valid(t, l, first, last)
	}

	for i := int64(0); i < 100; i++ {
		err = l.Write(i, makeData(int64(i)))
		assert.NoError(t, err)
	}
	validReopen(t, 0, 99)

	/////////////////////////////////////////////////////////////
	err = l.TruncateBack(99)
	assert.NoError(t, err)
	validReopen(t, 0, 99)
	err = l.Write(100, makeData(100))
	assert.NoError(t, err)
	validReopen(t, 0, 100)

	/////////////////////////////////////////////////////////////
	err = l.TruncateBack(98)
	assert.NoError(t, err)
	validReopen(t, 0, 98)
	err = l.Write(99, makeData(99))
	assert.NoError(t, err)
	validReopen(t, 0, 99)

	err = l.TruncateBack(93)
	assert.NoError(t, err)
	validReopen(t, 0, 93)

	err = l.TruncateBack(92)
	assert.NoError(t, err)
	validReopen(t, 0, 92)

	err = l.TruncateBack(91)
	assert.NoError(t, err)
	validReopen(t, 0, 91)

}

func TestConcurrency(t *testing.T) {

	path := t.TempDir()
	l, err := open(path, &Options{
		NoSync: true,
		NoCopy: true,
	})
	assert.NoError(t, err)
	defer l.Close()

	// Write 1000 entries
	for i := int64(0); i < 1000; i++ {
		err := l.Write(i, []byte(dataStr(i)))
		assert.NoError(t, err)
	}

	// Perform 100,000 reads (over 100 threads)
	finished := int32(0)
	maxIndex := int32(1000)
	numReads := int32(0)
	for i := 0; i < 100; i++ {
		go func() {
			defer atomic.AddInt32(&finished, 1)

			for i := 0; i < 1_000; i++ {
				index := rand.Int31n(atomic.LoadInt32(&maxIndex))
				if _, err := l.Read(int64(index)); err != nil {
					panic(err)
				}
				atomic.AddInt32(&numReads, 1)
			}
		}()
	}

	// continue writing
	for index := maxIndex; atomic.LoadInt32(&finished) < 100; index++ {
		err := l.Write(int64(index), []byte(dataStr(int64(index))))
		assert.NoError(t, err)

		atomic.StoreInt32(&maxIndex, index)
	}

	// confirm total reads
	if exp := int32(100_000); numReads != exp {
		t.Fatalf("expected %d reads, but god %d", exp, numReads)
	}
}

func must(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}
