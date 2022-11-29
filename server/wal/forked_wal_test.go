package wal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func dataStr(index int64) string {
	if index%2 == 0 {
		return fmt.Sprintf("data-\"%d\"", index)
	}
	return fmt.Sprintf("data-'%d'", index)
}

func testLog(t *testing.T, path string, opts *Options, N int) {
	logPath := path + strings.Join(strings.Split(t.Name(), "/")[1:], "/")
	l, err := Open(logPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// FirstIndex - should be zero
	n, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected %d, got %d", 0, n)
	}

	// LastIndex - should be zero
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected %d, got %d", 0, n)
	}

	for i := 1; i <= N; i++ {
		// Write - try to append previous index, should fail
		err = l.Write(int64(i-1), nil)
		if err != ErrOutOfOrder {
			t.Fatalf("expected %v, got %v", ErrOutOfOrder, err)
		}
		// Write - append next item
		err = l.Write(int64(i), []byte(dataStr(int64(i))))
		if err != nil {
			t.Fatalf("expected %v, got %v", nil, err)
		}
		// Write - get next item
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("expected %v, got %v", nil, err)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}

	// Read -- should fail, not found
	_, err = l.Read(0)
	if err != ErrNotFound {
		t.Fatalf("expected %v, got %v", ErrNotFound, err)
	}
	// Read -- read back all entries
	for i := 1; i <= N; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}
	// Read -- read back first half entries
	for i := 1; i <= N/2; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}
	// Read -- read second third entries
	for i := N / 3; i <= N/3+N/3; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}

	// Read -- random access
	for _, v := range rand.Perm(N) {
		index := int64(v + 1)
		data, err := l.Read(index)
		if err != nil {
			t.Fatal(err)
		}
		if dataStr(index) != string(data) {
			t.Fatalf("expected %v, got %v", dataStr(index), string(data))
		}
	}

	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected %d, got %d", 1, n)
	}
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(N) {
		t.Fatalf("expected %d, got %d", N, n)
	}

	// Close -- close the log
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Write - try while closed
	err = l.Write(1, nil)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// WriteBatch - try while closed
	err = l.WriteBatch(nil)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// FirstIndex - try while closed
	_, err = l.FirstIndex()
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// LastIndex - try while closed
	_, err = l.LastIndex()
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// Get - try while closed
	_, err = l.Read(0)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// TruncateFront - try while closed
	err = l.TruncateFront(0)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}
	// TruncateBack - try while closed
	err = l.TruncateBack(0)
	if err != ErrClosed {
		t.Fatalf("expected %v, got %v", ErrClosed, err)
	}

	// Open -- reopen log
	l, err = Open(logPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Read -- read back all entries
	for i := 1; i <= N; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d, err=%s", i, err)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}
	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected %d, got %d", 1, n)
	}
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(N) {
		t.Fatalf("expected %d, got %d", N, n)
	}
	// Write -- add 50 more items
	for i := N + 1; i <= N+50; i++ {
		index := int64(i)
		if err := l.Write(index, []byte(dataStr(index))); err != nil {
			t.Fatal(err)
		}
		data, err := l.Read(index)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != dataStr(index) {
			t.Fatalf("expected %v, got %v", dataStr(index), string(data))
		}
	}
	N += 50
	// FirstIndex/LastIndex -- check valid first and last indexes
	n, err = l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected %d, got %d", 1, n)
	}
	n, err = l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(N) {
		t.Fatalf("expected %d, got %d", N, n)
	}
	// Batch -- test batch writes
	b := new(Batch)
	b.Write(1, nil)
	b.Write(2, nil)
	b.Write(3, nil)
	// WriteBatch -- should fail out of order
	err = l.WriteBatch(b)
	if err != ErrOutOfOrder {
		t.Fatalf("expected %v, got %v", ErrOutOfOrder, nil)
	}
	// Clear -- clear the batch
	b.Clear()
	// WriteBatch -- should succeed
	err = l.WriteBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	// Write 100 entries in batches of 10
	for i := 0; i < 10; i++ {
		for i := N + 1; i <= N+10; i++ {
			index := int64(i)
			b.Write(index, []byte(dataStr(index)))
		}
		err = l.WriteBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		N += 10
	}
	// Read -- read back all entries
	for i := 1; i <= N; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}

	// Write -- one entry, so the buffer might be activated
	err = l.Write(int64(N+1), []byte(dataStr(int64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	// Read -- one random read, so there is an opened reader
	data, err := l.Read(int64(N / 2))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != dataStr(int64(N/2)) {
		t.Fatalf("expected %v, got %v", dataStr(int64(N/2)), string(data))
	}

	// TruncateFront -- should fail, out of range
	for _, i := range []int{0, N + 1} {
		index := int64(i)
		if err = l.TruncateFront(index); err != ErrOutOfRange {
			t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
		}
		testFirstLast(t, l, int64(1), int64(N), nil)
	}

	// TruncateBack -- should fail, out of range
	err = l.TruncateFront(0)
	if err != ErrOutOfRange {
		t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
	}
	testFirstLast(t, l, int64(1), int64(N), nil)

	// TruncateFront -- Remove no entries
	if err = l.TruncateFront(1); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, int64(1), int64(N), nil)

	// TruncateFront -- Remove first 80 entries
	if err = l.TruncateFront(81); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, int64(81), int64(N), nil)

	// Write -- one entry, so the buffer might be activated
	err = l.Write(int64(N+1), []byte(dataStr(int64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	testFirstLast(t, l, int64(81), int64(N), nil)

	// Read -- one random read, so there is an opened reader
	data, err = l.Read(int64(N / 2))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != dataStr(int64(N/2)) {
		t.Fatalf("expected %v, got %v", dataStr(int64(N/2)), string(data))
	}

	// TruncateBack -- should fail, out of range
	for _, i := range []int{0, 80} {
		index := int64(i)
		if err = l.TruncateBack(index); err != ErrOutOfRange {
			t.Fatalf("expected %v, got %v", ErrOutOfRange, err)
		}
		testFirstLast(t, l, int64(81), int64(N), nil)
	}

	// TruncateBack -- Remove no entries
	if err = l.TruncateBack(int64(N)); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, int64(81), int64(N), nil)
	// TruncateBack -- Remove last 80 entries
	if err = l.TruncateBack(int64(N - 80)); err != nil {
		t.Fatal(err)
	}
	N -= 80
	testFirstLast(t, l, int64(81), int64(N), nil)

	// Read -- read back all entries
	for i := 81; i <= N; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}

	// Close -- close log after truncating
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}

	// Open -- open log after truncating
	l, err = Open(logPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	testFirstLast(t, l, int64(81), int64(N), nil)

	// Read -- read back all entries
	for i := 81; i <= N; i++ {
		data, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("error while getting %d", i)
		}
		if string(data) != dataStr(int64(i)) {
			t.Fatalf("expected %s, got %s", dataStr(int64(i)), data)
		}
	}

	// TruncateFront -- truncate all entries but one
	if err = l.TruncateFront(int64(N)); err != nil {
		t.Fatal(err)
	}
	testFirstLast(t, l, int64(N), int64(N), nil)

	// Write -- write on entry
	err = l.Write(int64(N+1), []byte(dataStr(int64(N+1))))
	if err != nil {
		t.Fatal(err)
	}
	N++
	testFirstLast(t, l, int64(N-1), int64(N), nil)

	// TruncateBack -- truncate all entries but one
	if err = l.TruncateBack(int64(N - 1)); err != nil {
		t.Fatal(err)
	}
	N--
	testFirstLast(t, l, int64(N), int64(N), nil)

	if err = l.Write(int64(N+1), []byte(dataStr(int64(N+1)))); err != nil {
		t.Fatal(err)
	}
	N++

	assert.NoError(t, l.Sync())
	testFirstLast(t, l, int64(N-1), int64(N), nil)
}

func testFirstLast(t *testing.T, l *Log, expectFirst, expectLast int64, data func(index int64) []byte) {
	t.Helper()
	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	li, err := l.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if fi != expectFirst || li != expectLast {
		t.Fatalf("expected %v/%v, got %v/%v", expectFirst, expectLast, fi, li)
	}
	for i := fi; i <= li; i++ {
		dt1, err := l.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		if data != nil {
			dt2 := data(i)
			if string(dt1) != string(dt2) {
				t.Fatalf("mismatch '%s' != '%s'", dt2, dt1)
			}
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
}

func TestOutliers(t *testing.T) {
	// Create some scenarios where the log has been corrupted, operations
	// fail, or various weirdnesses.
	t.Run("fail-in-memory", func(t *testing.T) {
		if l, err := Open(":memory:", nil); err == nil {
			l.Close()
			t.Fatal("expected error")
		}
	})
	t.Run("fail-not-a-directory", func(t *testing.T) {
		path := t.TempDir()
		if f, err := os.Create(path + "/file"); err != nil {
			t.Fatal(err)
		} else if err := f.Close(); err != nil {
			t.Fatal(err)
		} else if l, err := Open(path+"/file", nil); err == nil {
			l.Close()
			t.Fatal("expected error")
		}
	})
	t.Run("load-with-junk-files", func(t *testing.T) {
		path := t.TempDir()
		// junk should be ignored
		if err := os.MkdirAll(path+"/junk/other1", 0777); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(path + "/junk/other2")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		f, err = os.Create(path + "/junk/" + strings.Repeat("A", 20))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		l, err := Open(path+"/junk", nil)
		if err != nil {
			t.Fatal(err)
		}
		l.Close()
	})

	t.Run("start-marker-file", func(t *testing.T) {
		lpath := t.TempDir() + "/start-marker"
		opts := makeOpts(512, true)
		l := must(Open(lpath, opts)).(*Log)
		defer l.Close()
		for i := int64(1); i <= 100; i++ {
			must(nil, l.Write(i, []byte(dataStr(i))))
		}
		path := l.segments[l.findSegment(35)].path
		firstIndex := l.segments[l.findSegment(35)].index
		must(nil, l.Close())
		data := must(os.ReadFile(path)).([]byte)
		must(nil, os.WriteFile(path+".START", data, 0666))
		l = must(Open(lpath, opts)).(*Log)
		defer l.Close()
		testFirstLast(t, l, firstIndex, 100, nil)

	})

}

func makeOpts(segSize int, noSync bool) *Options {
	opts := *DefaultOptions
	opts.SegmentSize = segSize
	opts.NoSync = noSync
	return &opts
}

// https://github.com/tidwall/wal/issues/1
func TestIssue1(t *testing.T) {
	in := []byte{0, 0, 0, 0, 0, 0, 0, 1, 37, 108, 131, 178, 151, 17, 77, 32,
		27, 48, 23, 159, 63, 14, 240, 202, 206, 151, 131, 98, 45, 165, 151, 67,
		38, 180, 54, 23, 138, 238, 246, 16, 0, 0, 0, 0}
	opts := *DefaultOptions
	l, err := Open(t.TempDir(), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Write(1, in); err != nil {
		t.Fatal(err)
	}
	out, err := l.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(in) != string(out) {
		t.Fatal("data mismatch")
	}
}

func TestSimpleTruncateFront(t *testing.T) {
	opts := &Options{
		NoSync:      true,
		SegmentSize: 100,
	}
	path := t.TempDir()

	l, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		l.Close()
	}()

	makeData := func(index int64) []byte {
		return []byte(fmt.Sprintf("data-%d", index))
	}

	valid := func(t *testing.T, first, last int64) {
		t.Helper()
		index, err := l.FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != first {
			t.Fatalf("expected %v, got %v", first, index)
		}
		index, err = l.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != last {
			t.Fatalf("expected %v, got %v", last, index)
		}
		for i := first; i <= last; i++ {
			data, err := l.Read(i)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(makeData(i)) {
				t.Fatalf("expcted '%s', got '%s'", makeData(i), data)
			}
		}
	}
	validReopen := func(t *testing.T, first, last int64) {
		t.Helper()
		valid(t, first, last)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l, err = Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		valid(t, first, last)
	}
	for i := 1; i <= 100; i++ {
		err := l.Write(int64(i), makeData(int64(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	validReopen(t, 1, 100)

	if err := l.TruncateFront(1); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 100)

	if err := l.TruncateFront(2); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 2, 100)

	if err := l.TruncateFront(4); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 4, 100)

	if err := l.TruncateFront(5); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 5, 100)

	if err := l.TruncateFront(99); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 99, 100)

	if err := l.TruncateFront(100); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 100, 100)

}

func TestSimpleTruncateBack(t *testing.T) {

	opts := &Options{
		NoSync:      true,
		SegmentSize: 100,
	}
	path := t.TempDir()
	l, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		l.Close()
	}()

	makeData := func(index int64) []byte {
		return []byte(fmt.Sprintf("data-%d", index))
	}

	valid := func(t *testing.T, first, last int64) {
		t.Helper()
		index, err := l.FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != first {
			t.Fatalf("expected %v, got %v", first, index)
		}
		index, err = l.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		if index != last {
			t.Fatalf("expected %v, got %v", last, index)
		}
		for i := first; i <= last; i++ {
			data, err := l.Read(i)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != string(makeData(i)) {
				t.Fatalf("expcted '%s', got '%s'", makeData(i), data)
			}
		}
	}
	validReopen := func(t *testing.T, first, last int64) {
		t.Helper()
		valid(t, first, last)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l, err = Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		valid(t, first, last)
	}
	for i := 1; i <= 100; i++ {
		err := l.Write(int64(i), makeData(int64(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	validReopen(t, 1, 100)

	/////////////////////////////////////////////////////////////
	if err := l.TruncateBack(100); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 100)
	if err := l.Write(101, makeData(101)); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 101)

	/////////////////////////////////////////////////////////////
	if err := l.TruncateBack(99); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 99)
	if err := l.Write(100, makeData(100)); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 100)

	if err := l.TruncateBack(94); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 94)

	if err := l.TruncateBack(93); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 93)

	if err := l.TruncateBack(92); err != nil {
		t.Fatal(err)
	}
	validReopen(t, 1, 92)

}

func TestConcurrency(t *testing.T) {

	path := t.TempDir()
	l, err := Open(path, &Options{
		NoSync: true,
		NoCopy: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Write 1000 entries
	for i := 1; i <= 1000; i++ {
		err := l.Write(int64(i), []byte(dataStr(int64(i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Perform 100,000 reads (over 100 threads)
	finished := int32(0)
	maxIndex := int32(1000)
	numReads := int32(0)
	for i := 0; i < 100; i++ {
		go func() {
			defer atomic.AddInt32(&finished, 1)

			for i := 0; i < 1_000; i++ {
				index := rand.Int31n(atomic.LoadInt32(&maxIndex)) + 1
				if _, err := l.Read(int64(index)); err != nil {
					panic(err)
				}
				atomic.AddInt32(&numReads, 1)
			}
		}()
	}

	// continue writing
	for index := maxIndex + 1; atomic.LoadInt32(&finished) < 100; index++ {
		err := l.Write(int64(index), []byte(dataStr(int64(index))))
		if err != nil {
			t.Fatal(err)
		}
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
