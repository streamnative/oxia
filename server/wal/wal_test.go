package wal

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"testing"
	"time"
)

const shard = uint32(100)

type walFactoryFactory interface {
	NewWalFactory(t *testing.T) WalFactory
	Name() string
	Persistent() bool
}

type inMemoryWalFactoryFactory struct{}
type persistentWalFactoryFactory struct{}

func (_ *inMemoryWalFactoryFactory) NewWalFactory(_ *testing.T) WalFactory {
	return NewInMemoryWalFactory()
}

func (_ *inMemoryWalFactoryFactory) Name() string {
	return "InMemory/"
}

func (_ *inMemoryWalFactoryFactory) Persistent() bool {
	return false
}

func (_ *persistentWalFactoryFactory) NewWalFactory(t *testing.T) WalFactory {
	dir := t.TempDir()
	f := NewWalFactory(&WalFactoryOptions{dir, false})
	return f
}

func (_ *persistentWalFactoryFactory) Name() string {
	return "Persistent/"
}

func (_ *persistentWalFactoryFactory) Persistent() bool {
	return true
}

var walFF walFactoryFactory = &inMemoryWalFactoryFactory{}

func TestWal(t *testing.T) {
	for _, f := range []walFactoryFactory{&inMemoryWalFactoryFactory{}, &persistentWalFactoryFactory{}} {
		walFF = f
		t.Run(f.Name()+"FactoryNewWal", FactoryNewWal)
		t.Run(f.Name()+"Append", Append)
		t.Run(f.Name()+"AppendAsync", AppendAsync)
		t.Run(f.Name()+"Truncate", Truncate)
		t.Run(f.Name()+"Clear", Clear)
		t.Run(f.Name()+"Trim", Trim)
		if f.Persistent() {
			t.Run(f.Name()+"Reopen", Reopen)
		}
	}
}

func createWal(t *testing.T) (WalFactory, Wal) {
	f := walFF.NewWalFactory(t)
	w, err := f.NewWal(shard)
	assert.NoError(t, err)

	return f, w

}

func assertReaderReads(t *testing.T, r WalReader, entries []string) {
	for i := 0; i < len(entries); i++ {
		assert.True(t, r.HasNext())
		e, err := r.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, entries[i], string(e.Value))
	}
	assert.False(t, r.HasNext())
}

func assertReaderReadsEventually(t *testing.T, r WalReader, entries []string) chan error {
	ch := make(chan error)
	go func() {
		for i := 0; i < len(entries); i++ {
			assert.Eventually(t, r.HasNext,
				100*time.Millisecond,
				10*time.Millisecond,
				fmt.Sprintf("did not read all expected entries: only read %d/%d", i, len(entries)))
			e, err := r.ReadNext()
			if err != nil {
				ch <- err
				return
			}
			if entries[i] != string(e.Value) {
				ch <- errors.Errorf("entry #%d not equal. Expected '%s', got '%s'", i, entries[i], string(e.Value))
			}
		}
		ch <- nil
	}()
	return ch
}

func FactoryNewWal(t *testing.T) {
	f, w := createWal(t)
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assert.False(t, rr.HasNext())

	assert.NoError(t, rr.Close())
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assert.False(t, fr.HasNext())
	assert.NoError(t, fr.Close())
	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func Append(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C"}
	for i, s := range input {
		err := w.Append(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	// Read entries backwards
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assertReaderReads(t, rr, []string{"C", "B", "A"})
	assert.NoError(t, rr.Close())

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())

	// Read with forward reader from the middle
	fr, err = w.NewReader(1)
	assert.NoError(t, err)
	assertReaderReads(t, fr, []string{"C"})
	assert.NoError(t, fr.Close())

	// Read with forward reader waiting for new entries
	fr, err = w.NewReader(0)
	assert.NoError(t, err)
	ch := assertReaderReadsEventually(t, fr, []string{"B", "C", "D"})

	err = w.Append(&proto.LogEntry{
		Epoch:  1,
		Offset: int64(3),
		Value:  []byte("D"),
	})
	assert.NoError(t, err)
	assert.NoError(t, <-ch)

	assert.NoError(t, fr.Close())

	// Append invalid offset
	err = w.Append(&proto.LogEntry{
		Epoch:  1,
		Offset: int64(88),
		Value:  []byte("E"),
	})
	assert.ErrorIs(t, err, ErrorInvalidNextOffset)

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func AppendAsync(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C"}
	for i, s := range input {
		err := w.AppendAsync(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	assert.Equal(t, InvalidOffset, w.LastOffset())

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assert.False(t, fr.HasNext())

	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assert.False(t, rr.HasNext())

	assert.NoError(t, w.Sync(context.Background()))

	assert.EqualValues(t, 2, w.LastOffset())

	fr, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assert.True(t, fr.HasNext())

	rr, err = w.NewReverseReader()
	assert.NoError(t, err)
	assert.True(t, rr.HasNext())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func Truncate(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C", "D", "E"}
	for i, s := range input {
		err := w.Append(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	headIndex, err := w.TruncateLog(2)
	assert.NoError(t, err)

	assert.EqualValues(t, 2, headIndex)

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input[:3])
	assert.NoError(t, fr.Close())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func Reopen(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C", "D", "E"}
	for i, s := range input {
		err := w.Append(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	err := w.Close()
	assert.NoError(t, err)

	w, err = f.NewWal(shard)
	assert.NoError(t, err)

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func Clear(t *testing.T) {
	f, w := createWal(t)

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	for i := 0; i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	assert.NoError(t, w.Clear())

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	for i := 250; i < 300; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))

		assert.EqualValues(t, 250, w.FirstOffset())
		assert.EqualValues(t, i, w.LastOffset())
	}

	// Test forward reader
	r, err := w.NewReader(249)
	assert.NoError(t, err)

	for i := 250; i < 300; i++ {
		assert.True(t, r.HasNext())
		le, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	// Test reverse reader
	r, err = w.NewReverseReader()
	assert.NoError(t, err)

	for i := 299; i >= 250; i-- {
		assert.True(t, r.HasNext())
		le, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func Trim(t *testing.T) {
	f, w := createWal(t)

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	for i := 0; i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Epoch:  1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	assert.NoError(t, w.Trim(50))

	assert.EqualValues(t, 50, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	// Test forward reader
	r, err := w.NewReader(49)
	assert.NoError(t, err)

	for i := 50; i < 100; i++ {
		assert.True(t, r.HasNext())
		le, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	// Test reverse reader
	r, err = w.NewReverseReader()
	assert.NoError(t, err)

	for i := 99; i >= 50; i-- {
		assert.True(t, r.HasNext())
		le, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	// Test reading a trimmed offset
	r, err = w.NewReader(48)
	assert.ErrorIs(t, err, ErrorEntryNotFound)
	assert.Nil(t, r)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}
