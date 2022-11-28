package wal

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"strings"
	"testing"
)

const shard = uint32(100)

type walFactoryFactory interface {
	NewWalFactory(t *testing.T) WalFactory
	Name() string
	Persistent() bool
}

type inMemoryWalFactoryFactory struct{}
type tidwallWalFactoryFactory struct{}

func (_ *inMemoryWalFactoryFactory) NewWalFactory(_ *testing.T) WalFactory {
	return NewInMemoryWalFactory()
}

func (_ *inMemoryWalFactoryFactory) Name() string {
	return "InMemory/"
}

func (_ *inMemoryWalFactoryFactory) Persistent() bool {
	return false
}

func (_ *tidwallWalFactoryFactory) NewWalFactory(t *testing.T) WalFactory {
	dir := t.TempDir()
	f := NewWalFactory(&WalFactoryOptions{dir})
	return f
}

func (_ *tidwallWalFactoryFactory) Name() string {
	return "Tidwall/"
}

func (_ *tidwallWalFactoryFactory) Persistent() bool {
	return true
}

var walFF walFactoryFactory = &inMemoryWalFactoryFactory{}

func TestWal(t *testing.T) {
	for _, f := range []walFactoryFactory{&inMemoryWalFactoryFactory{}, &tidwallWalFactoryFactory{}} {
		walFF = f
		t.Run(f.Name()+"Factory_NewWal", Factory_NewWal)
		t.Run(f.Name()+"Append", Append)
		t.Run(f.Name()+"Truncate", Truncate)
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

func assertReaderReadsEventually(r WalReader, entries []string) chan error {
	ch := make(chan error)
	go func() {
		for i := 0; i < len(entries); i++ {
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

func Factory_NewWal(t *testing.T) {
	f, w := createWal(t)
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assert.False(t, rr.HasNext())
	assert.NoError(t, rr.Close())
	fr, err := w.NewReader(EntryId{})
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
			EntryId: &proto.EntryId{
				Epoch:  1,
				Offset: uint64(i),
			},
			Value: []byte(s),
		})
		assert.NoError(t, err)
	}

	// Read entries backwards
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assertReaderReads(t, rr, []string{"C", "B", "A"})
	assert.NoError(t, rr.Close())

	// Read with forward reader from beginning
	fr, err := w.NewReader(EntryId{})
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())

	// Read with forward reader from the middle
	fr, err = w.NewReader(EntryId{1, 1})
	assert.NoError(t, err)
	assertReaderReads(t, fr, []string{"C"})
	assert.NoError(t, fr.Close())

	// Read with forward reader waiting for new entries
	fr, err = w.NewReader(EntryId{1, 0})
	assert.NoError(t, err)
	ch := assertReaderReadsEventually(fr, []string{"B", "C", "D"})

	err = w.Append(&proto.LogEntry{
		EntryId: &proto.EntryId{
			Epoch:  1,
			Offset: uint64(3),
		},
		Value: []byte("D"),
	})
	assert.NoError(t, err)
	assert.NoError(t, <-ch)

	assert.NoError(t, fr.Close())

	// Append invalid offset
	err = w.Append(&proto.LogEntry{
		EntryId: &proto.EntryId{
			Epoch:  1,
			Offset: uint64(88),
		},
		Value: []byte("E"),
	})
	assert.True(t, err != nil && strings.Contains(err.Error(), "Invalid next entry"))

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
			EntryId: &proto.EntryId{
				Epoch:  1,
				Offset: uint64(i),
			},
			Value: []byte(s),
		})
		assert.NoError(t, err)
	}

	headIndex, err := w.TruncateLog(EntryId{1, 2})
	assert.NoError(t, err)

	assert.Equal(t, EntryId{1, 2}, headIndex)

	// Read with forward reader from beginning
	fr, err := w.NewReader(EntryId{})
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
			EntryId: &proto.EntryId{
				Epoch:  1,
				Offset: uint64(i),
			},
			Value: []byte(s),
		})
		assert.NoError(t, err)
	}

	err := w.Close()
	assert.NoError(t, err)

	w, err = f.NewWal(shard)
	assert.NoError(t, err)

	// Read with forward reader from beginning
	fr, err := w.NewReader(EntryId{})
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}
