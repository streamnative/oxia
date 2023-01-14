package wal

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
	"os"
	"testing"
)

const (
	testFile  = "test-file.bin"
	writeSize = 4096
)

func BenchmarkFSync_stdlib(b *testing.B) {
	_ = os.Remove(testFile)
	f, err := os.Create(testFile)
	assert.NoError(b, err)
	data := make([]byte, writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		assert.NoError(b, err)

		err = f.Sync()
		assert.NoError(b, err)
	}

	b.StopTimer()
	_ = os.Remove(testFile)
}

func BenchmarkFSync_x_sys(b *testing.B) {
	_ = os.Remove(testFile)
	f, err := os.Create(testFile)
	assert.NoError(b, err)
	data := make([]byte, writeSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		assert.NoError(b, err)

		err = unix.Fsync(int(f.Fd()))
		assert.NoError(b, err)
	}

	b.StopTimer()
	_ = os.Remove(testFile)
}
