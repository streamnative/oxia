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
