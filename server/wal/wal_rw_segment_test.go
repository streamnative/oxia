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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWriteSegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024)
	assert.NoError(t, err)

	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, -1, rw.LastOffset())

	assert.NoError(t, rw.Append(0, []byte("entry-0")))
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 0, rw.LastOffset())

	assert.NoError(t, rw.Flush())

	assert.NoError(t, rw.Append(1, []byte("entry-1")))
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 1, rw.LastOffset())

	assert.NoError(t, rw.Close())

	// Re-open and recover the segment
	rw, err = newReadWriteSegment(path, 0, 128*1024)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 1, rw.LastOffset())

	data, err := rw.Read(0)
	assert.NoError(t, err)
	assert.Equal(t, "entry-0", string(data))

	data, err = rw.Read(1)
	assert.NoError(t, err)
	assert.Equal(t, "entry-1", string(data))

	assert.NoError(t, rw.Close())
}

func TestReadWriteSegment_NonZero(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 5, 128*1024)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 4, rw.LastOffset())

	assert.NoError(t, rw.Append(5, []byte("entry-0")))
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 5, rw.LastOffset())

	assert.NoError(t, rw.Flush())

	assert.NoError(t, rw.Append(6, []byte("entry-1")))
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.ErrorIs(t, rw.Append(4, []byte("entry-4")), ErrInvalidNextOffset)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.ErrorIs(t, rw.Append(8, []byte("entry-8")), ErrInvalidNextOffset)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.NoError(t, rw.Close())

	// Re-open and recover the segment
	rw, err = newReadWriteSegment(path, 5, 128*1024)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())
}

func TestReadWriteSegment_HasSpace(t *testing.T) {
	rw, err := newReadWriteSegment(t.TempDir(), 0, 1024)
	assert.NoError(t, err)

	assert.True(t, rw.HasSpace(10))
	assert.False(t, rw.HasSpace(1024))
	assert.True(t, rw.HasSpace(1020))
	assert.False(t, rw.HasSpace(1021))

	assert.NoError(t, rw.Append(0, make([]byte, 100)))
	assert.True(t, rw.HasSpace(10))
	assert.False(t, rw.HasSpace(1020))
	assert.False(t, rw.HasSpace(1020-100))
	assert.True(t, rw.HasSpace(1020-100-8))
}
