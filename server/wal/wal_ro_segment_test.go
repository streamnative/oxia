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
	"fmt"
	"testing"

	"github.com/streamnative/oxia/server/wal/codec"

	"github.com/stretchr/testify/assert"
)

func TestReadOnlySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}

	assert.NoError(t, rw.Close())

	ro, err := newReadOnlySegment(path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	data, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)

	assert.NoError(t, ro.Close())
}
