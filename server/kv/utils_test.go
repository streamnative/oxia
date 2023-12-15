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

package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareWithSlash(t *testing.T) {
	assert.Equal(t, 0, compareWithSlash([]byte("aaaaa"), []byte("aaaaa")))
	assert.Equal(t, -1, compareWithSlash([]byte("aaaaa"), []byte("zzzzz")))
	assert.Equal(t, +1, compareWithSlash([]byte("bbbbb"), []byte("aaaaa")))

	assert.Equal(t, +1, compareWithSlash([]byte("aaaaa"), []byte("")))
	assert.Equal(t, -1, compareWithSlash([]byte(""), []byte("aaaaaa")))
	assert.Equal(t, 0, compareWithSlash([]byte(""), []byte("")))

	assert.Equal(t, -1, compareWithSlash([]byte("aaaaa"), []byte("aaaaaaaaaaa")))
	assert.Equal(t, +1, compareWithSlash([]byte("aaaaaaaaaaa"), []byte("aaa")))

	assert.Equal(t, -1, compareWithSlash([]byte("a"), []byte("/")))
	assert.Equal(t, +1, compareWithSlash([]byte("/"), []byte("a")))

	assert.Equal(t, -1, compareWithSlash([]byte("/aaaa"), []byte("/bbbbb")))
	assert.Equal(t, -1, compareWithSlash([]byte("/aaaa"), []byte("/aa/a")))
	assert.Equal(t, -1, compareWithSlash([]byte("/aaaa/a"), []byte("/aaaa/b")))
	assert.Equal(t, +1, compareWithSlash([]byte("/aaaa/a/a"), []byte("/bbbbbbbbbb")))
	assert.Equal(t, +1, compareWithSlash([]byte("/aaaa/a/a"), []byte("/aaaa/bbbbbbbbbb")))

	assert.Equal(t, +1, compareWithSlash([]byte("/a/b/a/a/a"), []byte("/a/b/a/b")))
}
