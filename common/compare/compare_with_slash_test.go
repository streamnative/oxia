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

package compare

import (
	"fmt"
	"testing"

	"github.com/emirpasic/gods/utils"
	"github.com/stretchr/testify/assert"
)

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

func TestAbbreviatedKey(t *testing.T) {
	aKey1 := AbbreviatedKeyDisableSlash([]byte("aaa"))
	aKey2 := AbbreviatedKeyDisableSlash([]byte("/aaa"))
	aKey3 := AbbreviatedKeyDisableSlash([]byte("/aa/a"))
	aKey4 := AbbreviatedKeyDisableSlash([]byte("/aa/a/a"))
	aKey5 := AbbreviatedKeyDisableSlash([]byte("/aa/a/a/"))

	bKey1 := AbbreviatedKeyDisableSlash([]byte("bbb"))
	bKey2 := AbbreviatedKeyDisableSlash([]byte("/bbb"))
	bKey3 := AbbreviatedKeyDisableSlash([]byte("/bb/b"))
	bKey4 := AbbreviatedKeyDisableSlash([]byte("/bb/b/b"))
	bKey5 := AbbreviatedKeyDisableSlash([]byte("/bb/b/b/"))

	assert.Greater(t, aKey2, aKey1)
	assert.Greater(t, aKey3, aKey2)
	assert.Greater(t, aKey4, aKey3)
	assert.Equal(t, aKey5, aKey4)

	assert.Greater(t, bKey1, aKey1)
	assert.Greater(t, bKey2, aKey2)
	assert.Greater(t, bKey3, aKey3)
	assert.Greater(t, bKey4, aKey4)
	assert.Greater(t, bKey5, aKey5)
}

func TestCompareWithDataset(t *testing.T) {
	for _, test := range []struct {
		leftKey  string
		rightKey string
		expect   int
	}{
		{"aaa", "aaa", 0},
		{"aaa", "zzzzz", -1},
		{"bbbbb", "aaaaa", +1},
		{"aaaaa", "", +1},
		{"", "aaaaaa", -1},
		{"", "", 0},
		{"aaaaa", "aaaaaaaaaaa", -1},
		{"aaaaaaaaaaa", "aaa", +1},
		{"a", "/", -1},
		{"/", "a", +1},
		{"/aaaa", "/bbbbb", -1},
		{"/aaaa", "/aa/a", -1},
		{"/aaaa/a", "/aaaa/b", -1},
		{"/aaaa/a/a", "/bbbbbbbbbb", +1},
		{"/aaaa/a/a", "/aaaa/bbbbbbbbbb", +1},
		{"/a/b/a/a/a", "/a/b/a/b", +1},
	} {
		t.Run(fmt.Sprintf("%v?%v", test.leftKey, test.rightKey), func(t *testing.T) {
			// test compare with slash
			assert.Equal(t, test.expect, CompareWithSlash([]byte(test.leftKey), []byte(test.rightKey)))
			// test compare with abbreviated key
			lbk := AbbreviatedKeyDisableSlash([]byte(test.leftKey))
			rbk := AbbreviatedKeyDisableSlash([]byte(test.rightKey))
			if lbk == rbk {
				assert.Equal(t, test.expect, CompareWithSlash([]byte(test.leftKey), []byte(test.rightKey)))
			} else {
				assert.Equal(t, test.expect, utils.UInt64Comparator(lbk, rbk))
			}
		})
	}
}
