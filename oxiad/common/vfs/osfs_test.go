// Copyright 2023-2026 The Oxia Authors
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

package vfs

import (
	"os"
	"path"
	"testing"

	"github.com/edsrzf/mmap-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOSFSMapRejectsFileNotOpenedByIt(t *testing.T) {
	f, err := OSFS{}.OpenFile(path.Join(t.TempDir(), "file"), os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, f.Close()) })
	wrapped := struct{ File }{f}

	m, err := OSFS{}.Map(wrapped, 1, mmap.RDONLY)
	assert.ErrorContains(t, err, "not opened by OSFS")
	assert.Nil(t, m)
}
