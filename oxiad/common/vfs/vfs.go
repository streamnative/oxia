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
	"github.com/edsrzf/mmap-go"
	"github.com/spf13/afero"
)

// FS holds the files of the wal, and maps the segment files in memory. The
// server uses OSFS: the tests use another FS to make its operations fail.
type FS interface {
	afero.Fs

	// Map maps the first length bytes of a file opened by this FS, or the
	// whole file when length is negative.
	Map(f File, length int, prot int) (mmap.MMap, error)
	// Flush syncs the changes of the mapping to the file.
	Flush(m mmap.MMap) error
	// Unmap releases the mapping, and sets it to nil.
	Unmap(m *mmap.MMap) error
}

type File interface {
	afero.File
}
