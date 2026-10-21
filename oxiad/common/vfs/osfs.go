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
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/spf13/afero"
)

// OSFS is the FS of the operating system.
type OSFS struct {
	afero.OsFs
}

func (OSFS) Map(f File, length int, prot int) (mmap.MMap, error) {
	osFile, ok := f.(*os.File)
	if !ok {
		return nil, fmt.Errorf("cannot map a %T: it was not opened by OSFS", f)
	}
	return mmap.MapRegion(osFile, length, prot, 0, 0)
}

func (OSFS) Flush(m mmap.MMap) error {
	return m.Flush()
}

func (OSFS) Unmap(m *mmap.MMap) error {
	return m.Unmap()
}
