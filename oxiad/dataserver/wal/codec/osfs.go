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

//revive:disable-next-line:var-naming
package codec

import (
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
)

// OSFS is the FS of the operating system.
type OSFS struct{}

func (OSFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (OSFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (OSFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (OSFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		// A nil *os.File in a File would not compare equal to nil
		return nil, err
	}
	return f, nil
}

func (OSFS) Remove(name string) error {
	return os.Remove(name)
}

func (OSFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
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
