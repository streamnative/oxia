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

package commonio

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

func ReadJSONFromFile(fs afero.Fs, path string, value any) (bool, error) {
	data, err := afero.ReadFile(fs, path)
	if err != nil {
		return false, err
	}

	if len(data) == 0 {
		return false, nil
	}

	return true, json.Unmarshal(data, value)
}

// WriteJSONToFile replaces the file at path with the JSON of value: it writes
// a temporary file, then renames it.
func WriteJSONToFile(fs afero.Fs, path string, value any) (err error) {
	parentDir := filepath.Dir(path)
	if err := fs.MkdirAll(parentDir, 0755); err != nil {
		return err
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	tmp, err := afero.TempFile(fs, parentDir, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		// The temporary file doesn't exist anymore once it is renamed
		if removeErr := fs.Remove(tmpName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := tmp.Sync(); err != nil {
		return errors.Join(err, tmp.Close())
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := fs.Rename(tmpName, path); err != nil {
		return err
	}

	dir, err := fs.Open(parentDir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
