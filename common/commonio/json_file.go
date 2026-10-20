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

	"github.com/oxia-db/oxia/common/failpoint"
)

func ReadJSONFromFile(path string, value any) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}

	if len(data) == 0 {
		return false, nil
	}

	return true, json.Unmarshal(data, value)
}

// WriteJSONToFile replaces the file at path with value encoded in JSON: it
// writes a temporary file, then renames it into place, so that the file holds
// either the old or the new value.
func WriteJSONToFile(path string, value any) error {
	return writeJSONToFile(path, value, nil)
}

func writeJSONToFile(path string, value any, failpoints *failpoint.Failpoints) error {
	parentDir := filepath.Dir(path)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return err
	}

	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(parentDir, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if err := writeAndClose(tmp, data, failpoints); err != nil {
		return errors.Join(err, os.Remove(tmpName))
	}
	if err := os.Rename(tmpName, path); err != nil {
		return errors.Join(err, os.Remove(tmpName))
	}

	dir, err := os.Open(parentDir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func writeAndClose(file *os.File, data []byte, failpoints *failpoint.Failpoints) error {
	_, err := file.Write(data)
	if err = failpoints.Fail("writeJSONToFile.write", err); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := failpoints.Fail("writeJSONToFile.sync", file.Sync()); err != nil {
		return errors.Join(err, file.Close())
	}
	return failpoints.Fail("writeJSONToFile.close", file.Close())
}
