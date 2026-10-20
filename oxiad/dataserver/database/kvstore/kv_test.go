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

package kvstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/assert"
)

func TestFactoryOptionsEnsureDefaultsFillsMissingFields(t *testing.T) {
	options := &FactoryOptions{}
	options.EnsureDefaults()

	assert.Equal(t, DefaultFactoryOptions.DataDir, options.DataDir)
	assert.Equal(t, DefaultFactoryOptions.CacheSizeMB, options.CacheSizeMB)
	assert.Equal(t, vfs.Default, options.FS)
}

func TestFactoryOptionsEnsureDefaultsKeepsSetFields(t *testing.T) {
	fs := vfs.NewMem()
	options := &FactoryOptions{DataDir: "/my/data", CacheSizeMB: 7, FS: fs}
	options.EnsureDefaults()

	assert.Equal(t, "/my/data", options.DataDir)
	assert.EqualValues(t, 7, options.CacheSizeMB)
	assert.Same(t, fs, options.FS)
}
