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

package option

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDurationUnmarshalYAMLParsesDurationString(t *testing.T) {
	var d Duration
	require.NoError(t, yaml.Unmarshal([]byte(`1h30m`), &d))

	assert.Equal(t, 90*time.Minute, d.ToDuration())
}

func TestDurationUnmarshalYAMLRejectsNonScalar(t *testing.T) {
	var d Duration
	err := yaml.Unmarshal([]byte(`[1s]`), &d)

	assert.Error(t, err)
	assert.Equal(t, Duration(0), d)
}

func TestDurationUnmarshalYAMLRejectsInvalidDuration(t *testing.T) {
	var d Duration
	err := yaml.Unmarshal([]byte(`ten seconds`), &d)

	assert.ErrorContains(t, err, "invalid duration")
	assert.Equal(t, Duration(0), d)
}

func TestDurationMarshalYAMLWritesDurationString(t *testing.T) {
	out, err := yaml.Marshal(Duration(5 * time.Second))
	require.NoError(t, err)

	assert.Equal(t, "5s\n", string(out))
}

func TestDurationMarshalJSONWritesDurationString(t *testing.T) {
	out, err := json.Marshal(Duration(30 * time.Minute))
	require.NoError(t, err)

	assert.JSONEq(t, `"30m0s"`, string(out))
}

func TestDurationJSONSchemaDescribesDurationString(t *testing.T) {
	schema := Duration(0).JSONSchema()

	assert.Equal(t, "string", schema.Type)
	assert.Regexp(t, schema.Pattern, "1h30m")
	assert.Regexp(t, schema.Pattern, "-1.5s")
	assert.NotRegexp(t, schema.Pattern, "ten seconds")
	assert.Equal(t, []any{"1h", "30m", "5s"}, schema.Examples)
}
