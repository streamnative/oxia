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

package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringMapReturnsEmptyMapForNoValues(t *testing.T) {
	result, err := StringMap(nil)

	require.NoError(t, err)
	assert.Equal(t, map[string]string{}, result)
}

func TestStringMapSkipsEmptyEntries(t *testing.T) {
	result, err := StringMap([]string{"", "rack=rack-1", "", "zone="})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"rack": "rack-1", "zone": ""}, result)
}

func TestStringMapKeepsSeparatorInValue(t *testing.T) {
	result, err := StringMap([]string{"selector=a=b"})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"selector": "a=b"}, result)
}

func TestStringMapRejectsInvalidEntries(t *testing.T) {
	for _, value := range []string{"rack", "=rack-1"} {
		t.Run(value, func(t *testing.T) {
			result, err := StringMap([]string{value})

			require.EqualError(t, err, `invalid entry "`+value+`", expected key=value`)
			assert.Nil(t, result)
		})
	}
}
