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

package component

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func TestNewOptionsDataServer(t *testing.T) {
	options, err := NewOptions("dataserver")
	require.NoError(t, err)
	assert.IsType(t, &dataserveroption.Options{}, options)
}

func TestNewOptionsCoordinator(t *testing.T) {
	options, err := NewOptions("coordinator")
	require.NoError(t, err)
	assert.IsType(t, &coordinatoroption.Options{}, options)
}

func TestNewOptionsUnknownComponent(t *testing.T) {
	_, err := NewOptions("broker")
	assert.EqualError(t, err, `unknown component "broker": use dataserver or coordinator`)
}
