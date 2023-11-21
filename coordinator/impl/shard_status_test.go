// Copyright 2023 StreamNative, Inc.
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

package impl

import (
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShardStatus_String(t *testing.T) {
	assert.Equal(t, "Unknown", model.ShardStatusUnknown.String())
	assert.Equal(t, "SteadyState", model.ShardStatusSteadyState.String())
	assert.Equal(t, "Election", model.ShardStatusElection.String())
}

func TestShardStatus_JSON(t *testing.T) {
	j, err := model.ShardStatusSteadyState.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, []byte("\"SteadyState\""), j)

	var s model.ShardStatus
	err = s.UnmarshalJSON(j)
	assert.NoError(t, err)
	assert.Equal(t, model.ShardStatusSteadyState, s)

	err = s.UnmarshalJSON([]byte("xyz"))
	assert.Error(t, err)
}
