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

package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
)

// Every request goes to the lookup address, whatever the shard leader.
func TestDizzyShardManagerLeader(t *testing.T) {
	sm, err := NewDizzyShardManager(NewShardStrategy(), &singleAssignmentsRpcProvider{}, "lookup:6648", "default",
		10*time.Second, nil)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, sm.Close())
	}()

	shards := sm.GetAll()
	require.Len(t, shards, 1)
	assert.Equal(t, "lookup:6648", sm.Leader(shards[0]))
}

func TestNewDizzyShardManagerFailsWithoutAssignments(t *testing.T) {
	provider := &failingAssignmentsRpcProvider{err: constant.ErrNamespaceNotFound}
	sm, err := NewDizzyShardManager(NewShardStrategy(), provider, "lookup:6648", "default", 10*time.Second, nil)
	assert.ErrorIs(t, err, constant.ErrNamespaceNotFound)
	assert.Nil(t, sm)
}
