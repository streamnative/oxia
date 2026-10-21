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

package oxia

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSecondaryIndexNameValidation(t *testing.T) {
	_, err := newPutOptions([]PutOption{SecondaryIndex("tenant/users", "email")})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.ErrorContains(t, err, "must not contain '/'")

	_, err = newPutOptions([]PutOption{SecondaryIndex("tenant", "users/email")})
	assert.NoError(t, err)
}

func TestInvalidArgumentStatus(t *testing.T) {
	result := toPutResult("key", &proto.PutResponse{Status: proto.Status_INVALID_ARGUMENT})
	assert.ErrorIs(t, result.Err, ErrInvalidOptions)
}

func TestSequenceKeysDeltasValidation(t *testing.T) {
	_, err := newPutOptions([]PutOption{SequenceKeysDeltas(1)})
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.ErrorContains(t, err, "requires PartitionKey()")

	_, err = newPutOptions([]PutOption{SequenceKeysDeltas(1), PartitionKey("p"), ExpectedVersionId(1)})
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.ErrorContains(t, err, "does not allow to specify an ExpectedVersionId")

	_, err = newPutOptions([]PutOption{SequenceKeysDeltas(0, 1), PartitionKey("p")})
	assert.ErrorIs(t, err, ErrInvalidOptions)
	assert.ErrorContains(t, err, "first delta in sequence keys delta must always be > 0")

	putOpts, err := newPutOptions([]PutOption{SequenceKeysDeltas(1, 0), PartitionKey("p")})
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 0}, putOpts.sequenceKeysDeltas)
}
