// Copyright 2024 StreamNative, Inc.
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

package codec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestV1_GetHeaderSize(t *testing.T) {
	assert.EqualValues(t, v1.GetHeaderSize(), 4)
}

func TestV1_Codec(t *testing.T) {
	buf := make([]byte, 100)
	payload := []byte{1}
	recordSize, _ := v1.WriteRecord(buf, 0, 0, payload)
	assert.EqualValues(t, recordSize, 5)
	getRecordSize, err := v1.GetRecordSize(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, getRecordSize, recordSize)

	getPayload, err := v1.ReadRecordWithValidation(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload, getPayload)
}
