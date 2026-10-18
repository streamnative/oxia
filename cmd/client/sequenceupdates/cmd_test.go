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

package sequenceupdates

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/cmd/common/clientauth"
)

func TestFlagsReset(t *testing.T) {
	config := flags{key: "key", partitionKey: "partition-key"}

	config.Reset()

	assert.Equal(t, flags{}, config)
}

func TestExecReturnsNewClientError(t *testing.T) {
	mocked, auth := common.MockedClient, common.Config.Auth
	common.MockedClient = nil
	common.Config.Auth = clientauth.Config{Token: "token", TokenFile: "token-file"}
	t.Cleanup(func() { common.MockedClient, common.Config.Auth = mocked, auth })

	err := runCmd("key", "-p", "partition-key")

	assert.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
}

func TestExecReturnsSequenceUpdatesErrorAndClosesClient(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })
	common.MockedClient.On("Close").Return(nil)

	err := runCmd("key", "-p", "partition-key")

	assert.EqualError(t, err, "not implemented in mock")
	common.MockedClient.AssertExpectations(t)
}

func runCmd(args ...string) error {
	out := new(bytes.Buffer)
	Cmd.SetOut(out)
	Cmd.SetErr(out)
	Cmd.SetArgs(args)
	err := Cmd.Execute()
	Config.Reset()
	return err
}
