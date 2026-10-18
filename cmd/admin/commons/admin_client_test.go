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

package commons

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/common/clientauth"
)

func TestAdminClientConfigNewAdminClientReturnsMockedClient(t *testing.T) {
	MockedAdminClient = NewMockAdminClient()
	t.Cleanup(func() { MockedAdminClient = nil })

	client, err := AdminClientConfig{}.NewAdminClient()

	require.NoError(t, err)
	assert.Same(t, MockedAdminClient, client)
}

func TestAdminClientConfigNewAdminClientWithoutAuth(t *testing.T) {
	client, err := AdminClientConfig{AdminAddress: "localhost:0"}.NewAdminClient()

	require.NoError(t, err)
	require.NotNil(t, client)
	assert.NoError(t, client.Close())
}

func TestAdminClientConfigNewAdminClientWithTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("secret\n"), 0o600))

	client, err := AdminClientConfig{
		AdminAddress: "localhost:0",
		Auth:         clientauth.Config{TokenFile: tokenFile},
	}.NewAdminClient()

	require.NoError(t, err)
	require.NotNil(t, client)
	assert.NoError(t, client.Close())
}

func TestAdminClientConfigNewAdminClientRejectsInvalidAuth(t *testing.T) {
	client, err := AdminClientConfig{
		AdminAddress: "localhost:0",
		Auth:         clientauth.Config{Token: "token", TokenFile: "token-file"},
	}.NewAdminClient()

	require.ErrorIs(t, err, clientauth.ErrAuthTokenConflict)
	assert.Nil(t, client)
}
