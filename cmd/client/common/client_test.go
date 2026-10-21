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

//revive:disable-next-line:var-naming
package common

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver"

	"github.com/oxia-db/oxia/cmd/common/clientauth"
)

func TestClientConfigNewClientReturnsAuthenticationError(t *testing.T) {
	config := ClientConfig{
		ServiceAddr:    "localhost:0",
		RequestTimeout: time.Second,
		Auth:           clientauth.Config{TokenFile: filepath.Join(t.TempDir(), "missing-token")},
	}

	client, err := config.NewClient()

	require.Error(t, err)
	assert.Nil(t, client)
}

func TestClientConfigNewClientWithAuthToken(t *testing.T) {
	server, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, server.Close()) })

	config := ClientConfig{
		ServiceAddr:    server.ServiceAddr(),
		Namespace:      "default",
		RequestTimeout: 30 * time.Second,
		Auth:           clientauth.Config{Token: "token"},
	}

	client, err := config.NewClient()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	_, _, err = client.Put(context.Background(), "key", []byte("value"))
	require.NoError(t, err)
	_, value, _, err := client.Get(context.Background(), "key")
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}
