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

package datanode

import (
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/datanode/config"
)

func TestNewServer(t *testing.T) {
	nodeConfig := config.NodeConfig{
		InternalServiceAddr: "localhost:0",
		PublicServiceAddr:   "localhost:0",
		MetricsServiceAddr:  "localhost:0",
	}

	server, err := New(nodeConfig)
	assert.NoError(t, err)

	url := fmt.Sprintf("http://localhost:%d/metrics", server.metrics.Port())
	response, err := http.Get(url)
	assert.NoError(t, err)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}

	assert.Equal(t, 200, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	assert.NoError(t, err)

	// Looks like exposition format
	assert.Equal(t, "# HELP ", string(body[0:7]))
}
