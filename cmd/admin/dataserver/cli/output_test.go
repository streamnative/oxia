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

package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func TestWriteDataServersRejectsUnsupportedFormat(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServers(out, "xml", []*proto.DataServerView{newDataServerView("server-1")})

	require.ErrorContains(t, err, `unsupported output format "xml"`)
	assert.Empty(t, out.String())
}

func TestWriteDataServersSkipsNilViews(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServers(out, "json", []*proto.DataServerView{nil, newDataServerView("server-1"), nil})

	require.NoError(t, err)
	var values []map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &values))
	require.Len(t, values, 1)
	assert.Equal(t, "server-1", values[0]["data_server"].(map[string]any)["identity"].(map[string]any)["name"])
}

func TestWriteDataServersTableSkipsViewsWithoutDataServer(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServers(out, "", []*proto.DataServerView{
		nil,
		{DataServerStatus: &proto.DataServerStatus{State: proto.DataServerState_DATA_SERVER_STATE_RUNNING}},
		newDataServerView("server-1"),
	})

	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], "NAME")
	assert.Contains(t, lines[1], "server-1")
}

func TestWriteDataServerRejectsUnsupportedFormat(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServer(out, "name", newDataServerView("server-1").GetDataServer())

	require.ErrorContains(t, err, `unsupported output format "name"`)
	assert.Empty(t, out.String())
}

func TestWriteDataServerTableSkipsNilDataServer(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServer(out, "table", nil)

	require.NoError(t, err)
	assert.Equal(t, "NAME  PUBLIC  INTERNAL  LABELS", strings.TrimSpace(out.String()))
}

func TestWriteDataServerViewRejectsUnsupportedFormat(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServerView(out, "xml", newDataServerView("server-1"))

	require.ErrorContains(t, err, `unsupported output format "xml"`)
	assert.Empty(t, out.String())
}

func TestWriteDataServerViewWritesEmptyObjectForNilView(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteDataServerView(out, "json", nil)

	require.NoError(t, err)
	assert.JSONEq(t, "{}", out.String())
}

func TestWriteDataServerViewOmitsMissingStatus(t *testing.T) {
	out := new(bytes.Buffer)
	view := newDataServerView("server-1")
	view.DataServerStatus = nil

	err := WriteDataServerView(out, "json", view)

	require.NoError(t, err)
	var value map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	assert.Contains(t, value, "data_server")
	assert.NotContains(t, value, "data_server_status")
}

func newDataServerView(name string) *proto.DataServerView {
	return &proto.DataServerView{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{Name: &name, Public: name + "-public", Internal: name + "-internal"},
		},
		DataServerStatus: &proto.DataServerStatus{State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
	}
}
