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

func TestWriteNamespacesWritesTable(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteNamespaces(out, "", []*proto.Namespace{
		{Name: "ns-1", InitialShardCount: 1, ReplicationFactor: 3},
		{Name: "ns-2", InitialShardCount: 4, ReplicationFactor: 1},
	})

	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 3)
	assert.Equal(t, []string{"NAME", "INITIAL_SHARDS", "REPLICATION_FACTOR"}, strings.Fields(lines[0]))
	assert.Equal(t, []string{"ns-1", "1", "3"}, strings.Fields(lines[1]))
	assert.Equal(t, []string{"ns-2", "4", "1"}, strings.Fields(lines[2]))
}

func TestWriteNamespacesWritesJSON(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteNamespaces(out, "json", []*proto.Namespace{{Name: "ns-1"}})

	require.NoError(t, err)
	var values []map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &values))
	require.Len(t, values, 1)
	assert.Equal(t, "ns-1", values[0]["name"])
}

func TestWriteNamespacesRejectsNilNamespace(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteNamespaces(out, "json", []*proto.Namespace{{Name: "ns-1"}, nil})

	require.EqualError(t, err, "namespace must not be nil")
	assert.Empty(t, out.String())
}

func TestWriteNamespaceRejectsUnsupportedFormat(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteNamespace(out, "xml", &proto.Namespace{Name: "ns-1"})

	require.ErrorContains(t, err, `unsupported output format "xml"`)
	assert.Empty(t, out.String())
}

func TestWriteNamespaceViewRejectsViewWithoutNamespace(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteNamespaceView(out, "", &proto.NamespaceView{})

	require.EqualError(t, err, "namespace must not be nil")
	assert.Empty(t, out.String())
}

func TestWriteNamespaceViewsRejectsNilView(t *testing.T) {
	out := new(bytes.Buffer)

	err := WriteNamespaceViews(out, "", []*proto.NamespaceView{nil})

	require.EqualError(t, err, "namespace must not be nil")
	assert.Empty(t, out.String())
}

func TestWriteNamespaceTableSkipsNilNamespace(t *testing.T) {
	out := new(bytes.Buffer)

	err := writeNamespaceTable(out, []*proto.Namespace{nil, {Name: "ns-1", InitialShardCount: 2, ReplicationFactor: 3}})

	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, []string{"ns-1", "2", "3"}, strings.Fields(lines[1]))
}

func TestWriteNamespaceViewTableSkipsViewsWithoutNamespace(t *testing.T) {
	out := new(bytes.Buffer)

	err := writeNamespaceViewTable(out, []*proto.NamespaceView{
		nil,
		{},
		{Namespace: &proto.Namespace{Name: "ns-1", InitialShardCount: 2, ReplicationFactor: 3}},
	})

	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, []string{"ns-1", "2", "3"}, strings.Fields(lines[1]))
}
