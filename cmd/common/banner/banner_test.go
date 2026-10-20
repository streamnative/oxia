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

package banner

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

func TestMetricsFieldShowsEndpoint(t *testing.T) {
	assert.Equal(t, Field{"Metrics Endpoint", "http://0.0.0.0:8080/metrics"},
		MetricsField(&option.MetricOptions{BindAddress: "0.0.0.0:8080"}))
}

func TestMetricsFieldShowsTLSEndpoint(t *testing.T) {
	options := &option.MetricOptions{BindAddress: "0.0.0.0:8080"}
	options.TLS.CertFile = "cert.pem"
	options.TLS.KeyFile = "key.pem"
	assert.Equal(t, Field{"Metrics Endpoint", "https://0.0.0.0:8080/metrics"}, MetricsField(options))
}

func TestMetricsFieldShowsDisabled(t *testing.T) {
	enabled := false
	assert.Equal(t, Field{"Metrics Endpoint", "disabled"},
		MetricsField(&option.MetricOptions{Enabled: &enabled, BindAddress: "0.0.0.0:8080"}))
}

func TestPprofFieldShowsAddress(t *testing.T) {
	process.PprofEnable = true
	process.PprofBindAddress = "127.0.0.1:6060"
	t.Cleanup(func() { process.PprofEnable = false })

	assert.Equal(t, Field{"Pprof Endpoint", "http://127.0.0.1:6060/debug/pprof"}, PprofField())
}

func TestPprofFieldShowsDisabled(t *testing.T) {
	assert.Equal(t, Field{"Pprof Endpoint", "disabled"}, PprofField())
}

func TestInstanceIDFieldShowsInstanceID(t *testing.T) {
	dataDir := t.TempDir()
	m, err := manifest.NewManifest(dataDir)
	require.NoError(t, err)
	_, err = m.SetInstanceID("instance-1")
	require.NoError(t, err)

	assert.Equal(t, Field{"InstanceID", "instance-1"}, InstanceIDField(dataDir))
}

func TestInstanceIDFieldShowsUnassigned(t *testing.T) {
	assert.Equal(t, Field{"InstanceID", "unassigned"}, InstanceIDField(t.TempDir()))
}

func TestInstanceIDFieldShowsUnknown(t *testing.T) {
	dataDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "MANIFEST"), []byte("{not-json"), 0600))

	assert.Equal(t, Field{"InstanceID", "unknown"}, InstanceIDField(dataDir))
}

func TestPrintShowsFieldsNextToLogo(t *testing.T) {
	var out bytes.Buffer
	Print(&out, "1.2.3", Field{"Component", "DATASERVER"}, Field{"Data Dir", "./data/db"})

	lines := strings.Split(strings.Trim(out.String(), "\n"), "\n")
	assert.Equal(t, logo[2], lines[2])
	assert.Equal(t, logo[3]+"      Component  DATASERVER", lines[3])
	assert.Equal(t, logo[4]+"     Data Dir   ./data/db", lines[4])
	assert.Equal(t, logo[5]+"    Version    1.2.3", lines[5])
	assert.Len(t, lines, len(logo))
}

func TestPrintUnknownVersion(t *testing.T) {
	var out bytes.Buffer
	Print(&out, "")
	assert.Contains(t, out.String(), "Version  unknown")
}

func TestPrintSkipsJSONLogs(t *testing.T) {
	logging.LogJSON = true
	t.Cleanup(func() { logging.LogJSON = false })

	var out bytes.Buffer
	Print(&out, "1.2.3")
	assert.Empty(t, out.String())
}
