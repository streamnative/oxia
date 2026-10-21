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

package version

import (
	"context"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oxia-db/oxia/common/metric"
)

func TestGetReportsVersionAndRuntime(t *testing.T) {
	info := Get("1.2.3")

	assert.Equal(t, "1.2.3", info.Version)
	assert.Equal(t, runtime.Version(), info.GoVersion)
	assert.Equal(t, runtime.GOOS+"/"+runtime.GOARCH, info.Platform)
}

func TestNewInfoReadsVCSSettings(t *testing.T) {
	info := newInfo("1.2.3", &debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "vcs.revision", Value: "abc123"},
		{Key: "vcs.time", Value: "2026-10-20T19:47:47Z"},
		{Key: "vcs.modified", Value: "false"},
		{Key: "GOOS", Value: "linux"},
	}})

	assert.Equal(t, "abc123", info.Commit)
	assert.Equal(t, "2026-10-20T19:47:47Z", info.CommitTime)
}

func TestNewInfoMarksModifiedCommit(t *testing.T) {
	info := newInfo("1.2.3", &debug.BuildInfo{Settings: []debug.BuildSetting{
		{Key: "vcs.revision", Value: "abc123"},
		{Key: "vcs.modified", Value: "true"},
	}})

	assert.Equal(t, "abc123-dirty", info.Commit)
}

func TestNewInfoWithoutBuildInfo(t *testing.T) {
	info := newInfo("", nil)

	assert.Equal(t, Info{
		Version:    "unknown",
		Commit:     "unknown",
		CommitTime: "unknown",
		GoVersion:  runtime.Version(),
		Platform:   runtime.GOOS + "/" + runtime.GOARCH,
	}, info)
}

func TestInfoString(t *testing.T) {
	info := Info{Version: "1.2.3", Commit: "abc123", CommitTime: "2026-10-20T19:47:47Z", GoVersion: "go1.27.1", Platform: "linux/amd64"}

	assert.Equal(t, "oxia version 1.2.3\n"+
		"  commit:      abc123\n"+
		"  commit time: 2026-10-20T19:47:47Z\n"+
		"  go:          go1.27.1\n"+
		"  platform:    linux/amd64\n", info.String())
}

func TestRegisterMetricExportsBuildInfo(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	metric.GetMeter()
	metric.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("oxia"))

	info := Info{Version: "1.2.3", Commit: "abc123", GoVersion: "go1.27.1"}
	RegisterMetric(info)
	// Registering again doesn't duplicate the series
	RegisterMetric(info)

	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)
	buildInfo := data.ScopeMetrics[0].Metrics[0]
	assert.Equal(t, "oxia_build_info", buildInfo.Name)
	points := buildInfo.Data.(metricdata.Gauge[int64]).DataPoints
	require.Len(t, points, 1)
	assert.EqualValues(t, 1, points[0].Value)
	assert.Equal(t, attribute.NewSet(
		attribute.String("commit", "abc123"),
		attribute.String("go_version", "go1.27.1"),
		attribute.String("version", "1.2.3"),
	), points[0].Attributes)
}
