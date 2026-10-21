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
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/oxia-db/oxia/common/metric"
)

const unknown = "unknown"

// Info describes the build of the oxia binary.
type Info struct {
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	CommitTime string `json:"commitTime"`
	GoVersion  string `json:"goVersion"`
	Platform   string `json:"platform"`
}

// Get returns the build of the running binary, with the version set at build time.
func Get(version string) Info {
	buildInfo, _ := debug.ReadBuildInfo()
	return newInfo(version, buildInfo)
}

func newInfo(version string, buildInfo *debug.BuildInfo) Info {
	info := Info{
		Version:    version,
		Commit:     unknown,
		CommitTime: unknown,
		GoVersion:  runtime.Version(),
		Platform:   runtime.GOOS + "/" + runtime.GOARCH,
	}
	if info.Version == "" {
		info.Version = unknown
	}
	if buildInfo == nil {
		return info
	}
	modified := false
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			info.Commit = setting.Value
		case "vcs.time":
			info.CommitTime = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		default:
		}
	}
	if modified && info.Commit != unknown {
		info.Commit += "-dirty"
	}
	return info
}

// String formats the build for `oxia --version` and `oxia version`. The first line keeps the
// format of the earlier releases, for the scripts that parse it.
func (i Info) String() string {
	return fmt.Sprintf("oxia version %s\n  commit:      %s\n  commit time: %s\n  go:          %s\n  platform:    %s\n",
		i.Version, i.Commit, i.CommitTime, i.GoVersion, i.Platform)
}

var registerMetricOnce sync.Once

// RegisterMetric exports the build as the oxia_build_info gauge, whose labels carry the version,
// the commit and the Go version, so that the dashboards show what every node runs.
func RegisterMetric(info Info) {
	registerMetricOnce.Do(func() {
		// No unit: the Prometheus exporter would otherwise add a unit suffix to the name
		metric.NewGauge("oxia_build_info", "The build of the running oxia binary", "",
			map[string]any{"version": info.Version, "commit": info.Commit, "go_version": info.GoVersion},
			func() int64 { return 1 })
	})
}
