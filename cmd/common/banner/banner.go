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
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

var logo = []string{
	"      ⣼⣦⡀   ⣀⣀⣀⣀⣀⣀",
	"    ⢀⣾⣿⣿⣷ ⣴⣿⣿⣿⣿⣿⣿⠏",
	"    ⣼⣿⣿⣿⡏⠸⠿⠿⠿⠿⠿⠟⠉",
	"    ⢹⣿⣿⠏      ⠰⣿⣿⣿⣶⣄",
	"⢴⣶⣶⣶⣤⡙⠋  ⢀⣤⣤⡀  ⠘⣿⣿⣿⣿⣆",
	" ⢻⣿⣿⣿⣷⡀  ⠘⢿⡿⠇  ⢀⡈⠻⣿⣿⣿⣧",
	"  ⠹⢿⣿⣿⣿⡄      ⢠⣾⣿⣆ ⠈⠁",
	"    ⠉⠉⣉⣥⣤⣤⣤⣤⡄⢠⣿⣿⣿⣿",
	"    ⢠⣾⣿⣿⣿⣿⣿⡿⠃⢸⣿⣿⣿⠏",
	"    ⠘⠛⠛⠛⠛⠛⠉  ⠘⢿⣿⠃",
}

const (
	logoWidth = 22
	gap       = "    "
)

// Field is a line of information printed on the right side of the logo.
type Field struct {
	Name  string
	Value string
}

// MetricsField shows where the metrics server serves the Prometheus metrics, or that it is disabled.
func MetricsField(options *option.MetricOptions) Field {
	if !options.IsEnabled() {
		return Field{"Metrics Endpoint", "disabled"}
	}
	scheme := "http"
	if options.TLS.IsEnabled() {
		scheme = "https"
	}
	endpoint := url.URL{Scheme: scheme, Host: options.BindAddress, Path: "/metrics"}
	return Field{"Metrics Endpoint", endpoint.String()}
}

// PprofField shows where the pprof server serves the profiles, or that it is disabled.
func PprofField() Field {
	if !process.PprofEnable {
		return Field{"Pprof Endpoint", "disabled"}
	}
	endpoint := url.URL{Scheme: "http", Host: process.PprofBindAddress, Path: "/debug/pprof"}
	return Field{"Pprof Endpoint", endpoint.String()}
}

// InstanceIDField shows the instance id of the cluster that the data server in dataDir is bound to.
func InstanceIDField(dataDir string) Field {
	instanceID, err := manifest.ReadInstanceID(dataDir)
	switch {
	case err != nil:
		return Field{"InstanceID", "unknown"}
	case instanceID == "":
		return Field{"InstanceID", "unassigned"}
	default:
		return Field{"InstanceID", instanceID}
	}
}

// Print writes the Oxia logo to w, with the fields and the version on its right side.
// It prints nothing when the logs are in JSON, since the banner would break their parsing.
func Print(w io.Writer, version string, fields ...Field) {
	if logging.LogJSON {
		return
	}
	if version == "" {
		version = "unknown"
	}

	fields = append(fields, Field{"Version", version})
	nameWidth := 0
	for _, f := range fields {
		nameWidth = max(nameWidth, len(f.Name))
	}
	info := make([]string, 0, len(fields))
	for _, f := range fields {
		info = append(info, fmt.Sprintf("%-*s  %s", nameWidth, f.Name, f.Value))
	}
	top := max(0, (len(logo)-len(info))/2)

	lines := []string{""}
	for i := 0; i < max(len(logo), top+len(info)); i++ {
		line := ""
		if i < len(logo) {
			line = logo[i]
		}
		if j := i - top; j >= 0 && j < len(info) {
			line += strings.Repeat(" ", logoWidth-utf8.RuneCountInString(line)) + gap + info[j]
		}
		lines = append(lines, line)
	}
	_, _ = io.WriteString(w, strings.Join(lines, "\n")+"\n\n")
}
