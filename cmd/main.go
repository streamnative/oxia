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

package main

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/streamnative/oxia/cmd/client"
	"github.com/streamnative/oxia/cmd/coordinator"
	"github.com/streamnative/oxia/cmd/health"
	"github.com/streamnative/oxia/cmd/pebble"
	"github.com/streamnative/oxia/cmd/perf"
	"github.com/streamnative/oxia/cmd/server"
	"github.com/streamnative/oxia/cmd/standalone"
	"github.com/streamnative/oxia/common"
)

var (
	logLevelStr string
	rootCmd     = &cobra.Command{
		Use:               "oxia",
		Short:             "Oxia root command",
		Long:              `Oxia root command`,
		PersistentPreRunE: configureLogLevel,
	}
)

type LogLevelError string

func (l LogLevelError) Error() string {
	return fmt.Sprintf("unknown log level (%s)", string(l))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", common.DefaultLogLevel.String(), "Set logging level [disabled|trace|debug|info|warn|error|fatal|panic]")
	rootCmd.PersistentFlags().BoolVarP(&common.LogJson, "log-json", "j", false, "Print logs in JSON format")
	rootCmd.PersistentFlags().BoolVar(&common.PprofEnable, "profile", false, "Enable pprof profiler")
	rootCmd.PersistentFlags().StringVar(&common.PprofBindAddress, "profile-bind-address", "127.0.0.1:6060", "Bind address for pprof")

	rootCmd.AddCommand(client.Cmd)
	rootCmd.AddCommand(coordinator.Cmd)
	rootCmd.AddCommand(health.Cmd)
	rootCmd.AddCommand(perf.Cmd)
	rootCmd.AddCommand(server.Cmd)
	rootCmd.AddCommand(standalone.Cmd)
	rootCmd.AddCommand(pebble.Cmd)
}

func configureLogLevel(cmd *cobra.Command, args []string) error {
	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	common.LogLevel = logLevel
	common.ConfigureLogger()
	return nil
}

func main() {
	common.DoWithLabels(map[string]string{
		"oxia": "main",
	}, func() {
		if _, err := maxprocs.Set(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := rootCmd.Execute(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	})
}
