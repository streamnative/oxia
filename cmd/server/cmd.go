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

package server

import (
	"github.com/spf13/cobra"
	"io"
	"oxia/cmd/flag"
	"oxia/common"
	"oxia/server"
	"time"
)

var (
	conf = server.Config{}

	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	flag.PublicPort(Cmd, &conf.PublicServicePort)
	flag.InternalPort(Cmd, &conf.InternalServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
	Cmd.Flags().StringVar(&conf.DataDir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().StringVar(&conf.WalDir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().DurationVar(&conf.WalRetentionTime, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return server.New(conf)
	})
}
