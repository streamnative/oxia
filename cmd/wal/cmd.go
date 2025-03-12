// Copyright 2024 StreamNative, Inc.
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

package wal

import (
	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/wal/common"
	"github.com/streamnative/oxia/cmd/wal/perf"
	"github.com/streamnative/oxia/cmd/wal/truncate"
)

var (
	Cmd = &cobra.Command{
		Use:   "wal",
		Short: "Wal utils",
		Long:  `Tools for the oxia WAL`,
	}
)

func init() {
	Cmd.PersistentFlags().Int64Var(&common.WalOption.Shard, "shard", 0, "shard id")
	Cmd.PersistentFlags().StringVar(&common.WalOption.Namespace, "namespace", "default", "namespace name")
	Cmd.PersistentFlags().StringVar(&common.WalOption.WalDir, "wal-dir", "", "directory path")
	Cmd.AddCommand(truncate.Cmd)
	Cmd.AddCommand(perf.Cmd)

	if err := Cmd.MarkPersistentFlagRequired("wal-dir"); err != nil {
		panic(err)
	}
	if err := Cmd.MarkPersistentFlagRequired("shard"); err != nil {
		panic(err)
	}
	if err := Cmd.MarkPersistentFlagRequired("namespace"); err != nil {
		panic(err)
	}
}
