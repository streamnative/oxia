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

package truncate

import (
	"github.com/spf13/cobra"
	"github.com/streamnative/oxia/server/wal"
	"log/slog"
	"math"
)

type truncateOptions struct {
	namespace string
	shard     int64
	walDir    string

	lastEntry      bool
	safePointEntry int64
}

var (
	options = truncateOptions{}
	Cmd     = &cobra.Command{
		Use:   "truncate",
		Short: "truncate the WAL",
		Long:  `truncate the WAL by some conditions`,
		RunE:  exec,
	}
)

func init() {
	Cmd.Flags().Int64Var(&options.shard, "shard", 0, "shard id")
	Cmd.Flags().StringVar(&options.namespace, "namespace", "default", "namespace name")
	Cmd.Flags().StringVar(&options.walDir, "wal-dir", "", "directory path")
	// operations
	Cmd.Flags().Int64Var(&options.safePointEntry, "safe-point-entry", math.MaxInt64, "the last safe entry offset")
	Cmd.Flags().BoolVar(&options.lastEntry, "last-entry", false, "if trim the last entry")

	Cmd.MarkFlagsMutuallyExclusive("safe-point-entry", "last-entry")
	if err := Cmd.MarkFlagRequired("wal-dir"); err != nil {
		panic(err)
	}
	if err := Cmd.MarkFlagRequired("shard"); err != nil {
		panic(err)
	}
	if err := Cmd.MarkFlagRequired("namespace"); err != nil {
		panic(err)
	}
}

func exec(*cobra.Command, []string) error {
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  options.walDir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
		SyncData:    true,
	})
	writeAheadLog, err := factory.NewWal(options.namespace, options.shard, nil)
	if err != nil {
		panic(err)
	}
	defer writeAheadLog.Close()

	if options.safePointEntry != math.MaxInt64 {
		slog.Info("truncating the entries. ", slog.Int64("start", options.safePointEntry),
			slog.Int64("end", writeAheadLog.LastOffset()))
		newLastEntry, err := writeAheadLog.TruncateLog(options.safePointEntry)
		if err != nil {
			return err
		}
		slog.Info("truncate complete", slog.Int64("newLastEntry", newLastEntry))
		return nil
	}

	if options.lastEntry {
		lastOffset := writeAheadLog.LastOffset()
		safePoint := lastOffset - 1
		slog.Info("truncating the last entry. ",
			slog.Int64("start", safePoint),
			slog.Int64("end", lastOffset))
		newLastEntry, err := writeAheadLog.TruncateLog(safePoint)
		if err != nil {
			return err
		}
		slog.Info("truncate complete", slog.Int64("newLastEntry", newLastEntry))
		return nil
	}

	slog.Info("no operation applied")
	return nil
}
