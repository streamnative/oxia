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

package scan

import (
	"encoding/json"
	"fmt"
	"github.com/streamnative/oxia/proto"
	"log/slog"
	"math"
	"time"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/wal/common"
	"github.com/streamnative/oxia/server/wal"
)

type scanOptions struct {
	json bool
	//lastEntry      bool
	//safePointEntry int64
}

var (
	options = scanOptions{}
	Cmd     = &cobra.Command{
		Use:   "scan",
		Short: "scan the WAL",
		Long:  `scan the WAL`,
		RunE:  exec,
	}
)

func init() {
	// operations
	//Cmd.Flags().Int64Var(&options.safePointEntry, "last-safe-entry", math.MaxInt64,
	//	"removes entries from the end of the log that have an offset greater than last safe entry")
	Cmd.Flags().BoolVar(&options.json, "json", false, "dump entries in json format")
	//
	//Cmd.MarkFlagsMutuallyExclusive("last-safe-entry", "truncate-last-entry")
}

type LogEntry struct {
	Term          int64                `json:"term"`
	Offset        int64                `json:"offset"`
	Timestamp     time.Time            `json:"time"`
	WriteRequests *proto.WriteRequests `json:"writeRequests"`
}

func exec(*cobra.Command, []string) error {
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
		SyncData:    false,
	})

	wal, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	if err != nil {
		return err
	}
	defer wal.Close()

	reader, err := wal.NewReader(wal.FirstOffset() - 1)
	if err != nil {
		return err
	}

	for reader.HasNext() {
		le, err := reader.ReadNext()
		if err != nil {
			return err
		}

		lev := proto.LogEntryValue{}
		if err = lev.UnmarshalVTUnsafe(le.Value); err != nil {
			return err
		}

		if options.json {
			logEntry := LogEntry{
				Term:          le.Term,
				Offset:        le.Offset,
				Timestamp:     time.UnixMilli(int64(le.Timestamp)),
				WriteRequests: lev.GetRequests(),
			}

			ser, err := json.Marshal(logEntry)
			if err != nil {
				return err
			}
			fmt.Println(string(ser))
		} else {
			//fmt.Printf("Timestamp: %s -- Term: %d -- Offset: %d\n", time.UnixMilli(int64(le.Timestamp)), logEntry.Term, logEntry.Offset)
			for _, writes := range lev.GetRequests().Writes {
				for _, p := range writes.Puts {
					args := []any{
						slog.String("op", "put"),
						slog.Time("ts", time.UnixMilli(int64(le.Timestamp))),
						slog.String("key", p.Key),
						slog.Int64("offset", le.Offset),
						slog.Int64("term", le.Term),
					}

					if p.ExpectedVersionId != nil {
						args = append(args, slog.Int64("expected-version-id", p.GetExpectedVersionId()))
					}
					if p.ClientIdentity != nil {
						args = append(args, slog.String("client-identity", p.GetClientIdentity()))
					}
					if p.SessionId != nil {
						args = append(args, slog.Int64("session-id", p.GetSessionId()))
					}

					slog.Info("", args...)
				}

				for _, d := range writes.Deletes {
					args := []any{
						slog.String("op", "delete"),
						slog.Time("ts", time.UnixMilli(int64(le.Timestamp))),
						slog.String("key", d.Key),
						slog.Int64("offset", le.Offset),
						slog.Int64("term", le.Term),
					}
					if d.ExpectedVersionId != nil {
						args = append(args, slog.Int64("expected-version-i", d.GetExpectedVersionId()))
					}
					slog.Info("", args...)
				}

				for _, dr := range writes.DeleteRanges {
					slog.Info("",
						slog.String("op", "delete-range"),
						slog.Time("ts", time.UnixMilli(int64(le.Timestamp))),
						slog.String("key-start", dr.StartInclusive),
						slog.String("key-end", dr.EndExclusive),
						slog.Int64("offset", le.Offset),
						slog.Int64("term", le.Term),
					)
				}
			}
		}
	}

	return nil
}
