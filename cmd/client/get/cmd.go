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

package get

import (
	"context"
	"github.com/streamnative/oxia/server"
	"log/slog"
	"time"

	"github.com/pkg/errors"

	"github.com/streamnative/oxia/oxia"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
)

var (
	Config = flags{}
)

type flags struct {
	key                string
	hexDump            bool
	includeVersion     bool
	partitionKey       string
	comparisonType     string
	ephemeralShadowKey bool
}

func (flags *flags) Reset() {
	flags.key = ""
	flags.hexDump = false
	flags.includeVersion = false
	flags.partitionKey = ""
	flags.comparisonType = "equal"
	flags.ephemeralShadowKey = false
}

func init() {
	Cmd.Flags().BoolVarP(&Config.includeVersion, "include-version", "v", false, "Include the record version object")
	Cmd.Flags().BoolVar(&Config.hexDump, "hex", false, "Print the value in HexDump format")
	Cmd.Flags().BoolVar(&Config.ephemeralShadowKey, "ephemeral-shadow-key", false, "Print the ephemeral shadow key")
	Cmd.Flags().StringVarP(&Config.partitionKey, "partition-key", "p", "", "Partition Key to be used in override the shard routing")

	Cmd.Flags().StringVarP(&Config.comparisonType, "comparison-type", "t", "equal",
		"The type of get comparison. Allowed value: equal, floor, ceiling, lower, higher")
}

var Cmd = &cobra.Command{
	Use:   "get [flags] KEY",
	Short: "Get one record",
	Long:  `Get the values of the recover associated with the given key.`,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	var options []oxia.GetOption
	if Config.partitionKey != "" {
		options = append(options, oxia.PartitionKey(Config.partitionKey))
	}

	switch Config.comparisonType {
	case "equal":
		// Nothing to do, this is default
	case "floor":
		options = append(options, oxia.ComparisonFloor())
	case "ceiling":
		options = append(options, oxia.ComparisonCeiling())
	case "lower":
		options = append(options, oxia.ComparisonLower())
	case "higher":
		options = append(options, oxia.ComparisonHigher())
	default:
		return errors.Errorf("invalid comparison type: %s", Config.comparisonType)
	}

	queryKeys := args

	for _, key := range queryKeys {
		key, value, version, err := client.Get(context.Background(), key, options...)
		if err != nil {
			return err
		}
		output(cmd, key, value, version)
		if Config.ephemeralShadowKey && version.Ephemeral {
			originKey := key
			shadowKey := server.ShadowKey(server.SessionId(version.SessionId), originKey)
			key, value, version, err := client.Get(context.Background(), shadowKey, options...)
			if err != nil {
				slog.Warn("Failed to get shadow key", slog.String("originKey", originKey),
					slog.String("shadowKey", shadowKey))
			}
			output(cmd, key, value, version)
		}
	}
	return nil
}

func output(cmd *cobra.Command, key string, value []byte, version oxia.Version) {
	if Config.hexDump {
		common.WriteHexDump(cmd.OutOrStdout(), value)
	} else {
		common.WriteOutput(cmd.OutOrStdout(), value)
	}

	if Config.includeVersion {
		_, _ = cmd.OutOrStdout().Write([]byte("---\n"))
		common.WriteOutput(cmd.OutOrStdout(), common.OutputVersion{
			Key:                key,
			VersionId:          version.VersionId,
			CreatedTimestamp:   time.UnixMilli(int64(version.CreatedTimestamp)),
			ModifiedTimestamp:  time.UnixMilli(int64(version.ModifiedTimestamp)),
			ModificationsCount: version.ModificationsCount,
			Ephemeral:          version.Ephemeral,
			SessionId:          version.SessionId,
			ClientIdentity:     version.ClientIdentity,
		})
	}
}
