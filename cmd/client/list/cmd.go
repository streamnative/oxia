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

package list

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/oxia"

	"github.com/streamnative/oxia/cmd/client/common"
)

var (
	Config = flags{}
)

type flags struct {
	keyMin string
	keyMax string
}

func (flags *flags) Reset() {
	flags.keyMin = ""
	flags.keyMax = ""
}

func init() {
	Cmd.Flags().StringVarP(&Config.keyMin, "key-min", "s", "", "Key range minimum (inclusive)")
	Cmd.Flags().StringVarP(&Config.keyMax, "key-max", "e", "", "Key range maximum (exclusive)")
}

var Cmd = &cobra.Command{
	Use:   "list",
	Short: "List keys",
	Long:  `List keys that fall within the given key range.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	var options []oxia.ListOption
	if Config.keyMax == "" {
		// By default, do not list internal keys
		Config.keyMax = "__oxia/"
	}
	list, err := client.List(context.Background(), Config.keyMin, Config.keyMax, options...)
	if err != nil {
		return err
	}

	common.WriteOutput(cmd.OutOrStdout(), list)
	return nil
}
