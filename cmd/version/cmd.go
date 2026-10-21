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
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/common/version"
)

var (
	output string
	Cmd    = &cobra.Command{
		Use:   "version",
		Short: "Print the version of oxia",
		Long:  `Print the version of oxia, with the commit, the Go version and the platform it was built for.`,
		Example: `  oxia version
  oxia version -o json`,
		Args: cobra.NoArgs,
		RunE: exec,
	}
)

func init() {
	Cmd.Flags().StringVarP(&output, "output", "o", "text", "Output format: text or json")
}

func exec(cmd *cobra.Command, _ []string) error {
	info := version.Get(cmd.Root().Version)
	switch output {
	case "text":
		_, err := fmt.Fprint(cmd.OutOrStdout(), info.String())
		return err
	case "json":
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(info)
	default:
		return fmt.Errorf("unknown output format %q: use text or json", output)
	}
}
