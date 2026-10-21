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

package validate

import (
	"bytes"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/oxia-db/oxia/cmd/config/component"
)

var (
	componentName string
	Cmd           = &cobra.Command{
		Use:   "validate FILE",
		Short: "Check a configuration file",
		Long: `Check a configuration file of the data server or the coordinator without starting it: the file must
parse, have only known keys, and hold valid values.`,
		Example: `  oxia config validate --component dataserver conf/dataserver.yaml
  oxia config validate --component coordinator conf/coordinator.yaml`,
		Args: cobra.ExactArgs(1),
		RunE: exec,
	}
)

func init() {
	Cmd.Flags().StringVarP(&componentName, "component", "c", "dataserver", "The component of the configuration file: dataserver or coordinator")
}

func exec(cmd *cobra.Command, args []string) error {
	options, err := component.NewOptions(componentName)
	if err != nil {
		return err
	}
	file := args[0]
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(options); err != nil {
		return fmt.Errorf("invalid %s configuration %s: %w", componentName, file, err)
	}
	options.WithDefault()
	if err := options.Validate(); err != nil {
		return fmt.Errorf("invalid %s configuration %s: %w", componentName, file, err)
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s is a valid %s configuration\n", file, componentName)
	return err
}
