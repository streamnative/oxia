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

package config

import (
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/config/createschema"
	"github.com/oxia-db/oxia/cmd/config/validate"
)

var (
	Cmd = &cobra.Command{
		Use:   "config",
		Short: "Configuration tools",
		Long:  `Tools to generate and check the configuration files of the coordinator and the data server.`,
	}
)

func init() {
	Cmd.AddCommand(createschema.Cmd)
	Cmd.AddCommand(validate.Cmd)
}
