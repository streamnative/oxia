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

package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/pflag"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/oxia"
)

type Executor struct {
	Ctx          context.Context
	Client       func() (oxia.AdminClient, error)
	Out          io.Writer
	OutputFormat string
}

func (e Executor) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: admin namespace|dataserver|shard COMMAND [ARGS...]")
	}

	switch args[0] {
	case "namespace", "namespaces":
		return e.namespace(args[1:])
	case "dataserver", "dataservers", "data-server", "data-servers":
		return e.dataServer(args[1:])
	case "shard", "shards":
		return e.shard(args[1:])
	default:
		return fmt.Errorf("unknown admin command %q", args[0])
	}
}

func (e Executor) parseOutputCommand(args []string, usage string) (string, error) {
	flags, outputFormat := e.newFlagSet(args[0])
	if err := flags.Parse(args[1:]); err != nil {
		return "", fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 0 {
		return "", fmt.Errorf("usage: %s", usage)
	}
	if err := admincommons.ValidateOutputFormat(*outputFormat); err != nil {
		return "", err
	}
	return *outputFormat, nil
}

func (e Executor) parseGetCommand(args []string, usage string) (outputFormat string, name string, err error) {
	flags, output := e.newFlagSet(args[0])
	if err := flags.Parse(args[1:]); err != nil {
		return "", "", fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 1 {
		return "", "", fmt.Errorf("usage: %s", usage)
	}

	if err := admincommons.ValidateOutputFormat(*output); err != nil {
		return "", "", err
	}
	name = strings.TrimSpace(flags.Arg(0))
	if name == "" {
		return "", "", fmt.Errorf("usage: %s", usage)
	}
	return *output, name, nil
}

// newFlagSet returns the flags of an admin command, and the output format that
// they set.
func (e Executor) newFlagSet(name string) (flags *pflag.FlagSet, outputFormat *string) {
	flags = pflag.NewFlagSet(name, pflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags, flags.StringP("output", "o", e.OutputFormat, "")
}
