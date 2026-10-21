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

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/oxia-db/oxia/common/process"
)

func main() {
	process.DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia": "main",
		},
		func() {
			if _, err := maxprocs.Set(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := rootCmd.Execute(); err != nil {
				if errors.Is(err, syscall.EADDRINUSE) {
					_, _ = fmt.Fprintln(os.Stderr, "Hint: another process already listens on this address. "+
						"Stop it, or pick another address with the --*-addr flags or in the configuration file.")
				}
				os.Exit(1)
			}
		},
	)
}
