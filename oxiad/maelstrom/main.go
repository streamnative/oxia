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
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
)

func main() {
	configureLogger()
	// One subdirectory per Maelstrom run (os.Getppid() is the Maelstrom process
	// that spawned every node): a node restarted by the kill nemesis reopens its
	// own data, while separate runs don't share state.
	dataDir := filepath.Join(os.TempDir(), "oxia-maelstrom", strconv.Itoa(os.Getppid()))
	if err := run(bufio.NewScanner(os.Stdin), dataDir); err != nil {
		slog.Error("Failed to run the Maelstrom node", slog.Any("error", err))
		os.Exit(1)
	}
}
