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

// Command coverignore removes from a Go coverage profile, in place, the blocks
// marked with a "// coverage-ignore: <reason>" comment as their first line.
//
//	coverignore [-trim prefix] profile
//
// A block of the profile starts at its first statement, so a block is marked
// when the marker is in the comment lines right above that statement. A file of
// the profile is read at its name without the prefix, from the current
// directory.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

const marker = "// coverage-ignore"

func main() {
	// coverage-ignore: the process entry point
	if err := run(os.Args[1:]); err != nil {
		// coverage-ignore: terminates the process
		_, _ = fmt.Fprintln(os.Stderr, "coverignore:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("coverignore", flag.ContinueOnError)
	trim := flags.String("trim", "", "the import path prefix to remove from the file names")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: coverignore [-trim prefix] profile")
	}
	path := flags.Arg(0)
	profile, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	filtered, err := filter(profile, os.DirFS("."), *trim)
	if err != nil {
		return err
	}
	return os.WriteFile(path, filtered, 0600)
}

// filter returns the profile without the marked blocks, reading the files of
// the profile from src.
func filter(profile []byte, src fs.FS, trim string) ([]byte, error) {
	var filtered []byte
	sources := map[string][]string{}
	for line := range bytes.Lines(profile) {
		ignored, err := marked(string(line), src, trim, sources)
		if err != nil {
			return nil, err
		}
		if !ignored {
			filtered = append(filtered, line...)
		}
	}
	return filtered, nil
}

// marked reports whether the block of a profile line is marked, caching the
// lines of each file in sources.
func marked(line string, src fs.FS, trim string, sources map[string][]string) (bool, error) {
	if strings.HasPrefix(line, "mode:") {
		return false, nil
	}
	sep := strings.LastIndexByte(line, ':')
	startLine, _, _ := strings.Cut(line[sep+1:], ".")
	start, err := strconv.Atoi(startLine)
	if sep < 0 || err != nil {
		return false, fmt.Errorf("invalid profile line %q", line)
	}
	name := line[:sep]
	lines, ok := sources[name]
	if !ok {
		data, err := fs.ReadFile(src, strings.TrimPrefix(name, trim))
		if err != nil {
			return false, err
		}
		lines = strings.Split(string(data), "\n")
		sources[name] = lines
	}
	if start < 1 || start > len(lines) {
		return false, fmt.Errorf("%s has no line %d", name, start)
	}
	for i := start - 2; i >= 0; i-- {
		comment := strings.TrimSpace(lines[i])
		if strings.HasPrefix(comment, marker) {
			return true, nil
		}
		if !strings.HasPrefix(comment, "//") {
			return false, nil
		}
	}
	return false, nil
}
