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
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

const source = `package p

func f(ok bool) {
	// coverage-ignore: the reason, which
	// takes two lines
	a()
	if ok {
		// a comment
		b()
	}
	// a comment
	c()
}
`

const profile = `mode: atomic
example.com/m/p/p.go:6.2,7.8 2 0
example.com/m/p/p.go:9.3,10.3 1 0
example.com/m/p/p.go:12.2,12.5 1 1
`

const filtered = `mode: atomic
example.com/m/p/p.go:9.3,10.3 1 0
example.com/m/p/p.go:12.2,12.5 1 1
`

func TestRunFiltersProfileInPlace(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "p", "p.go"), source)
	writeFile(t, filepath.Join(dir, "coverage.out"), profile)
	t.Chdir(dir)

	if err := run([]string{"-trim", "example.com/m/", "coverage.out"}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("coverage.out")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != filtered {
		t.Fatalf("got:\n%s\nwant:\n%s", got, filtered)
	}
}

func TestRunRejectsUnknownFlag(t *testing.T) {
	if err := run([]string{"-unknown", "coverage.out"}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestRunRequiresOneProfile(t *testing.T) {
	if err := run(nil); err == nil {
		t.Fatal("expected an error")
	}
}

func TestRunFailsOnMissingProfile(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := run([]string{"coverage.out"}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestRunKeepsProfileOnMissingSource(t *testing.T) {
	t.Chdir(t.TempDir())
	writeFile(t, "coverage.out", profile)

	if err := run([]string{"coverage.out"}); err == nil {
		t.Fatal("expected an error")
	}
	got, err := os.ReadFile("coverage.out")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != profile {
		t.Fatalf("the profile changed:\n%s", got)
	}
}

func TestFilterRejectsInvalidLine(t *testing.T) {
	for _, line := range []string{"p.go 6.2,7.8 2 0\n", "p.go:x.2,7.8 2 0\n"} {
		if _, err := filter([]byte(line), fstest.MapFS{}, ""); err == nil {
			t.Fatalf("expected an error for %q", line)
		}
	}
}

func TestFilterRejectsLineOutsideSource(t *testing.T) {
	src := fstest.MapFS{"p.go": {Data: []byte(source)}}
	for _, line := range []string{"p.go:0.1,1.1 1 0\n", "p.go:20.1,21.1 1 0\n"} {
		if _, err := filter([]byte(line), src, ""); err == nil {
			t.Fatalf("expected an error for %q", line)
		}
	}
}

func TestFilterKeepsBlockUnderFileComment(t *testing.T) {
	src := fstest.MapFS{"p.go": {Data: []byte("// a comment\nvar v = func() {}\n")}}
	line := "p.go:2.1,2.18 1 0\n"
	got, err := filter([]byte(line), src, "")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != line {
		t.Fatalf("got %q, want %q", got, line)
	}
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}
