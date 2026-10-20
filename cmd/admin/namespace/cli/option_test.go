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

package cli

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceFieldsAddFlagsCompletesKeySorting(t *testing.T) {
	var fields NamespaceFields
	root := &cobra.Command{Use: "admin"}
	create := &cobra.Command{Use: "create", Run: func(*cobra.Command, []string) {}}
	fields.AddFlags(create)
	root.AddCommand(create)
	out := new(bytes.Buffer)
	root.SetOut(out)
	root.SetErr(new(bytes.Buffer))
	root.SetArgs([]string{cobra.ShellCompRequestCmd, "create", "--key-sorting", ""})

	require.NoError(t, root.Execute())

	assert.Contains(t, out.String(), "hierarchical\tUse file-system like hierarchical sorting based on `/`")
	assert.Contains(t, out.String(), "natural\tUse natural, byte-wise sorting")
	assert.Contains(t, out.String(), ":0")
}
