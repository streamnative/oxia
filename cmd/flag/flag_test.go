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

package flag

import (
	"fmt"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/common/option"
)

func TestPublicAddrRegistersFlag(t *testing.T) {
	assertAddrFlag(t, PublicAddr, "public-addr", "p", fmt.Sprintf("0.0.0.0:%d", constant.DefaultPublicPort))
}

func TestInternalAddrRegistersFlag(t *testing.T) {
	assertAddrFlag(t, InternalAddr, "internal-addr", "i", fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort))
}

func TestMetricsAddrRegistersFlag(t *testing.T) {
	assertAddrFlag(t, MetricsAddr, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", option.DefaultMetricsPort))
}

func TestAdminAddrRegistersFlag(t *testing.T) {
	assertAddrFlag(t, AdminAddr, "admin-addr", "a", fmt.Sprintf("0.0.0.0:%d", constant.DefaultAdminPort))
}

func assertAddrFlag(t *testing.T, register func(*cobra.Command, *string), name, shorthand, defaultValue string) {
	t.Helper()
	var addr string
	cmd := &cobra.Command{Use: "test"}

	register(cmd, &addr)

	assert.Equal(t, defaultValue, addr)
	flag := cmd.Flags().Lookup(name)
	require.NotNil(t, flag)
	assert.Equal(t, shorthand, flag.Shorthand)
	require.NoError(t, cmd.Flags().Parse([]string{"-" + shorthand, "localhost:1234"}))
	assert.Equal(t, "localhost:1234", addr)
}
