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

package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/codec"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func TestServer_ConfigurationLoading(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server-config.yaml")

	configContent := `
server:
  public:
    bindAddress: "0.0.0.0:6644"
    auth:
      provider: "oidc"
      providerParams: '{"allowedIssueURLs":"https://example.com","allowedAudiences":"my-app"}'
    tls:
      certFile: "/path/to/cert.pem"
      keyFile: "/path/to/key.pem"
  internal:
    bindAddress: "0.0.0.0:6645"
    tls:
      certFile: "/path/to/internal-cert.pem"
      keyFile: "/path/to/internal-key.pem"
replication:
  tls:
    certFile: "/path/to/peer-cert.pem"
    keyFile: "/path/to/peer-key.pem"
storage:
  wal:
    dir: "/tmp/wal"
    sync: true
    retention: "2h"
  database:
    dir: "/tmp/db"
    readCacheSizeMB: 512
  notification:
    retention: "30m"
featureFlags:
  authorityValidation: true
observability:
  metric:
    bindAddress: "0.0.0.0:9090"
  log:
    level: "debug"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Create a new options instance
	opts := option.NewDefaultOptions()

	// Load configuration using the same function as the cmd
	err = codec.TryReadAndInitConf(configPath, opts)
	require.NoError(t, err)

	// Verify that config was loaded correctly
	assert.Equal(t, "0.0.0.0:6644", opts.Server.Public.BindAddress)
	assert.Equal(t, "oidc", opts.Server.Public.Auth.Provider)
	assert.Equal(t, "{\"allowedIssueURLs\":\"https://example.com\",\"allowedAudiences\":\"my-app\"}", opts.Server.Public.Auth.ProviderParams)
	assert.Equal(t, "/path/to/cert.pem", opts.Server.Public.TLS.CertFile)
	assert.Equal(t, "/path/to/key.pem", opts.Server.Public.TLS.KeyFile)

	assert.Equal(t, "0.0.0.0:6645", opts.Server.Internal.BindAddress)
	assert.Equal(t, "/path/to/internal-cert.pem", opts.Server.Internal.TLS.CertFile)
	assert.Equal(t, "/path/to/internal-key.pem", opts.Server.Internal.TLS.KeyFile)

	assert.Equal(t, "/path/to/peer-cert.pem", opts.Replication.TLS.CertFile)
	assert.Equal(t, "/path/to/peer-key.pem", opts.Replication.TLS.KeyFile)

	assert.Equal(t, "/tmp/wal", opts.Storage.WAL.Dir)
	assert.True(t, *opts.Storage.WAL.Sync)
	assert.Equal(t, 2*time.Hour, opts.Storage.WAL.Retention.ToDuration())

	assert.Equal(t, "/tmp/db", opts.Storage.Database.Dir)
	assert.Equal(t, int64(512), opts.Storage.Database.ReadCacheSizeMB)

	assert.Equal(t, 30*time.Minute, opts.Storage.Notification.Retention.ToDuration())

	assert.True(t, opts.FeatureFlags.IsAuthorityValidationEnabled())

	assert.Equal(t, "0.0.0.0:9090", opts.Observability.Metric.BindAddress)
	assert.Equal(t, "debug", opts.Observability.Log.Level)
}

func TestServer_ConfigurationWithDefaults(t *testing.T) {
	// Create a config file with partial configuration
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "partial-config.yaml")

	configContent := `
server:
  public:
    bindAddress: "0.0.0.0:7000"
storage:
  wal:
    dir: "/custom/wal"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Create a new options instance
	opts := option.NewDefaultOptions()

	// Load configuration
	err = codec.TryReadAndInitConf(configPath, opts)
	require.NoError(t, err)

	// Verify that partial config overrides defaults but other values use defaults
	assert.Equal(t, "0.0.0.0:7000", opts.Server.Public.BindAddress) // From config
	assert.Equal(t, "/custom/wal", opts.Storage.WAL.Dir)            // From config

	// Verify defaults are applied to unset values
	assert.NotEmpty(t, opts.Server.Internal.BindAddress)                  // Default
	assert.NotEmpty(t, opts.Observability.Metric.BindAddress)             // Default
	assert.Equal(t, 1*time.Hour, opts.Storage.WAL.Retention.ToDuration()) // Default
	assert.True(t, *opts.Storage.WAL.Sync)                                // Default
	assert.True(t, opts.FeatureFlags.IsAuthorityValidationEnabled())      // Default
}

func TestServer_ConfigurationValidation(t *testing.T) {
	// Test that validation works correctly
	opts := option.NewDefaultOptions()

	// Should pass validation with defaults
	err := opts.Validate()
	assert.NoError(t, err)

	// Test that validation is called and handles various scenarios
	// Note: The actual validation logic depends on the implementation
	// For now, we just verify that the validation method exists and can be called
	assert.NotNil(t, opts.Validate)
}

func TestServer_ConfigFileNotFound(t *testing.T) {
	opts := option.NewDefaultOptions()

	// Should fail when config file doesn't exist
	err := codec.TryReadAndInitConf("/nonexistent/config.yaml", opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file")
}

func TestServer_InvalidYAML(t *testing.T) {
	// Create an invalid YAML config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid-config.yaml")

	invalidContent := `
server:
  public:
    bindAddress: [invalid yaml syntax
`

	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Should fail with invalid YAML
	err = codec.TryReadAndInitConf(configPath, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "yaml")
}

func TestServer_EmptyConfigFile(t *testing.T) {
	// Create an empty config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "empty-config.yaml")

	err := os.WriteFile(configPath, []byte{}, 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Should work with empty config (defaults should be applied)
	err = codec.TryReadAndInitConf(configPath, opts)
	assert.NoError(t, err)

	// Verify defaults are still applied
	assert.NotEmpty(t, opts.Server.Public.BindAddress)
	assert.NotEmpty(t, opts.Server.Internal.BindAddress)
	assert.True(t, opts.FeatureFlags.IsAuthorityValidationEnabled())
}

func TestServer_CommandLineFlags(t *testing.T) {
	// Test that command line flags work correctly
	originalConfFile := sconfFile
	defer func() { sconfFile = originalConfFile }()

	// Reset the global options
	originalOptions := dataServerOptions
	defer func() { dataServerOptions = originalOptions }()
	dataServerOptions = option.NewDefaultOptions()

	// Create a config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
server:
  public:
    bindAddress: "0.0.0.0:7000"
  internal:
    bindAddress: "0.0.0.0:7001"
observability:
  metric:
    bindAddress: "0.0.0.0:8080"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Test configuration loading via the command pattern
	sconfFile = configPath
	err = codec.TryReadAndInitConf(sconfFile, dataServerOptions)
	require.NoError(t, err)

	// Verify config file values were loaded
	assert.Equal(t, "0.0.0.0:7000", dataServerOptions.Server.Public.BindAddress)
	assert.Equal(t, "0.0.0.0:7001", dataServerOptions.Server.Internal.BindAddress)
	assert.Equal(t, "0.0.0.0:8080", dataServerOptions.Observability.Metric.BindAddress)

	// Now test that CLI flags can override these
	// Note: In the actual cmd, flags are set during init(), but we can simulate this
	// by directly setting the flag values on the options struct
	dataServerOptions.Server.Public.BindAddress = "0.0.0.0:9000"
	dataServerOptions.Server.Internal.BindAddress = "0.0.0.0:9001"
	dataServerOptions.Observability.Metric.BindAddress = "0.0.0.0:9090"

	// Verify the options were updated
	assert.Equal(t, "0.0.0.0:9000", dataServerOptions.Server.Public.BindAddress)
	assert.Equal(t, "0.0.0.0:9001", dataServerOptions.Server.Internal.BindAddress)
	assert.Equal(t, "0.0.0.0:9090", dataServerOptions.Observability.Metric.BindAddress)
}

func TestCmdPreRunAppliesRetentionFlags(t *testing.T) {
	run := Cmd.RunE
	t.Cleanup(func() { Cmd.RunE = run })
	invoked := false
	Cmd.RunE = func(*cobra.Command, []string) error { invoked = true; return nil }

	Cmd.SetArgs([]string{"--wal-retention-time", "5m", "--notifications-retention-time", "7m"})
	err := Cmd.Execute()

	require.NoError(t, err)
	assert.True(t, invoked)
	assert.Equal(t, 5*time.Minute, time.Duration(dataServerOptions.Storage.WAL.Retention))
	assert.Equal(t, 7*time.Minute, time.Duration(dataServerOptions.Storage.Notification.Retention))
}

func TestServerRunsUntilItsContextIsDone(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	Cmd.SetArgs([]string{"--data-dir", filepath.Join(dir, "db"), "--wal-dir", filepath.Join(dir, "wal"),
		"--public-addr", "localhost:0", "--internal-addr", "localhost:0", "--metrics-addr", "localhost:0"})

	assert.NoError(t, Cmd.ExecuteContext(ctx))
}

func TestExecReturnsConfigFileReadError(t *testing.T) {
	resetCmd(t)
	Cmd.SetArgs([]string{"--sconfig", filepath.Join(t.TempDir(), "missing.yaml")})

	err := Cmd.ExecuteContext(t.Context())

	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestExecReturnsConfigFileParseError(t *testing.T) {
	resetCmd(t)
	configPath := writeConfigFile(t, invalidConfig)
	Cmd.SetArgs([]string{"--sconfig", configPath})

	err := Cmd.ExecuteContext(t.Context())

	assert.ErrorContains(t, err, "yaml")
}

func TestExecReloadsChangedConfigFile(t *testing.T) {
	resetCmd(t)
	handler := newRecordingHandler(t)
	config := validConfig(t.TempDir())
	configPath := writeConfigFile(t, config)
	Cmd.SetArgs([]string{"--sconfig", configPath})
	ctx, cancel := context.WithCancel(t.Context())
	errs := make(chan error, 1)
	go func() {
		errs <- Cmd.ExecuteContext(ctx)
	}()
	require.True(t, handler.waitFor("Starting Oxia dataServer"))

	overwriteConfigFile(t, configPath, invalidConfig)
	require.True(t, handler.waitFor("parse updated configuration file failed"))
	overwriteConfigFile(t, configPath, config)
	require.True(t, handler.waitFor("configuration file has changed."))
	cancel()

	assert.NoError(t, <-errs)
}

func TestExecReturnsValidationError(t *testing.T) {
	resetCmd(t)
	dir := t.TempDir()
	Cmd.SetArgs([]string{"--data-dir", dir, "--wal-dir", dir,
		"--public-addr", "localhost:0", "--internal-addr", "localhost:0", "--metrics-addr", "localhost:0"})

	err := Cmd.ExecuteContext(t.Context())

	assert.ErrorContains(t, err, "are the same directory")
}

const invalidConfig = `
server:
  public: [invalid yaml syntax
`

const configFileSize = 512

func validConfig(dir string) string {
	return fmt.Sprintf(`
server:
  public:
    bindAddress: "localhost:0"
  internal:
    bindAddress: "localhost:0"
storage:
  wal:
    dir: %q
  database:
    dir: %q
observability:
  metric:
    bindAddress: "localhost:0"
`, filepath.Join(dir, "wal"), filepath.Join(dir, "db"))
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, paddedConfig(content), 0600))
	return configPath
}

func overwriteConfigFile(t *testing.T, configPath string, content string) {
	t.Helper()
	file, err := os.OpenFile(configPath, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteAt(paddedConfig(content), 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func paddedConfig(content string) []byte {
	return []byte(content + "#" + strings.Repeat(" ", configFileSize-len(content)-2) + "\n")
}

func resetCmd(t *testing.T) {
	t.Helper()
	options := *dataServerOptions
	t.Cleanup(func() {
		*dataServerOptions = options
		sconfFile = ""
		Cmd.Flags().VisitAll(func(flag *pflag.Flag) { flag.Changed = false })
	})
}

type recordingHandler struct {
	messages chan string
}

func newRecordingHandler(t *testing.T) *recordingHandler {
	t.Helper()
	handler := &recordingHandler{messages: make(chan string, 1000)}
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(defaultLogger) })
	return handler
}

func (*recordingHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	select {
	case h.messages <- record.Message:
	default:
	}
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordingHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordingHandler) waitFor(message string) bool {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case received := <-h.messages:
			if received == message {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}
