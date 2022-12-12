package main

import (
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/common"
	"testing"
)

func TestCall_LogLevel(t *testing.T) {
	tests := []struct {
		name          string
		level         string
		expectedErr   error
		expectedLevel zerolog.Level
	}{
		{"disabled", "disabled", nil, zerolog.Disabled},
		{"trace", "trace", nil, zerolog.TraceLevel},
		{"debug", "debug", nil, zerolog.DebugLevel},
		{"info", "info", nil, zerolog.InfoLevel},
		{"warn", "warn", nil, zerolog.WarnLevel},
		{"error", "error", nil, zerolog.ErrorLevel},
		{"fatal", "fatal", nil, zerolog.FatalLevel},
		{"panic", "panic", nil, zerolog.PanicLevel},
		{"junk", "junk", LogLevelError("junk"), zerolog.InfoLevel},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run(test.name, func(t *testing.T) {
				logLevelStr = ""
				rootCmd.SetArgs(append([]string{"-g"}, test.level))
				rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
					if test.expectedErr == nil {
						assert.Equal(t, test.expectedLevel, common.LogLevel)
					}
					return nil
				}
				err := rootCmd.Execute()
				assert.ErrorIs(t, test.expectedErr, err)
			})
		})
	}
}
