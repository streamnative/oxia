package crd

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCrd(t *testing.T) {
	for _, test := range []struct {
		op              string
		expectedErr     error
		expectedInvoked bool
	}{
		{"install", nil, true},
		{"uninstall", nil, true},
	} {
		t.Run(test.op, func(t *testing.T) {
			Cmd.SetArgs([]string{test.op})
			invoked := false
			if test.op == "install" {
				installCmd.RunE = func(*cobra.Command, []string) error {
					invoked = true
					return nil
				}
			} else {
				uninstallCmd.RunE = func(*cobra.Command, []string) error {
					invoked = true
					return nil
				}
			}
			err := Cmd.Execute()
			assert.ErrorIs(t, err, test.expectedErr)
			assert.Equal(t, test.expectedInvoked, invoked)
		})
	}
}
