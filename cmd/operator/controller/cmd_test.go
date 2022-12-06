package controller

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/operator/resource/controller"
	"strings"
	"testing"
)

func TestController(t *testing.T) {
	for _, op := range []string{"install", "uninstall"} {
		for _, test := range []struct {
			args                      []string
			expectedErr               error
			expectedInvoked           bool
			expectedNamespace         string
			expectedMonitoringEnabled bool
		}{
			{[]string{op, "--namespace=myns", "--monitoring-enabled"}, nil, true, "myns", true},
			{[]string{op, "--namespace=myns"}, nil, true, "myns", false},
			{[]string{op}, errInvalidNamespace, false, "", false},
		} {
			t.Run(strings.Join(test.args, " "), func(t *testing.T) {
				config = controller.Config{}

				Cmd.SetArgs(test.args)
				invoked := false
				if op == "install" {
					installCmd.RunE = func(*cobra.Command, []string) error {
						invoked = true
						assert.Equal(t, test.expectedNamespace, config.Namespace)
						assert.Equal(t, test.expectedMonitoringEnabled, config.MonitoringEnabled)
						return nil
					}
				} else {
					uninstallCmd.RunE = func(*cobra.Command, []string) error {
						invoked = true
						assert.Equal(t, test.expectedNamespace, config.Namespace)
						assert.Equal(t, test.expectedMonitoringEnabled, config.MonitoringEnabled)
						return nil
					}
				}
				err := Cmd.Execute()
				assert.ErrorIs(t, err, test.expectedErr)
				assert.Equal(t, test.expectedInvoked, invoked)
			})
		}
	}
}
