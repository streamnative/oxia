package controller

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/operator/resource/controller"
	"oxia/operator/resource/crd"
	"strings"
	"testing"
)

func TestController(t *testing.T) {
	for _, op := range []string{"install", "uninstall"} {
		for _, test := range []struct {
			args                      []string
			expectedErr               error
			expectedInvoked           bool
			expectedScope             string
			expectedNamespace         string
			expectedMonitoringEnabled bool
		}{
			{[]string{op, "--scope=cluster", "--namespace=myns", "--monitoring-enabled"}, nil, true, crd.ClusterScope, "myns", true},
			{[]string{op, "--scope=cluster", "--namespace=myns"}, nil, true, crd.ClusterScope, "myns", false},
			{[]string{op, "--scope=namespaced", "--namespace=myns"}, nil, true, crd.NamespacedScope, "myns", false},
			{[]string{op, "--scope=cluster"}, errInvalidNamespace, false, "", "", false},
			{[]string{op, "--scope=namespaced"}, errInvalidNamespace, false, "", "", false},
			{[]string{op, "--scope=invalid"}, errInvalidScope, false, "", "", false},
			{[]string{op, "--namespace=myns"}, errInvalidScope, false, "", "", false},
			{[]string{op}, errInvalidScope, false, "", "", false},
		} {
			t.Run(strings.Join(test.args, " "), func(t *testing.T) {
				config = controller.Config{}

				Cmd.SetArgs(test.args)
				invoked := false
				if op == "install" {
					installCmd.RunE = func(*cobra.Command, []string) error {
						invoked = true
						assert.Equal(t, test.expectedScope, config.Scope)
						assert.Equal(t, test.expectedNamespace, config.Namespace)
						assert.Equal(t, test.expectedMonitoringEnabled, config.MonitoringEnabled)
						return nil
					}
				} else {
					uninstallCmd.RunE = func(*cobra.Command, []string) error {
						invoked = true
						assert.Equal(t, test.expectedScope, config.Scope)
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
