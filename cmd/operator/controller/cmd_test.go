package controller

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/operator/resource/controller"
	"strings"
	"testing"
)

func TestControllerInstall(t *testing.T) {
	for _, test := range []struct {
		args                      []string
		expectedErr               error
		expectedInvoked           bool
		expectedNamespace         string
		expectedMonitoringEnabled bool
		expectedImage             string
		expectedCpu               string
		expectedMemory            string
	}{
		{[]string{"install", "--namespace=myns", "--monitoring-enabled"}, nil, true, "myns", true, "oxia:latest", "100m", "128Mi"},
		{[]string{"install", "--namespace=myns"}, nil, true, "myns", false, "oxia:latest", "100m", "128Mi"},
		{[]string{"install"}, errInvalidNamespace, false, "", false, "oxia:latest", "100m", "128Mi"},
		{[]string{"install", "--namespace=myns", "--image=foo", "--cpu=200m", "--memory=256Mi"}, nil, true, "myns", false, "foo", "200m", "256Mi"},
		{[]string{"install", "--namespace=myns", "--cpu=invalid"}, errInvalidCpu, false, "myns", false, "oxia:latest", "100m", "128Mi"},
		{[]string{"install", "--namespace=myns", "--memory=invalid"}, errInvalidMemory, false, "myns", false, "oxia:latest", "100m", "128Mi"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			config = controller.NewConfig()

			Cmd.SetArgs(test.args)
			invoked := false
			installCmd.RunE = func(*cobra.Command, []string) error {
				invoked = true
				assert.Equal(t, test.expectedNamespace, config.Namespace)
				assert.Equal(t, test.expectedMonitoringEnabled, config.MonitoringEnabled)
				return nil
			}
			err := Cmd.Execute()
			assert.ErrorIs(t, err, test.expectedErr)
			assert.Equal(t, test.expectedInvoked, invoked)
		})
	}
}

func TestControllerUninstall(t *testing.T) {
	for _, test := range []struct {
		args                      []string
		expectedErr               error
		expectedInvoked           bool
		expectedNamespace         string
		expectedMonitoringEnabled bool
	}{
		{[]string{"uninstall", "--namespace=myns", "--monitoring-enabled"}, nil, true, "myns", true},
		{[]string{"uninstall", "--namespace=myns"}, nil, true, "myns", false},
		{[]string{"uninstall"}, errInvalidNamespace, false, "", false},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			config = controller.NewConfig()

			Cmd.SetArgs(test.args)
			invoked := false
			uninstallCmd.RunE = func(*cobra.Command, []string) error {
				invoked = true
				assert.Equal(t, test.expectedNamespace, config.Namespace)
				assert.Equal(t, test.expectedMonitoringEnabled, config.MonitoringEnabled)
				return nil
			}
			err := Cmd.Execute()
			assert.ErrorIs(t, err, test.expectedErr)
			assert.Equal(t, test.expectedInvoked, invoked)
		})
	}
}
