package crd

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"oxia/operator/resource/crd"
	"strings"
	"testing"
)

func TestCrdInstall(t *testing.T) {
	for _, test := range []struct {
		args              []string
		expectedErr       error
		expectedInvoked   bool
		expectedScope     string
		expectedNamespace string
	}{
		{[]string{"install", "--scope=cluster", "--namespace=myns"}, nil, true, crd.ClusterScope, "myns"},
		{[]string{"install", "--scope=namespaced", "--namespace=myns"}, nil, true, crd.NamespacedScope, "myns"},
		{[]string{"install", "--scope=cluster"}, nil, true, crd.ClusterScope, ""},
		{[]string{"install", "--scope=namespaced"}, errInvalidNamespace, false, "", ""},
		{[]string{"install", "--scope=invalid"}, errInvalidScope, false, "", ""},
		{[]string{"install", "--namespace=myns"}, errInvalidScope, false, "", ""},
		{[]string{"install"}, errInvalidScope, false, "", ""},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			config = crd.Config{}

			Cmd.SetArgs(test.args)
			invoked := false
			installCmd.RunE = func(*cobra.Command, []string) error {
				invoked = true
				assert.Equal(t, test.expectedScope, config.Scope)
				assert.Equal(t, test.expectedNamespace, config.Namespace)
				return nil
			}
			err := Cmd.Execute()
			assert.ErrorIs(t, err, test.expectedErr)
			assert.Equal(t, test.expectedInvoked, invoked)
		})
	}
}

func TestCrdUninstall(t *testing.T) {
	Cmd.SetArgs([]string{"uninstall"})
	invoked := false
	uninstallCmd.RunE = func(*cobra.Command, []string) error {
		invoked = true
		return nil
	}
	err := Cmd.Execute()
	assert.NoError(t, err)
	assert.True(t, invoked)
}
