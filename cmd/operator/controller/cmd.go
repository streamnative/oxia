package controller

import (
	"errors"
	"github.com/spf13/cobra"
	"oxia/operator/resource/controller"
)

var (
	config = controller.Config{}

	Cmd = &cobra.Command{
		Use:               "crd",
		Short:             "Install or Uninstall CRD",
		Long:              `Install or Uninstall CRD`,
		PersistentPreRunE: validate,
	}

	installCmd = &cobra.Command{
		Use:   "install",
		Short: "Install CRD",
		Long:  `Install CRD`,
		RunE:  install,
	}

	uninstallCmd = &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall CRD",
		Long:  `Uninstall CRD`,
		RunE:  uninstall,
	}

	errInvalidNamespace = errors.New("namespace must be set")
)

func init() {
	Cmd.AddCommand(installCmd)
	Cmd.AddCommand(uninstallCmd)

	Cmd.PersistentFlags().StringVar(&config.Namespace, "namespace", "", "Kubernetes namespace")
	Cmd.PersistentFlags().BoolVar(&config.MonitoringEnabled, "monitoring-enabled", false, "Prometheus ServiceMonitor")
}

func validate(*cobra.Command, []string) error {
	if config.Namespace == "" {
		return errInvalidNamespace
	}
	return nil
}

func install(*cobra.Command, []string) error {
	//TODO to be implemented
	panic("not yet implemented")
}

func uninstall(*cobra.Command, []string) error {
	//TODO to be implemented
	panic("not yet implemented")
}
