package controller

import (
	"errors"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"
	"oxia/operator/resource/controller"
)

var (
	config = controller.NewConfig()

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
	errInvalidCpu       = errors.New("cpu must be parsable")
	errInvalidMemory    = errors.New("memory must be parsable")
)

func init() {
	Cmd.AddCommand(installCmd)
	Cmd.AddCommand(uninstallCmd)

	Cmd.PersistentFlags().StringVar(&config.Namespace, "namespace", config.Namespace, "Kubernetes namespace")
	Cmd.PersistentFlags().BoolVar(&config.MonitoringEnabled, "monitoring-enabled", config.MonitoringEnabled, "Prometheus ServiceMonitor")

	installCmd.Flags().StringVar(&config.Image, "image", config.Image, "Container image")
	installCmd.Flags().StringVar(&config.Resources.Cpu, "cpu", config.Resources.Cpu, "Container CPU resources")
	installCmd.Flags().StringVar(&config.Resources.Memory, "memory", config.Resources.Memory, "Container memory resources")
}

func validate(*cobra.Command, []string) error {
	if config.Namespace == "" {
		return errInvalidNamespace
	}
	if _, err := resource.ParseQuantity(config.Resources.Cpu); err != nil {
		return errInvalidCpu
	}
	if _, err := resource.ParseQuantity(config.Resources.Memory); err != nil {
		return errInvalidMemory
	}
	return nil
}

func install(cmd *cobra.Command, _ []string) error {
	return controller.NewClient().Install(cmd.OutOrStdout(), config)
}

func uninstall(cmd *cobra.Command, _ []string) error {
	return controller.NewClient().Uninstall(cmd.OutOrStdout(), config)
}
