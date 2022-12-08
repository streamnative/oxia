package crd

import (
	"github.com/spf13/cobra"
	"oxia/operator/resource/crd"
)

var (
	Cmd = &cobra.Command{
		Use:   "crd",
		Short: "Install or Uninstall CRD",
		Long:  `Install or Uninstall CRD`,
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
)

func init() {
	Cmd.AddCommand(installCmd)
	Cmd.AddCommand(uninstallCmd)
}

func install(cmd *cobra.Command, _ []string) error {
	return crd.NewClient().Install(cmd.OutOrStdout())
}

func uninstall(cmd *cobra.Command, _ []string) error {
	return crd.NewClient().Uninstall(cmd.OutOrStdout())
}
