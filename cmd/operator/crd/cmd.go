package crd

import (
	"github.com/spf13/cobra"
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

func install(*cobra.Command, []string) error {
	//TODO to be implemented
	panic("not yet implemented")
}

func uninstall(*cobra.Command, []string) error {
	//TODO to be implemented
	panic("not yet implemented")
}
