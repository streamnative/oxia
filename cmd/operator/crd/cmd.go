package crd

import (
	"errors"
	"github.com/spf13/cobra"
	"oxia/operator/resource/crd"
)

var (
	config = crd.Config{}

	Cmd = &cobra.Command{
		Use:   "crd",
		Short: "Install or Uninstall CRD",
		Long:  `Install or Uninstall CRD`,
	}

	installCmd = &cobra.Command{
		Use:     "install",
		Short:   "Install CRD",
		Long:    `Install CRD`,
		PreRunE: validate,
		RunE:    install,
	}

	uninstallCmd = &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall CRD",
		Long:  `Uninstall CRD`,
		RunE:  uninstall,
	}

	errInvalidScope     = errors.New("scope must be set and one of \"cluster\" or \"namespaced\"")
	errInvalidNamespace = errors.New("namespace must be set if scope is set")
)

func init() {
	Cmd.AddCommand(installCmd)
	Cmd.AddCommand(uninstallCmd)

	installCmd.Flags().StringVar(&config.Scope, "scope", "", "CRD scope, one of \"cluster\" or \"namespaced\"")
	installCmd.Flags().StringVar(&config.Namespace, "namespace", "", "Kubernetes namespace")
}

func validate(*cobra.Command, []string) error {
	if config.Scope != crd.ClusterScope && config.Scope != crd.NamespacedScope {
		return errInvalidScope
	}
	if config.Scope == crd.NamespacedScope && config.Namespace == "" {
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
