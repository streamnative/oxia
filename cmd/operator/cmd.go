package operator

import (
	"github.com/spf13/cobra"
	"oxia/cmd/controller"
	"oxia/cmd/operator/crd"
)

var (
	Cmd = &cobra.Command{
		Use:   "operator",
		Short: "Install or Uninstall CRD and controller",
		Long:  `Install or Uninstall CRD and controller`,
	}
)

func init() {
	Cmd.AddCommand(crd.Cmd)
	Cmd.AddCommand(controller.Cmd)
}
