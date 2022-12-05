package controller

import (
	"github.com/spf13/cobra"
	"io"
	"oxia/cmd/flag"
	"oxia/common"
	"oxia/controller"
)

var (
	conf = controller.Config{}

	Cmd = &cobra.Command{
		Use:   "controller",
		Short: "Start a controller",
		Long:  `Start a controller`,
		Run:   exec,
	}
)

func init() {
	flag.InternalPort(Cmd, &conf.InternalServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return controller.New(conf)
	})
}
