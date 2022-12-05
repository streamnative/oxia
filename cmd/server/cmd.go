package server

import (
	"github.com/spf13/cobra"
	"io"
	"oxia/cmd/flag"
	"oxia/common"
	"oxia/server"
)

var (
	conf = server.Config{}

	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	flag.PublicPort(Cmd, &conf.PublicServicePort)
	flag.InternalPort(Cmd, &conf.InternalServicePort)
	flag.MetricsPort(Cmd, &conf.MetricsPort)
	Cmd.Flags().StringVar(&conf.AdvertisedPublicAddress, "advertised-public-address", "", "Advertised public address")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return server.New(conf)
	})
}
