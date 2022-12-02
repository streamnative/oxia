package server

import (
	"github.com/spf13/cobra"
	"io"
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
	Cmd.Flags().IntVarP(&conf.PublicServicePort, "public-port", "p", 9190, "Public service port")
	Cmd.Flags().IntVarP(&conf.InternalServicePort, "internal-port", "i", 8190, "Internal service port")
	Cmd.Flags().IntVarP(&conf.MetricsPort, "metrics-port", "m", 8080, "Metrics port")
	Cmd.Flags().StringVar(&conf.AdvertisedPublicAddress, "advertised-public-address", "", "Advertised public address")
}

func exec(*cobra.Command, []string) {
	common.RunProcess(func() (io.Closer, error) {
		return server.New(conf)
	})
}
