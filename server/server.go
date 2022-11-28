package server

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/common"
	"oxia/server/metrics"
)

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int
	MetricsPort         int

	AdvertisedInternalAddress string
	AdvertisedPublicAddress   string
}

type server struct {
	*internalRpcServer
	*PublicRpcServer

	shardsDirector ShardsDirector
	clientPool     common.ClientPool
	metrics        *metrics.PrometheusMetrics
}

func newServer(config serverConfig) (*server, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia server")

	s := &server{
		clientPool: common.NewClientPool(),
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	advertisedInternalAddress := config.AdvertisedInternalAddress
	if advertisedInternalAddress == "" {
		advertisedInternalAddress = hostname
	}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		advertisedPublicAddress = hostname
	}
	log.Info().Msgf("AdvertisedPublicAddress %s", advertisedPublicAddress)

	identityAddr := fmt.Sprintf("%s:%d", advertisedInternalAddress, config.InternalServicePort)
	s.shardsDirector = NewShardsDirector(identityAddr)

	s.internalRpcServer, err = newCoordinationRpcServer(config.InternalServicePort, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = NewPublicRpcServer(config.PublicServicePort, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	s.metrics, err = metrics.Start(config.MetricsPort)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Close() error {
	var errs error
	if err := s.PublicRpcServer.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := s.internalRpcServer.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := s.clientPool.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := s.shardsDirector.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	if err := s.metrics.Close(); err != nil {
		errs = multierr.Append(errs, err)
	}
	return errs
}
