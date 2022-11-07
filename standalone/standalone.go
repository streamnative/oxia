package standalone

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"oxia/coordination"
	"oxia/operator"
	"oxia/server"
)

type standaloneConfig struct {
	PublicServicePort       uint32
	AdvertisedPublicAddress string
	NumShards               uint32
}

type standalone struct {
	rpc *server.PublicRpcServer

	shardsDirector          server.ShardsDirector
	identityInternalAddress coordination.ServerAddress
}

func NewStandalone(config *standaloneConfig) (*standalone, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia standalone")

	s := &standalone{}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	advertisedPublicAddress := config.AdvertisedPublicAddress
	if advertisedPublicAddress == "" {
		advertisedPublicAddress = hostname
	}

	identityAddr := fmt.Sprintf("%s:%d", advertisedPublicAddress, config.PublicServicePort)
	s.shardsDirector = server.NewShardsDirector(identityAddr)

	_ = operator.ComputeAssignments([]*coordination.ServerAddress{{
		InternalUrl: identityAddr,
		PublicUrl:   identityAddr,
	}}, 1, conf.NumShards)
	// TODO bootstrap shards in s.shardsDirector

	s.rpc, err = server.NewPublicRpcServer(int(config.PublicServicePort), advertisedPublicAddress, s.shardsDirector)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *standalone) Close() error {
	if err := s.rpc.Close(); err != nil {
		return err
	}

	if err := s.shardsDirector.Close(); err != nil {
		return err
	}

	return nil
}
