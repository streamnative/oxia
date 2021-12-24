package standalone

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"oxia/operator"
	"oxia/proto"
	"oxia/server"
)

type standaloneConfig struct {
	PublicServicePort       uint32
	AdvertisedPublicAddress string
	NumShards               uint32
}

type standalone struct {
	rpc *server.PublicRpcServer

	shardsManager           server.ShardsManager
	identityInternalAddress proto.ServerAddress
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
	s.shardsManager = server.NewShardsManager(identityAddr)

	cs := operator.ComputeAssignments([]*proto.ServerAddress{{
		InternalUrl: identityAddr,
		PublicUrl:   identityAddr,
	}}, 1, conf.NumShards)
	s.shardsManager.UpdateClusterStatus(cs)

	s.rpc, err = server.NewPublicRpcServer(int(config.PublicServicePort), advertisedPublicAddress, s.shardsManager)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *standalone) Close() error {
	if err := s.rpc.Close(); err != nil {
		return err
	}

	if err := s.shardsManager.Close(); err != nil {
		return err
	}

	return nil
}
