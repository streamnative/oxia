package standalone

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"oxia/proto"
	"oxia/server/kv"
)

type standaloneConfig struct {
	PublicServicePort       uint32
	AdvertisedPublicAddress string
	NumShards               uint32
	DataDir                 string
}

type standalone struct {
	rpc       *StandaloneRpcServer
	kvFactory kv.KVFactory

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

	s.kvFactory = kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir: config.DataDir,
	})

	s.rpc, err = NewStandaloneRpcServer(int(config.PublicServicePort), identityAddr, config.NumShards, s.kvFactory)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *standalone) Close() error {
	if err := s.rpc.Close(); err != nil {
		return err
	}

	if err := s.kvFactory.Close(); err != nil {
		return err
	}

	return nil
}
