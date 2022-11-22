package standalone

import (
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/server/kv"
	"oxia/server/wal"
)

type standaloneConfig struct {
	PublicServicePort       uint32
	AdvertisedPublicAddress string
	NumShards               uint32
	DataDir                 string
	WalDir                  string
}

type standalone struct {
	rpc        *StandaloneRpcServer
	kvFactory  kv.KVFactory
	walFactory wal.WalFactory
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

	s.walFactory = wal.NewWalFactory(&wal.WalFactoryOptions{
		LogDir: config.WalDir,
	})

	s.kvFactory = kv.NewPebbleKVFactory(&kv.KVFactoryOptions{
		DataDir: config.DataDir,
	})

	s.rpc, err = NewStandaloneRpcServer(int(config.PublicServicePort), advertisedPublicAddress, config.NumShards, s.walFactory, s.kvFactory)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *standalone) Close() error {
	return multierr.Combine(
		s.rpc.Close(),
		s.kvFactory.Close(),
	)
}
