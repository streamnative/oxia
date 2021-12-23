package main

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"oxia/common"
	"oxia/proto"
)

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int

	AdvertisedInternalAddress string
	AdvertisedPublicAddress   string
}

type server struct {
	*internalRpcServer
	*publicRpcServer

	shardsManager  ShardsManager
	connectionPool common.ClientPool

	identityInternalAddress proto.ServerAddress
}

func NewServer(config *serverConfig) (*server, error) {
	b, _ := json.Marshal(config)
	log.Info().
		RawJSON("config", b).
		Msg("Starting Oxia server")

	s := &server{
		connectionPool: common.NewClientPool(),
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

	identityAddr := fmt.Sprintf("%s:%d", advertisedInternalAddress, config.InternalServicePort)
	s.shardsManager = NewShardsManager(s.connectionPool, identityAddr)

	s.internalRpcServer, err = newInternalRpcServer(config.InternalServicePort, advertisedInternalAddress, s.shardsManager)
	if err != nil {
		return nil, err
	}

	s.publicRpcServer, err = newPublicRpcServer(config.PublicServicePort, advertisedPublicAddress, s.shardsManager)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Close() error {
	if err := s.publicRpcServer.Close(); err != nil {
		return err
	}

	if err := s.internalRpcServer.Close(); err != nil {
		return err
	}

	if err := s.connectionPool.Close(); err != nil {
		return err
	}

	if err := s.shardsManager.Close(); err != nil {
		return err
	}

	return nil
}
