package main

import "oxia/common"

type serverConfig struct {
	InternalServicePort int
	PublicServicePort   int
}

type server struct {
	*internalRpcServer
	*publicRpcServer

	shardsManager ShardsManager
	connectionPool common.ConnectionPool
}

func NewServer(config *serverConfig) (*server, error) {
	s := &server{
		connectionPool: common.NewConnectionPool(),
	}

	s.shardsManager = NewShardsManager(s.connectionPool)

	var err error
	s.internalRpcServer, err = newInternalRpcServer(config.InternalServicePort, s.shardsManager)
	if err != nil {
		return nil, err
	}

	s.publicRpcServer, err = newPublicRpcServer(config.PublicServicePort, s.shardsManager)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) Close() error {
	err := s.connectionPool.Close()
	if err != nil {
		return err
	}

	return nil
}
