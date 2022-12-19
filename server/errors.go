package server

import (
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CodeInvalidEpoch  codes.Code = 100
	CodeInvalidStatus codes.Code = 101
)

var (
	ErrorNotInitialized         = errors.New("oxia: server not initialized yet")
	ErrorCancelled              = errors.New("oxia: operation was cancelled")
	ErrorInvalidEpoch           = status.Error(CodeInvalidEpoch, "oxia: invalid epoch")
	ErrorInvalidStatus          = status.Error(CodeInvalidStatus, "oxia: invalid status")
	ErrorLeaderAlreadyConnected = errors.New("oxia: leader is already connected")
	ErrorAlreadyClosed          = errors.New("oxia: node is shutting down")
	ErrorNodeIsNotLeader        = errors.New("oxia: node is not leader for shard")
	ErrorNodeIsNotFollower      = errors.New("oxia: node is not follower for shard")
)
