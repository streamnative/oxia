package server

import (
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CodeInvalidEpoch    codes.Code = 100
	CodeInvalidStatus   codes.Code = 101
	CodeNotServingShard codes.Code = 102
	CodeInvalidSession  codes.Code = 103
)

var (
	ErrorNotInitialized         = errors.New("oxia: server not initialized yet")
	ErrorCancelled              = errors.New("oxia: operation was cancelled")
	ErrorInvalidEpoch           = status.Error(CodeInvalidEpoch, "oxia: invalid epoch")
	ErrorInvalidStatus          = status.Error(CodeInvalidStatus, "oxia: invalid status")
	ErrorNotServingShard        = status.Error(CodeNotServingShard, "oxia: not serving shard")
	ErrorLeaderAlreadyConnected = errors.New("oxia: leader is already connected")
	ErrorAlreadyClosed          = errors.New("oxia: node is shutting down")
	ErrorNodeIsNotLeader        = errors.New("oxia: node is not leader for shard")
	ErrorNodeIsNotFollower      = errors.New("oxia: node is not follower for shard")
	ErrorInvalidSession         = status.Error(CodeInvalidSession, "oxia: session does not exist")
)
