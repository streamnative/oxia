package common

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CodeNotInitialized         codes.Code = 100
	CodeInvalidEpoch           codes.Code = 101
	CodeInvalidStatus          codes.Code = 102
	CodeCancelled              codes.Code = 103
	CodeAlreadyClosed          codes.Code = 104
	CodeLeaderAlreadyConnected codes.Code = 105
	CodeNodeIsNotLeader        codes.Code = 106
	CodeNodeIsNotFollower      codes.Code = 107
	CodeInvalidSession         codes.Code = 108
	CodeInvalidSessionTimeout  codes.Code = 109
)

var (
	ErrorNotInitialized         = status.Error(CodeNotInitialized, "oxia: server not initialized yet")
	ErrorCancelled              = status.Error(CodeCancelled, "oxia: operation was cancelled")
	ErrorInvalidEpoch           = status.Error(CodeInvalidEpoch, "oxia: invalid epoch")
	ErrorInvalidStatus          = status.Error(CodeInvalidStatus, "oxia: invalid status")
	ErrorLeaderAlreadyConnected = status.Error(CodeLeaderAlreadyConnected, "oxia: leader is already connected")
	ErrorAlreadyClosed          = status.Error(CodeAlreadyClosed, "oxia: node is shutting down")
	ErrorNodeIsNotLeader        = status.Error(CodeNodeIsNotLeader, "oxia: node is not leader for shard")
	ErrorNodeIsNotFollower      = status.Error(CodeNodeIsNotFollower, "oxia: node is not follower for shard")
	ErrorInvalidSession         = status.Error(CodeInvalidSession, "oxia: session not found")
	ErrorInvalidSessionTimeout  = status.Error(CodeInvalidSessionTimeout, "oxia: invalid session timeout")
)
