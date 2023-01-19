// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CodeNotInitialized         codes.Code = 100
	CodeInvalidTerm            codes.Code = 101
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
	ErrorInvalidTerm            = status.Error(CodeInvalidTerm, "oxia: invalid term")
	ErrorInvalidStatus          = status.Error(CodeInvalidStatus, "oxia: invalid status")
	ErrorLeaderAlreadyConnected = status.Error(CodeLeaderAlreadyConnected, "oxia: leader is already connected")
	ErrorAlreadyClosed          = status.Error(CodeAlreadyClosed, "oxia: node is shutting down")
	ErrorNodeIsNotLeader        = status.Error(CodeNodeIsNotLeader, "oxia: node is not leader for shard")
	ErrorNodeIsNotFollower      = status.Error(CodeNodeIsNotFollower, "oxia: node is not follower for shard")
	ErrorInvalidSession         = status.Error(CodeInvalidSession, "oxia: session not found")
	ErrorInvalidSessionTimeout  = status.Error(CodeInvalidSessionTimeout, "oxia: invalid session timeout")
)
