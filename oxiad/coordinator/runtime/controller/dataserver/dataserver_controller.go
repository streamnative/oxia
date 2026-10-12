// Copyright 2023-2026 The Oxia Authors
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

package dataserver

import (
	"io"
	"time"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

type Status uint32

const (
	Running Status = iota
	NotRunning
	Draining //
)

func (s Status) ToProto() proto.DataServerState {
	switch s {
	case Running:
		return proto.DataServerState_DATA_SERVER_STATE_RUNNING
	case NotRunning:
		return proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE
	case Draining:
		return proto.DataServerState_DATA_SERVER_STATE_DRAINING
	default:
		return proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED
	}
}

// The Controller takes care of checking the health-status of each dataServer
// and to push all the service discovery updates.
type Controller interface {
	io.Closer

	GetDataServer() commonobject.Borrowed[*proto.DataServer]

	GetStatus() Status

	// IsStablyRunning reports whether the data server is Running and, if it
	// previously failed a health check, whether it has now been Running
	// continuously for at least window. The load balancer uses it to keep a
	// freshly recovered node out of leader rebalancing until it proves
	// stable, so that a flapping node does not get leaders moved back onto
	// it just to lose them again.
	IsStablyRunning(window time.Duration) bool

	GetSupportedFeatures() []proto.Feature

	SetStatus(status Status)
}
