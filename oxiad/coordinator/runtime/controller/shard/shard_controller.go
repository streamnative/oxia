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

package shard

import (
	"io"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

// The Controller is responsible to handle all the state transition for a given a shard
// e.g. electing a new leader.
type Controller interface {
	io.Closer

	controllerapi.DataServerEventListener

	SyncServerAddress()

	DeleteShard()

	Election(electionAction *action.ElectionAction) string

	ChangeEnsemble(changeEnsembleAction *action.ChangeEnsembleAction)
}

type DataServerSupportedFeaturesSupplier = func(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature

func NoOpSupportedFeaturesSupplier(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature, len(dataServers))
	for _, dataServer := range dataServers {
		features[dataServer.GetNameOrDefault()] = []proto.Feature{}
	}
	return features
}
