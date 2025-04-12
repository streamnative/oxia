// Copyright 2025 StreamNative, Inc.
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

package policies

import (
	"github.com/streamnative/oxia/common/codec"
	"github.com/streamnative/oxia/proto"
)

var _ codec.ProtoCodec[*proto.Policies] = &Policies{}

type Policies struct {
	Checkpoint *Checkpoint `json:"checkpoint" yaml:"checkpoint"`
}

func (p *Policies) FromProto(t *proto.Policies) error {
	checkpoint := t.GetCheckpointPolicies()
	if checkpoint != nil {
		cp := &Checkpoint{}
		if err := cp.FromProto(checkpoint); err != nil {
			return err
		}
		p.Checkpoint = cp
	}
	return nil
}

func (p *Policies) ToProto() *proto.Policies {
	return &proto.Policies{
		CheckpointPolicies: p.Checkpoint.ToProto(),
	}
}
