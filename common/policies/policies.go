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
