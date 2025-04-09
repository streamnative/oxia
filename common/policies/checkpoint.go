package policies

import (
	"github.com/streamnative/oxia/common"
)

const DefaultCommitEvery int64 = 1000
const CheckpointKey = common.InternalKeyPrefix + "checkpoint"

type Checkpoint struct {
	Enabled     *bool  `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	CommitEvery *int64 `json:"commitEvery,omitempty" yaml:"commitEvery,omitempty"`
}

func (c *Checkpoint) IsEnabled() bool {
	return c != nil && c.Enabled != nil && *c.Enabled
}

func (c *Checkpoint) GetCommitEvery() int64 {
	if c.CommitEvery == nil {
		return DefaultCommitEvery
	}
	return *c.CommitEvery
}
