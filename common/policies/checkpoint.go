package policies

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
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

func (c *Checkpoint) PiggybackWrite(requests *proto.WriteRequest, commitOffset int64) error {
	if commitOffset%c.GetCommitEvery() == 0 {
		checkPoint := proto.Checkpoint{
			CommitOffset: commitOffset,
		}
		value, err := checkPoint.MarshalVT()
		if err != nil {
			return err
		}
		requests.Puts = append(requests.Puts, &proto.PutRequest{
			Key:   CheckpointKey,
			Value: value,
		})
	}
	return nil
}

var ErrUnmatchedCheckpoint = errors.New("checkpoint not exactly same.")

func VerifyCheckpoint(expect *proto.Checkpoint, actual *proto.Checkpoint) error {
	if expect.VersionId != actual.VersionId {
		return errors.Wrap(ErrUnmatchedCheckpoint,
			fmt.Sprintf("expected version id %v, actual version id %v", expect.VersionId, actual.VersionId))
	}
	return nil
}
