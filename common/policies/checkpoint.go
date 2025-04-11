package policies

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/codec"
	"github.com/streamnative/oxia/proto"
)

const (
	DefaultCommitEvery                int32 = 1000
	CheckpointEntryKey                      = common.InternalKeyPrefix + "checkpoint"
	CheckpointMetadataCommitOffsetKey       = "checkpoint-commit-offset"
	CheckpointMetadataVersionIdKey          = "checkpoint-version-id"

	FailureHandlingWarn    int32 = 0
	FailureHandlingDiscard int32 = 1
)

var _ codec.ProtoCodec[*proto.CheckpointPolicies] = &Checkpoint{}

// Checkpoint represents a checkpoint policy with failure handling and commit settings.
type Checkpoint struct {
	Enabled         *bool  `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	CommitEvery     *int32 `json:"commitEvery,omitempty" yaml:"commitEvery,omitempty"`
	FailureHandling *int32 `json:"failureHandling,omitempty" yaml:"failureHandling,omitempty"`
}

func (c *Checkpoint) ToProto() *proto.CheckpointPolicies {
	return &proto.CheckpointPolicies{
		Enabled:         c.IsEnabled(),
		CommitEvery:     c.GetCommitEvery(),
		FailureHandling: c.GetFailureHandling(),
	}
}

func (c *Checkpoint) FromProto(t *proto.CheckpointPolicies) error {
	c.Enabled = &t.Enabled
	c.CommitEvery = &t.CommitEvery
	c.FailureHandling = &t.FailureHandling
	return nil
}

func (c *Checkpoint) IsEnabled() bool {
	return c != nil && c.Enabled != nil && *c.Enabled
}

func (c *Checkpoint) GetCommitEvery() int32 {
	if c.CommitEvery == nil {
		return DefaultCommitEvery
	}
	return *c.CommitEvery
}

func (c *Checkpoint) GetFailureHandling() int32 {
	if c.FailureHandling == nil {
		return FailureHandlingWarn
	}
	return *c.FailureHandling
}

// PiggybackWrite adds checkpoint metadata to a WriteRequest for atomic commit.
// Creates a proto.Checkpoint with the given commit offset, marshals it, and appends to requests.Puts.
func (c *Checkpoint) PiggybackWrite(requests *proto.WriteRequest, commitOffset int64) error {
	checkPoint := proto.Checkpoint{
		CommitOffset: commitOffset,
	}
	value, err := checkPoint.MarshalVT()
	if err != nil {
		return err
	}
	requests.Puts = append(requests.Puts, &proto.PutRequest{
		Key:   CheckpointEntryKey,
		Value: value,
	})
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
