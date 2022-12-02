package coordinator

import (
	"bytes"
	"encoding/json"
)

type ShardStatus uint16

const (
	ShardStatusUnknown = iota
	ShardStatusSteadyState
	ShardStatusElection
)

func (s ShardStatus) String() string {
	return toString[s]
}

var toString = map[ShardStatus]string{
	ShardStatusUnknown:     "Unknown",
	ShardStatusSteadyState: "SteadyState",
	ShardStatusElection:    "Election",
}

var toShardStatus = map[string]ShardStatus{
	"Unknown":     ShardStatusUnknown,
	"SteadyState": ShardStatusSteadyState,
	"Election":    ShardStatusElection,
}

// MarshalJSON marshals the enum as a quoted json string
func (s ShardStatus) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmarshals a quoted json string to the enum value
func (s *ShardStatus) UnmarshalJSON(b []byte) error {
	var j string
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	// If the string cannot be found then it will be set to the Unknown status value.
	*s = toShardStatus[j]
	return nil
}
