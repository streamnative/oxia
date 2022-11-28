package put

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
)

var (
	keys             []string
	payloads         []string
	expectedVersions []int64
	binaryPayloads   bool

	ErrorExpectedKeyPayloadInconsistent = errors.New("inconsistent flags; key and payload flags must be in pairs")
	ErrorExpectedVersionInconsistent    = errors.New("inconsistent flags; zero or all keys must have an expected version")
	ErrorBase64PayloadInvalid           = errors.New("binary flag was set but payload is not valid base64")
	ErrorIncorrectBinaryFlagUse         = errors.New("binary flag was set when config is being sourced from stdin")
)

func init() {
	Cmd.Flags().StringSliceVarP(&keys, "key", "k", []string{}, "The target key")
	Cmd.Flags().StringSliceVarP(&payloads, "payload", "p", []string{}, "Associated payload, assumed to be encoded with local charset unless -b is used")
	Cmd.Flags().Int64SliceVarP(&expectedVersions, "expected-version", "e", []int64{}, "Version of entry expected to be on the server")
	Cmd.Flags().BoolVarP(&binaryPayloads, "binary", "b", false, "Base64 decode the input payloads (required for binary payloads)")
}

var Cmd = &cobra.Command{
	Use:   "put",
	Short: "Put payloads",
	Long:  `Put a payloads and associated them with the given keys, either inserting a new entries or updating existing ones. If an expected version is provided, the put will only take place if it matches the version of the current record on the server`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	if len(keys) != len(payloads) && (len(payloads) > 0 || len(keys) > 0) {
		return ErrorExpectedKeyPayloadInconsistent
	}
	if (len(expectedVersions) > 0) && len(keys) != len(expectedVersions) {
		return ErrorExpectedVersionInconsistent
	}
	if len(keys) > 0 {
		for i, k := range keys {
			payload, err := convertPayload(payloads[i])
			if err != nil {
				return fmt.Errorf("invalid payload: %v, failed with [%w]", payload, err)
			}
			if len(expectedVersions) > 0 {
				fmt.Printf("Put %v -> %v with expected version %v\n", k, payload, expectedVersions[i])
			} else {
				fmt.Printf("Put %v -> %v\n", k, payload)
			}
		}
	} else {
		if binaryPayloads {
			return ErrorIncorrectBinaryFlagUse
		}
		fmt.Println("Put - read keys/payloads/expectedVersions from STDIN")
	}
	return nil
}

func convertPayload(payload string) ([]byte, error) {
	if binaryPayloads {
		decoded := make([]byte, int64(float64(len(payload))*0.8))
		_, err := base64.StdEncoding.Decode(decoded, []byte(payload))
		if err != nil {
			return nil, ErrorBase64PayloadInvalid
		}
		return decoded, nil
	} else {
		return []byte(payload), nil
	}
}
