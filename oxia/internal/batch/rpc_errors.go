package batch

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"oxia/common"
)

func isRetriable(err error) bool {
	code := status.Code(err)
	switch code {
	case codes.Unavailable:
		// Failure to connect is ok to re-attempt
		return true
	case common.CodeAlreadyClosed:
		// Leader is closing, though we expect a new leader to be elected
		return true
	case common.CodeNodeIsNotLeader:
		// We're making a request to a node that is not leader anymore.
		// Retry to make the request to the new leader
		return true
	}

	return false
}
