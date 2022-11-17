package batch

import (
	"oxia/oxia"
	"oxia/proto"
)

type PutCall struct {
	Key             string
	Payload         []byte
	ExpectedVersion *int64
	C               chan oxia.PutResult
}

type DeleteCall struct {
	Key             string
	ExpectedVersion *int64
	C               chan error
}

type DeleteRangeCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	C               chan error
}

type GetCall struct {
	Key string
	C   chan oxia.GetResult
}

type GetRangeCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	C               chan oxia.GetRangeResult
}

func (r PutCall) toProto() *proto.PutRequest {
	return &proto.PutRequest{
		Key:             r.Key,
		Payload:         r.Payload,
		ExpectedVersion: r.ExpectedVersion,
	}
}

func (r DeleteCall) toProto() *proto.DeleteRequest {
	return &proto.DeleteRequest{
		Key:             r.Key,
		ExpectedVersion: r.ExpectedVersion,
	}
}

func (r DeleteRangeCall) toProto() *proto.DeleteRangeRequest {
	return &proto.DeleteRangeRequest{
		StartInclusive: r.MinKeyInclusive,
		EndExclusive:   r.MaxKeyExclusive,
	}
}

func (r GetCall) toProto() *proto.GetRequest {
	return &proto.GetRequest{
		Key:            r.Key,
		IncludePayload: true,
	}
}

func (r GetRangeCall) toProto() *proto.GetRangeRequest {
	return &proto.GetRangeRequest{
		StartInclusive: r.MinKeyInclusive,
		EndExclusive:   r.MaxKeyExclusive,
	}
}

func convert[CALL any, PROTO any](calls []CALL, toProto func(CALL) PROTO) []PROTO {
	protos := make([]PROTO, len(calls))
	for i, call := range calls {
		protos[i] = toProto(call)
	}
	return protos
}

func toPutResult(r *proto.PutResponse) oxia.PutResult {
	if err := toError(r.Status); err != nil {
		return oxia.PutResult{
			Err: err,
		}
	}
	return oxia.PutResult{
		Stat: toStat(r.Stat),
	}
}

func toDeleteResult(r *proto.DeleteResponse) error {
	return toError(r.Status)
}

func toDeleteRangeResult(r *proto.DeleteRangeResponse) error {
	return toError(r.Status)
}

func toGetResult(r *proto.GetResponse) oxia.GetResult {
	if err := toError(r.Status); err != nil {
		return oxia.GetResult{
			Err: err,
		}
	}
	return oxia.GetResult{
		Payload: r.Payload,
		Stat:    toStat(r.Stat),
	}
}

func toGetRangeResult(r *proto.GetRangeResponse) oxia.GetRangeResult {
	return oxia.GetRangeResult{
		Keys: r.Keys,
	}
}

func toStat(stat *proto.Stat) oxia.Stat {
	return oxia.Stat{
		Version:           stat.Version,
		CreatedTimestamp:  stat.CreatedTimestamp,
		ModifiedTimestamp: stat.ModifiedTimestamp,
	}
}

func toError(status proto.Status) error {
	switch status {
	case proto.Status_OK:
		return nil
	case proto.Status_BAD_VERSION:
		return oxia.ErrorBadVersion
	case proto.Status_KEY_NOT_FOUND:
		return oxia.ErrorKeyNotFound
	default:
		return oxia.ErrorUnknownStatus
	}
}
