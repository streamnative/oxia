package batch

import (
	"oxia/proto"
)

type PutCall struct {
	Key             string
	Payload         []byte
	ExpectedVersion *int64
	Callback        func(*proto.PutResponse, error)
}

type DeleteCall struct {
	Key             string
	ExpectedVersion *int64
	Callback        func(*proto.DeleteResponse, error)
}

type DeleteRangeCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	Callback        func(*proto.DeleteRangeResponse, error)
}

type GetCall struct {
	Key      string
	Callback func(*proto.GetResponse, error)
}

type GetRangeCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	Callback        func(*proto.GetRangeResponse, error)
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
