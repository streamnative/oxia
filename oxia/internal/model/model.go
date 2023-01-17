package model

import (
	"oxia/proto"
)

type PutCall struct {
	Key             string
	Payload         []byte
	ExpectedVersion *int64
	SessionId       *int64
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

type ListCall struct {
	MinKeyInclusive string
	MaxKeyExclusive string
	Callback        func(*proto.ListResponse, error)
}

func (r PutCall) ToProto() *proto.PutRequest {
	return &proto.PutRequest{
		Key:             r.Key,
		Payload:         r.Payload,
		ExpectedVersion: r.ExpectedVersion,
		SessionId:       r.SessionId,
	}
}

func (r DeleteCall) ToProto() *proto.DeleteRequest {
	return &proto.DeleteRequest{
		Key:             r.Key,
		ExpectedVersion: r.ExpectedVersion,
	}
}

func (r DeleteRangeCall) ToProto() *proto.DeleteRangeRequest {
	return &proto.DeleteRangeRequest{
		StartInclusive: r.MinKeyInclusive,
		EndExclusive:   r.MaxKeyExclusive,
	}
}

func (r GetCall) ToProto() *proto.GetRequest {
	return &proto.GetRequest{
		Key:            r.Key,
		IncludePayload: true,
	}
}

func (r ListCall) ToProto() *proto.ListRequest {
	return &proto.ListRequest{
		StartInclusive: r.MinKeyInclusive,
		EndExclusive:   r.MaxKeyExclusive,
	}
}

func Convert[CALL any, PROTO any](calls []CALL, toProto func(CALL) PROTO) []PROTO {
	protos := make([]PROTO, len(calls))
	for i, call := range calls {
		protos[i] = toProto(call)
	}
	return protos
}
