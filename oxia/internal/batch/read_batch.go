package batch

import (
	"oxia/proto"
)

type readBatchFactory struct {
	execute func(*proto.ReadRequest) (*proto.ReadResponse, error)
}

func (b readBatchFactory) newBatch(shardId *uint32) Batch {
	return &readBatch{
		shardId:   shardId,
		execute:   b.execute,
		gets:      make([]GetCall, 0),
		getRanges: make([]GetRangeCall, 0),
	}
}

//////////

type readBatch struct {
	shardId   *uint32
	execute   func(*proto.ReadRequest) (*proto.ReadResponse, error)
	gets      []GetCall
	getRanges []GetRangeCall
}

func (b *readBatch) Add(call any) {
	switch c := call.(type) {
	case GetCall:
		b.gets = append(b.gets, c)
	case GetRangeCall:
		b.getRanges = append(b.getRanges, c)
	default:
		panic("invalid call")
	}
}

func (b *readBatch) Size() int {
	return len(b.gets) + len(b.getRanges)
}

func (b *readBatch) Complete() {
	if response, err := b.execute(b.toProto()); err != nil {
		b.Fail(err)
	} else {
		b.handle(response)
	}
}

func (b *readBatch) Fail(err error) {
	for _, get := range b.gets {
		get.Callback(nil, err)
	}
	for _, getRange := range b.getRanges {
		getRange.Callback(nil, err)
	}
}

func (b *readBatch) handle(response *proto.ReadResponse) {
	for i, get := range b.gets {
		get.Callback(response.Gets[i], nil)
	}
	for i, getRange := range b.getRanges {
		getRange.Callback(response.GetRanges[i], nil)
	}
}

func (b *readBatch) toProto() *proto.ReadRequest {
	return &proto.ReadRequest{
		ShardId:   b.shardId,
		Gets:      convert[GetCall, *proto.GetRequest](b.gets, GetCall.toProto),
		GetRanges: convert[GetRangeCall, *proto.GetRangeRequest](b.getRanges, GetRangeCall.toProto),
	}
}
