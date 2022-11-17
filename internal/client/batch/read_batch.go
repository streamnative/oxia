package batch

import (
	"oxia/oxia"
	"oxia/proto"
)

type readBatchFactory struct {
	execute func(*proto.ReadRequest) (*proto.ReadResponse, error)
}

func (b readBatchFactory) newBatch(shardId *uint32) batch {
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

func (b *readBatch) add(call any) {
	switch c := call.(type) {
	case GetCall:
		b.gets = append(b.gets, c)
	case GetRangeCall:
		b.getRanges = append(b.getRanges, c)
	default:
		panic("invalid call")
	}
}

func (b *readBatch) size() int {
	return len(b.gets) + len(b.getRanges)
}

func (b *readBatch) complete() {
	if response, err := b.execute(b.toProto()); err != nil {
		b.fail(err)
	} else {
		b.handle(response)
	}
}

func (b *readBatch) fail(err error) {
	for _, get := range b.gets {
		get.C <- oxia.GetResult{
			Err: err,
		}
		close(get.C)
	}
	for _, getRange := range b.getRanges {
		getRange.C <- oxia.GetRangeResult{
			Err: err,
		}
		close(getRange.C)
	}
}

func (b *readBatch) handle(response *proto.ReadResponse) {
	for i, get := range b.gets {
		get.C <- toGetResult(response.Gets[i])
		close(get.C)
	}
	for i, getRange := range b.getRanges {
		getRange.C <- toGetRangeResult(response.GetRanges[i])
		close(getRange.C)
	}
}

func (b *readBatch) toProto() *proto.ReadRequest {
	return &proto.ReadRequest{
		ShardId:   b.shardId,
		Gets:      convert[GetCall, *proto.GetRequest](b.gets, GetCall.toProto),
		GetRanges: convert[GetRangeCall, *proto.GetRangeRequest](b.getRanges, GetRangeCall.toProto),
	}
}
