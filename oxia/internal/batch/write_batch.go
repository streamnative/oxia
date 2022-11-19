package batch

import (
	"oxia/proto"
)

type writeBatchFactory struct {
	execute func(*proto.WriteRequest) (*proto.WriteResponse, error)
}

func (b writeBatchFactory) newBatch(shardId *uint32) Batch {
	return &writeBatch{
		shardId:      shardId,
		execute:      b.execute,
		puts:         make([]PutCall, 0),
		deletes:      make([]DeleteCall, 0),
		deleteRanges: make([]DeleteRangeCall, 0),
	}
}

//////////

type writeBatch struct {
	shardId      *uint32
	execute      func(*proto.WriteRequest) (*proto.WriteResponse, error)
	puts         []PutCall
	deletes      []DeleteCall
	deleteRanges []DeleteRangeCall
}

func (b *writeBatch) Add(call any) {
	switch c := call.(type) {
	case PutCall:
		b.puts = append(b.puts, c)
	case DeleteCall:
		b.deletes = append(b.deletes, c)
	case DeleteRangeCall:
		b.deleteRanges = append(b.deleteRanges, c)
	default:
		panic("invalid call")
	}
}

func (b *writeBatch) Size() int {
	return len(b.puts) + len(b.deletes) + len(b.deleteRanges)
}

func (b *writeBatch) Complete() {
	if response, err := b.execute(b.toProto()); err != nil {
		b.Fail(err)
	} else {
		b.handle(response)
	}
}

func (b *writeBatch) Fail(err error) {
	for _, put := range b.puts {
		put.Callback(nil, err)
	}
	for _, _delete := range b.deletes {
		_delete.Callback(nil, err)
	}
	for _, deleteRange := range b.deleteRanges {
		deleteRange.Callback(nil, err)
	}
}

func (b *writeBatch) handle(response *proto.WriteResponse) {
	for i, put := range b.puts {
		put.Callback(response.Puts[i], nil)
	}
	for i, _delete := range b.deletes {
		_delete.Callback(response.Deletes[i], nil)
	}
	for i, deleteRange := range b.deleteRanges {
		deleteRange.Callback(response.DeleteRanges[i], nil)
	}
}

func (b *writeBatch) toProto() *proto.WriteRequest {
	return &proto.WriteRequest{
		ShardId:      b.shardId,
		Puts:         convert[PutCall, *proto.PutRequest](b.puts, PutCall.toProto),
		Deletes:      convert[DeleteCall, *proto.DeleteRequest](b.deletes, DeleteCall.toProto),
		DeleteRanges: convert[DeleteRangeCall, *proto.DeleteRangeRequest](b.deleteRanges, DeleteRangeCall.toProto),
	}
}
