package batch

import (
	"oxia/oxia"
	"oxia/proto"
)

type writeBatchFactory struct {
	execute func(*proto.WriteRequest) (*proto.WriteResponse, error)
}

func (b writeBatchFactory) newBatch(shardId *uint32) batch {
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

func (b *writeBatch) add(call any) {
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

func (b *writeBatch) size() int {
	return len(b.puts) + len(b.deletes) + len(b.deleteRanges)
}

func (b *writeBatch) complete() {
	if response, err := b.execute(b.toProto()); err != nil {
		b.fail(err)
	} else {
		b.handle(response)
	}
}

func (b *writeBatch) fail(err error) {
	for _, put := range b.puts {
		put.C <- oxia.PutResult{
			Err: err,
		}
		close(put.C)
	}
	for _, _delete := range b.deletes {
		_delete.C <- err
		close(_delete.C)
	}
	for _, deleteRange := range b.deleteRanges {
		deleteRange.C <- err
		close(deleteRange.C)
	}
}

func (b *writeBatch) handle(response *proto.WriteResponse) {
	for i, put := range b.puts {
		put.C <- toPutResult(response.Puts[i])
		close(put.C)
	}
	for i, _delete := range b.deletes {
		_delete.C <- toDeleteResult(response.Deletes[i])
		close(_delete.C)
	}
	for i, deleteRange := range b.deleteRanges {
		deleteRange.C <- toDeleteRangeResult(response.DeleteRanges[i])
		close(deleteRange.C)
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
