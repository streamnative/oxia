package batch

import (
	"oxia/oxia/internal/metrics"
	"oxia/oxia/internal/model"
	"oxia/proto"
	"time"
)

type readBatchFactory struct {
	execute func(*proto.ReadRequest) (*proto.ReadResponse, error)
	metrics *metrics.Metrics
}

func (b readBatchFactory) newBatch(shardId *uint32) Batch {
	return &readBatch{
		shardId:  shardId,
		execute:  b.execute,
		gets:     make([]model.GetCall, 0),
		lists:    make([]model.ListCall, 0),
		start:    time.Now(),
		metrics:  b.metrics,
		callback: b.metrics.ReadCallback(),
	}
}

//////////

type readBatch struct {
	shardId  *uint32
	execute  func(*proto.ReadRequest) (*proto.ReadResponse, error)
	gets     []model.GetCall
	lists    []model.ListCall
	start    time.Time
	metrics  *metrics.Metrics
	callback func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error)
}

func (b *readBatch) Add(call any) {
	switch c := call.(type) {
	case model.GetCall:
		b.gets = append(b.gets, b.metrics.DecorateGet(c))
	case model.ListCall:
		b.lists = append(b.lists, b.metrics.DecorateList(c))
	default:
		panic("invalid call")
	}
}

func (b *readBatch) Size() int {
	return len(b.gets) + len(b.lists)
}

func (b *readBatch) Complete() {
	executionStart := time.Now()
	request := b.toProto()
	response, err := b.execute(request)
	b.callback(executionStart, request, response, err)
	if err != nil {
		b.Fail(err)
	} else {
		b.handle(response)
	}
}

func (b *readBatch) Fail(err error) {
	for _, get := range b.gets {
		get.Callback(nil, err)
	}
	for _, list := range b.lists {
		list.Callback(nil, err)
	}
}

func (b *readBatch) handle(response *proto.ReadResponse) {
	for i, get := range b.gets {
		get.Callback(response.Gets[i], nil)
	}
	for i, list := range b.lists {
		list.Callback(response.Lists[i], nil)
	}
}

func (b *readBatch) toProto() *proto.ReadRequest {
	return &proto.ReadRequest{
		ShardId: b.shardId,
		Gets:    model.Convert[model.GetCall, *proto.GetRequest](b.gets, model.GetCall.ToProto),
		Lists:   model.Convert[model.ListCall, *proto.ListRequest](b.lists, model.ListCall.ToProto),
	}
}
