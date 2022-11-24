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
		shardId:   shardId,
		execute:   b.execute,
		gets:      make([]model.GetCall, 0),
		getRanges: make([]model.GetRangeCall, 0),
		start:     time.Now(),
		metrics:   b.metrics,
		callback:  b.metrics.ReadCallback(),
	}
}

//////////

type readBatch struct {
	shardId   *uint32
	execute   func(*proto.ReadRequest) (*proto.ReadResponse, error)
	gets      []model.GetCall
	getRanges []model.GetRangeCall
	start     time.Time
	metrics   *metrics.Metrics
	callback  func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error)
}

func (b *readBatch) Add(call any) {
	switch c := call.(type) {
	case model.GetCall:
		b.gets = append(b.gets, b.metrics.DecorateGet(c))
	case model.GetRangeCall:
		b.getRanges = append(b.getRanges, b.metrics.DecorateGetRange(c))
	default:
		panic("invalid call")
	}
}

func (b *readBatch) Size() int {
	return len(b.gets) + len(b.getRanges)
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
		Gets:      model.Convert[model.GetCall, *proto.GetRequest](b.gets, model.GetCall.ToProto),
		GetRanges: model.Convert[model.GetRangeCall, *proto.GetRangeRequest](b.getRanges, model.GetRangeCall.ToProto),
	}
}
