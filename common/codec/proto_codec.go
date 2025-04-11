package codec

import "google.golang.org/protobuf/proto"

type ProtoCodec[T proto.Message] interface {
	ToProto() T

	FromProto(T) error
}
