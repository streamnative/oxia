package codec

import "google.golang.org/protobuf/proto"

// ProtoCodec is a generic interface defining the ability to convert to and from Protocol Buffers messages.
// It uses a generic type T, which must be an instance of proto.Message, meaning T represents any Protocol Buffers message type.
type ProtoCodec[T proto.Message] interface {

	// ToProto converts the implementing type to a Protocol Buffers message.
	ToProto() T

	// FromProto reconstructs the state of the implementing type from a Protocol Buffers message.
	FromProto(T) error
}
