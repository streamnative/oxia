// Copyright 2025 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
