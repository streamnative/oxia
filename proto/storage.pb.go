// Copyright 2023 StreamNative, Inc.
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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v5.27.3
// source: storage.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type StorageEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value                 []byte            `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	VersionId             int64             `protobuf:"varint,2,opt,name=version_id,json=versionId,proto3" json:"version_id,omitempty"`
	ModificationsCount    int64             `protobuf:"varint,3,opt,name=modifications_count,json=modificationsCount,proto3" json:"modifications_count,omitempty"`
	CreationTimestamp     uint64            `protobuf:"fixed64,4,opt,name=creation_timestamp,json=creationTimestamp,proto3" json:"creation_timestamp,omitempty"`
	ModificationTimestamp uint64            `protobuf:"fixed64,5,opt,name=modification_timestamp,json=modificationTimestamp,proto3" json:"modification_timestamp,omitempty"`
	SessionId             *int64            `protobuf:"varint,6,opt,name=session_id,json=sessionId,proto3,oneof" json:"session_id,omitempty"`
	ClientIdentity        *string           `protobuf:"bytes,7,opt,name=client_identity,json=clientIdentity,proto3,oneof" json:"client_identity,omitempty"`
	PartitionKey          *string           `protobuf:"bytes,8,opt,name=partition_key,json=partitionKey,proto3,oneof" json:"partition_key,omitempty"`
	SecondaryIndexes      []*SecondaryIndex `protobuf:"bytes,9,rep,name=secondary_indexes,json=secondaryIndexes,proto3" json:"secondary_indexes,omitempty"`
}

func (x *StorageEntry) Reset() {
	*x = StorageEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StorageEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StorageEntry) ProtoMessage() {}

func (x *StorageEntry) ProtoReflect() protoreflect.Message {
	mi := &file_storage_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StorageEntry.ProtoReflect.Descriptor instead.
func (*StorageEntry) Descriptor() ([]byte, []int) {
	return file_storage_proto_rawDescGZIP(), []int{0}
}

func (x *StorageEntry) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *StorageEntry) GetVersionId() int64 {
	if x != nil {
		return x.VersionId
	}
	return 0
}

func (x *StorageEntry) GetModificationsCount() int64 {
	if x != nil {
		return x.ModificationsCount
	}
	return 0
}

func (x *StorageEntry) GetCreationTimestamp() uint64 {
	if x != nil {
		return x.CreationTimestamp
	}
	return 0
}

func (x *StorageEntry) GetModificationTimestamp() uint64 {
	if x != nil {
		return x.ModificationTimestamp
	}
	return 0
}

func (x *StorageEntry) GetSessionId() int64 {
	if x != nil && x.SessionId != nil {
		return *x.SessionId
	}
	return 0
}

func (x *StorageEntry) GetClientIdentity() string {
	if x != nil && x.ClientIdentity != nil {
		return *x.ClientIdentity
	}
	return ""
}

func (x *StorageEntry) GetPartitionKey() string {
	if x != nil && x.PartitionKey != nil {
		return *x.PartitionKey
	}
	return ""
}

func (x *StorageEntry) GetSecondaryIndexes() []*SecondaryIndex {
	if x != nil {
		return x.SecondaryIndexes
	}
	return nil
}

type SessionMetadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TimeoutMs uint32 `protobuf:"varint,1,opt,name=timeout_ms,json=timeoutMs,proto3" json:"timeout_ms,omitempty"`
	Identity  string `protobuf:"bytes,2,opt,name=identity,proto3" json:"identity,omitempty"`
}

func (x *SessionMetadata) Reset() {
	*x = SessionMetadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SessionMetadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SessionMetadata) ProtoMessage() {}

func (x *SessionMetadata) ProtoReflect() protoreflect.Message {
	mi := &file_storage_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SessionMetadata.ProtoReflect.Descriptor instead.
func (*SessionMetadata) Descriptor() ([]byte, []int) {
	return file_storage_proto_rawDescGZIP(), []int{1}
}

func (x *SessionMetadata) GetTimeoutMs() uint32 {
	if x != nil {
		return x.TimeoutMs
	}
	return 0
}

func (x *SessionMetadata) GetIdentity() string {
	if x != nil {
		return x.Identity
	}
	return ""
}

type LogEntryValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Value:
	//
	//	*LogEntryValue_Requests
	Value isLogEntryValue_Value `protobuf_oneof:"value"`
}

func (x *LogEntryValue) Reset() {
	*x = LogEntryValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogEntryValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntryValue) ProtoMessage() {}

func (x *LogEntryValue) ProtoReflect() protoreflect.Message {
	mi := &file_storage_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogEntryValue.ProtoReflect.Descriptor instead.
func (*LogEntryValue) Descriptor() ([]byte, []int) {
	return file_storage_proto_rawDescGZIP(), []int{2}
}

func (m *LogEntryValue) GetValue() isLogEntryValue_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (x *LogEntryValue) GetRequests() *WriteRequests {
	if x, ok := x.GetValue().(*LogEntryValue_Requests); ok {
		return x.Requests
	}
	return nil
}

type isLogEntryValue_Value interface {
	isLogEntryValue_Value()
}

type LogEntryValue_Requests struct {
	Requests *WriteRequests `protobuf:"bytes,1,opt,name=requests,proto3,oneof"`
}

func (*LogEntryValue_Requests) isLogEntryValue_Value() {}

type WriteRequests struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Writes []*WriteRequest `protobuf:"bytes,1,rep,name=writes,proto3" json:"writes,omitempty"`
}

func (x *WriteRequests) Reset() {
	*x = WriteRequests{}
	if protoimpl.UnsafeEnabled {
		mi := &file_storage_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteRequests) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteRequests) ProtoMessage() {}

func (x *WriteRequests) ProtoReflect() protoreflect.Message {
	mi := &file_storage_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteRequests.ProtoReflect.Descriptor instead.
func (*WriteRequests) Descriptor() ([]byte, []int) {
	return file_storage_proto_rawDescGZIP(), []int{3}
}

func (x *WriteRequests) GetWrites() []*WriteRequest {
	if x != nil {
		return x.Writes
	}
	return nil
}

var file_storage_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.MessageOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         64101,
		Name:          "proto.mempool",
		Tag:           "varint,64101,opt,name=mempool",
		Filename:      "storage.proto",
	},
}

// Extension fields to descriptorpb.MessageOptions.
var (
	// optional bool mempool = 64101;
	E_Mempool = &file_storage_proto_extTypes[0]
)

var File_storage_proto protoreflect.FileDescriptor

var file_storage_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0c, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xea, 0x03, 0x0a, 0x0c, 0x53, 0x74, 0x6f, 0x72, 0x61,
	0x67, 0x65, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1d, 0x0a,
	0x0a, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x09, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x2f, 0x0a, 0x13,
	0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x5f, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x12, 0x6d, 0x6f, 0x64, 0x69, 0x66,
	0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x2d, 0x0a,
	0x12, 0x63, 0x72, 0x65, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x06, 0x52, 0x11, 0x63, 0x72, 0x65, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x35, 0x0a, 0x16,
	0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x74, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x05, 0x20, 0x01, 0x28, 0x06, 0x52, 0x15, 0x6d, 0x6f,
	0x64, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x12, 0x22, 0x0a, 0x0a, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69,
	0x64, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x48, 0x00, 0x52, 0x09, 0x73, 0x65, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x49, 0x64, 0x88, 0x01, 0x01, 0x12, 0x2c, 0x0a, 0x0f, 0x63, 0x6c, 0x69, 0x65, 0x6e,
	0x74, 0x5f, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09,
	0x48, 0x01, 0x52, 0x0e, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69,
	0x74, 0x79, 0x88, 0x01, 0x01, 0x12, 0x28, 0x0a, 0x0d, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69,
	0x6f, 0x6e, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x48, 0x02, 0x52, 0x0c,
	0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x4b, 0x65, 0x79, 0x88, 0x01, 0x01, 0x12,
	0x57, 0x0a, 0x11, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x61, 0x72, 0x79, 0x5f, 0x69, 0x6e, 0x64,
	0x65, 0x78, 0x65, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x69, 0x6f, 0x2e,
	0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x2e, 0x6f, 0x78, 0x69,
	0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x61, 0x72,
	0x79, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x52, 0x10, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x61, 0x72,
	0x79, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x65, 0x73, 0x3a, 0x04, 0xa8, 0xa6, 0x1f, 0x01, 0x42, 0x0d,
	0x0a, 0x0b, 0x5f, 0x73, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x42, 0x12, 0x0a,
	0x10, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74,
	0x79, 0x42, 0x10, 0x0a, 0x0e, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f,
	0x6b, 0x65, 0x79, 0x22, 0x52, 0x0a, 0x0f, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x4d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75,
	0x74, 0x5f, 0x6d, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65,
	0x6f, 0x75, 0x74, 0x4d, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74,
	0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x74,
	0x79, 0x3a, 0x04, 0xa8, 0xa6, 0x1f, 0x01, 0x22, 0x52, 0x0a, 0x0d, 0x4c, 0x6f, 0x67, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x32, 0x0a, 0x08, 0x72, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73,
	0x48, 0x00, 0x52, 0x08, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x3a, 0x04, 0xa8, 0xa6,
	0x1f, 0x01, 0x42, 0x07, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x51, 0x0a, 0x0d, 0x57,
	0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x12, 0x40, 0x0a, 0x06,
	0x77, 0x72, 0x69, 0x74, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x69,
	0x6f, 0x2e, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x2e, 0x6f,
	0x78, 0x69, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x52, 0x06, 0x77, 0x72, 0x69, 0x74, 0x65, 0x73, 0x3a, 0x3e,
	0x0a, 0x07, 0x6d, 0x65, 0x6d, 0x70, 0x6f, 0x6f, 0x6c, 0x12, 0x1f, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xe5, 0xf4, 0x03, 0x20, 0x01,
	0x28, 0x08, 0x52, 0x07, 0x6d, 0x65, 0x6d, 0x70, 0x6f, 0x6f, 0x6c, 0x88, 0x01, 0x01, 0x42, 0x24,
	0x5a, 0x22, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x2f, 0x6f, 0x78, 0x69, 0x61, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_storage_proto_rawDescOnce sync.Once
	file_storage_proto_rawDescData = file_storage_proto_rawDesc
)

func file_storage_proto_rawDescGZIP() []byte {
	file_storage_proto_rawDescOnce.Do(func() {
		file_storage_proto_rawDescData = protoimpl.X.CompressGZIP(file_storage_proto_rawDescData)
	})
	return file_storage_proto_rawDescData
}

var file_storage_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_storage_proto_goTypes = []interface{}{
	(*StorageEntry)(nil),                // 0: proto.StorageEntry
	(*SessionMetadata)(nil),             // 1: proto.SessionMetadata
	(*LogEntryValue)(nil),               // 2: proto.LogEntryValue
	(*WriteRequests)(nil),               // 3: proto.WriteRequests
	(*SecondaryIndex)(nil),              // 4: io.streamnative.oxia.proto.SecondaryIndex
	(*WriteRequest)(nil),                // 5: io.streamnative.oxia.proto.WriteRequest
	(*descriptorpb.MessageOptions)(nil), // 6: google.protobuf.MessageOptions
}
var file_storage_proto_depIdxs = []int32{
	4, // 0: proto.StorageEntry.secondary_indexes:type_name -> io.streamnative.oxia.proto.SecondaryIndex
	3, // 1: proto.LogEntryValue.requests:type_name -> proto.WriteRequests
	5, // 2: proto.WriteRequests.writes:type_name -> io.streamnative.oxia.proto.WriteRequest
	6, // 3: proto.mempool:extendee -> google.protobuf.MessageOptions
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	3, // [3:4] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_storage_proto_init() }
func file_storage_proto_init() {
	if File_storage_proto != nil {
		return
	}
	file_client_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_storage_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StorageEntry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_storage_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SessionMetadata); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_storage_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogEntryValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_storage_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteRequests); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_storage_proto_msgTypes[0].OneofWrappers = []interface{}{}
	file_storage_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*LogEntryValue_Requests)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_storage_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 1,
			NumServices:   0,
		},
		GoTypes:           file_storage_proto_goTypes,
		DependencyIndexes: file_storage_proto_depIdxs,
		MessageInfos:      file_storage_proto_msgTypes,
		ExtensionInfos:    file_storage_proto_extTypes,
	}.Build()
	File_storage_proto = out.File
	file_storage_proto_rawDesc = nil
	file_storage_proto_goTypes = nil
	file_storage_proto_depIdxs = nil
}
