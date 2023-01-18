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

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.9
// source: proto/coordination.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// OxiaControlClient is the client API for OxiaControl service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type OxiaControlClient interface {
	ShardAssignment(ctx context.Context, opts ...grpc.CallOption) (OxiaControl_ShardAssignmentClient, error)
	Fence(ctx context.Context, in *FenceRequest, opts ...grpc.CallOption) (*FenceResponse, error)
	BecomeLeader(ctx context.Context, in *BecomeLeaderRequest, opts ...grpc.CallOption) (*BecomeLeaderResponse, error)
	AddFollower(ctx context.Context, in *AddFollowerRequest, opts ...grpc.CallOption) (*AddFollowerResponse, error)
	GetStatus(ctx context.Context, in *GetStatusRequest, opts ...grpc.CallOption) (*GetStatusResponse, error)
}

type oxiaControlClient struct {
	cc grpc.ClientConnInterface
}

func NewOxiaControlClient(cc grpc.ClientConnInterface) OxiaControlClient {
	return &oxiaControlClient{cc}
}

func (c *oxiaControlClient) ShardAssignment(ctx context.Context, opts ...grpc.CallOption) (OxiaControl_ShardAssignmentClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaControl_ServiceDesc.Streams[0], "/coordination.OxiaControl/ShardAssignment", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaControlShardAssignmentClient{stream}
	return x, nil
}

type OxiaControl_ShardAssignmentClient interface {
	Send(*ShardAssignmentsResponse) error
	CloseAndRecv() (*CoordinationShardAssignmentsResponse, error)
	grpc.ClientStream
}

type oxiaControlShardAssignmentClient struct {
	grpc.ClientStream
}

func (x *oxiaControlShardAssignmentClient) Send(m *ShardAssignmentsResponse) error {
	return x.ClientStream.SendMsg(m)
}

func (x *oxiaControlShardAssignmentClient) CloseAndRecv() (*CoordinationShardAssignmentsResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(CoordinationShardAssignmentsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaControlClient) Fence(ctx context.Context, in *FenceRequest, opts ...grpc.CallOption) (*FenceResponse, error) {
	out := new(FenceResponse)
	err := c.cc.Invoke(ctx, "/coordination.OxiaControl/Fence", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaControlClient) BecomeLeader(ctx context.Context, in *BecomeLeaderRequest, opts ...grpc.CallOption) (*BecomeLeaderResponse, error) {
	out := new(BecomeLeaderResponse)
	err := c.cc.Invoke(ctx, "/coordination.OxiaControl/BecomeLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaControlClient) AddFollower(ctx context.Context, in *AddFollowerRequest, opts ...grpc.CallOption) (*AddFollowerResponse, error) {
	out := new(AddFollowerResponse)
	err := c.cc.Invoke(ctx, "/coordination.OxiaControl/AddFollower", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaControlClient) GetStatus(ctx context.Context, in *GetStatusRequest, opts ...grpc.CallOption) (*GetStatusResponse, error) {
	out := new(GetStatusResponse)
	err := c.cc.Invoke(ctx, "/coordination.OxiaControl/GetStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// OxiaControlServer is the server API for OxiaControl service.
// All implementations must embed UnimplementedOxiaControlServer
// for forward compatibility
type OxiaControlServer interface {
	ShardAssignment(OxiaControl_ShardAssignmentServer) error
	Fence(context.Context, *FenceRequest) (*FenceResponse, error)
	BecomeLeader(context.Context, *BecomeLeaderRequest) (*BecomeLeaderResponse, error)
	AddFollower(context.Context, *AddFollowerRequest) (*AddFollowerResponse, error)
	GetStatus(context.Context, *GetStatusRequest) (*GetStatusResponse, error)
	mustEmbedUnimplementedOxiaControlServer()
}

// UnimplementedOxiaControlServer must be embedded to have forward compatible implementations.
type UnimplementedOxiaControlServer struct {
}

func (UnimplementedOxiaControlServer) ShardAssignment(OxiaControl_ShardAssignmentServer) error {
	return status.Errorf(codes.Unimplemented, "method ShardAssignment not implemented")
}
func (UnimplementedOxiaControlServer) Fence(context.Context, *FenceRequest) (*FenceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Fence not implemented")
}
func (UnimplementedOxiaControlServer) BecomeLeader(context.Context, *BecomeLeaderRequest) (*BecomeLeaderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BecomeLeader not implemented")
}
func (UnimplementedOxiaControlServer) AddFollower(context.Context, *AddFollowerRequest) (*AddFollowerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddFollower not implemented")
}
func (UnimplementedOxiaControlServer) GetStatus(context.Context, *GetStatusRequest) (*GetStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStatus not implemented")
}
func (UnimplementedOxiaControlServer) mustEmbedUnimplementedOxiaControlServer() {}

// UnsafeOxiaControlServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to OxiaControlServer will
// result in compilation errors.
type UnsafeOxiaControlServer interface {
	mustEmbedUnimplementedOxiaControlServer()
}

func RegisterOxiaControlServer(s grpc.ServiceRegistrar, srv OxiaControlServer) {
	s.RegisterService(&OxiaControl_ServiceDesc, srv)
}

func _OxiaControl_ShardAssignment_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(OxiaControlServer).ShardAssignment(&oxiaControlShardAssignmentServer{stream})
}

type OxiaControl_ShardAssignmentServer interface {
	SendAndClose(*CoordinationShardAssignmentsResponse) error
	Recv() (*ShardAssignmentsResponse, error)
	grpc.ServerStream
}

type oxiaControlShardAssignmentServer struct {
	grpc.ServerStream
}

func (x *oxiaControlShardAssignmentServer) SendAndClose(m *CoordinationShardAssignmentsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *oxiaControlShardAssignmentServer) Recv() (*ShardAssignmentsResponse, error) {
	m := new(ShardAssignmentsResponse)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _OxiaControl_Fence_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FenceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaControlServer).Fence(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/coordination.OxiaControl/Fence",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaControlServer).Fence(ctx, req.(*FenceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaControl_BecomeLeader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BecomeLeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaControlServer).BecomeLeader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/coordination.OxiaControl/BecomeLeader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaControlServer).BecomeLeader(ctx, req.(*BecomeLeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaControl_AddFollower_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddFollowerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaControlServer).AddFollower(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/coordination.OxiaControl/AddFollower",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaControlServer).AddFollower(ctx, req.(*AddFollowerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaControl_GetStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaControlServer).GetStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/coordination.OxiaControl/GetStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaControlServer).GetStatus(ctx, req.(*GetStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// OxiaControl_ServiceDesc is the grpc.ServiceDesc for OxiaControl service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var OxiaControl_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "coordination.OxiaControl",
	HandlerType: (*OxiaControlServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Fence",
			Handler:    _OxiaControl_Fence_Handler,
		},
		{
			MethodName: "BecomeLeader",
			Handler:    _OxiaControl_BecomeLeader_Handler,
		},
		{
			MethodName: "AddFollower",
			Handler:    _OxiaControl_AddFollower_Handler,
		},
		{
			MethodName: "GetStatus",
			Handler:    _OxiaControl_GetStatus_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ShardAssignment",
			Handler:       _OxiaControl_ShardAssignment_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "proto/coordination.proto",
}

// OxiaLogReplicationClient is the client API for OxiaLogReplication service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type OxiaLogReplicationClient interface {
	Truncate(ctx context.Context, in *TruncateRequest, opts ...grpc.CallOption) (*TruncateResponse, error)
	AddEntries(ctx context.Context, opts ...grpc.CallOption) (OxiaLogReplication_AddEntriesClient, error)
	SendSnapshot(ctx context.Context, opts ...grpc.CallOption) (OxiaLogReplication_SendSnapshotClient, error)
}

type oxiaLogReplicationClient struct {
	cc grpc.ClientConnInterface
}

func NewOxiaLogReplicationClient(cc grpc.ClientConnInterface) OxiaLogReplicationClient {
	return &oxiaLogReplicationClient{cc}
}

func (c *oxiaLogReplicationClient) Truncate(ctx context.Context, in *TruncateRequest, opts ...grpc.CallOption) (*TruncateResponse, error) {
	out := new(TruncateResponse)
	err := c.cc.Invoke(ctx, "/coordination.OxiaLogReplication/Truncate", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaLogReplicationClient) AddEntries(ctx context.Context, opts ...grpc.CallOption) (OxiaLogReplication_AddEntriesClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaLogReplication_ServiceDesc.Streams[0], "/coordination.OxiaLogReplication/AddEntries", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaLogReplicationAddEntriesClient{stream}
	return x, nil
}

type OxiaLogReplication_AddEntriesClient interface {
	Send(*AddEntryRequest) error
	Recv() (*AddEntryResponse, error)
	grpc.ClientStream
}

type oxiaLogReplicationAddEntriesClient struct {
	grpc.ClientStream
}

func (x *oxiaLogReplicationAddEntriesClient) Send(m *AddEntryRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *oxiaLogReplicationAddEntriesClient) Recv() (*AddEntryResponse, error) {
	m := new(AddEntryResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaLogReplicationClient) SendSnapshot(ctx context.Context, opts ...grpc.CallOption) (OxiaLogReplication_SendSnapshotClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaLogReplication_ServiceDesc.Streams[1], "/coordination.OxiaLogReplication/SendSnapshot", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaLogReplicationSendSnapshotClient{stream}
	return x, nil
}

type OxiaLogReplication_SendSnapshotClient interface {
	Send(*SnapshotChunk) error
	CloseAndRecv() (*SnapshotResponse, error)
	grpc.ClientStream
}

type oxiaLogReplicationSendSnapshotClient struct {
	grpc.ClientStream
}

func (x *oxiaLogReplicationSendSnapshotClient) Send(m *SnapshotChunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *oxiaLogReplicationSendSnapshotClient) CloseAndRecv() (*SnapshotResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(SnapshotResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// OxiaLogReplicationServer is the server API for OxiaLogReplication service.
// All implementations must embed UnimplementedOxiaLogReplicationServer
// for forward compatibility
type OxiaLogReplicationServer interface {
	Truncate(context.Context, *TruncateRequest) (*TruncateResponse, error)
	AddEntries(OxiaLogReplication_AddEntriesServer) error
	SendSnapshot(OxiaLogReplication_SendSnapshotServer) error
	mustEmbedUnimplementedOxiaLogReplicationServer()
}

// UnimplementedOxiaLogReplicationServer must be embedded to have forward compatible implementations.
type UnimplementedOxiaLogReplicationServer struct {
}

func (UnimplementedOxiaLogReplicationServer) Truncate(context.Context, *TruncateRequest) (*TruncateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Truncate not implemented")
}
func (UnimplementedOxiaLogReplicationServer) AddEntries(OxiaLogReplication_AddEntriesServer) error {
	return status.Errorf(codes.Unimplemented, "method AddEntries not implemented")
}
func (UnimplementedOxiaLogReplicationServer) SendSnapshot(OxiaLogReplication_SendSnapshotServer) error {
	return status.Errorf(codes.Unimplemented, "method SendSnapshot not implemented")
}
func (UnimplementedOxiaLogReplicationServer) mustEmbedUnimplementedOxiaLogReplicationServer() {}

// UnsafeOxiaLogReplicationServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to OxiaLogReplicationServer will
// result in compilation errors.
type UnsafeOxiaLogReplicationServer interface {
	mustEmbedUnimplementedOxiaLogReplicationServer()
}

func RegisterOxiaLogReplicationServer(s grpc.ServiceRegistrar, srv OxiaLogReplicationServer) {
	s.RegisterService(&OxiaLogReplication_ServiceDesc, srv)
}

func _OxiaLogReplication_Truncate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TruncateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaLogReplicationServer).Truncate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/coordination.OxiaLogReplication/Truncate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaLogReplicationServer).Truncate(ctx, req.(*TruncateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaLogReplication_AddEntries_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(OxiaLogReplicationServer).AddEntries(&oxiaLogReplicationAddEntriesServer{stream})
}

type OxiaLogReplication_AddEntriesServer interface {
	Send(*AddEntryResponse) error
	Recv() (*AddEntryRequest, error)
	grpc.ServerStream
}

type oxiaLogReplicationAddEntriesServer struct {
	grpc.ServerStream
}

func (x *oxiaLogReplicationAddEntriesServer) Send(m *AddEntryResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *oxiaLogReplicationAddEntriesServer) Recv() (*AddEntryRequest, error) {
	m := new(AddEntryRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _OxiaLogReplication_SendSnapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(OxiaLogReplicationServer).SendSnapshot(&oxiaLogReplicationSendSnapshotServer{stream})
}

type OxiaLogReplication_SendSnapshotServer interface {
	SendAndClose(*SnapshotResponse) error
	Recv() (*SnapshotChunk, error)
	grpc.ServerStream
}

type oxiaLogReplicationSendSnapshotServer struct {
	grpc.ServerStream
}

func (x *oxiaLogReplicationSendSnapshotServer) SendAndClose(m *SnapshotResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *oxiaLogReplicationSendSnapshotServer) Recv() (*SnapshotChunk, error) {
	m := new(SnapshotChunk)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// OxiaLogReplication_ServiceDesc is the grpc.ServiceDesc for OxiaLogReplication service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var OxiaLogReplication_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "coordination.OxiaLogReplication",
	HandlerType: (*OxiaLogReplicationServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Truncate",
			Handler:    _OxiaLogReplication_Truncate_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "AddEntries",
			Handler:       _OxiaLogReplication_AddEntries_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "SendSnapshot",
			Handler:       _OxiaLogReplication_SendSnapshot_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "proto/coordination.proto",
}
