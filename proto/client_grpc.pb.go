// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v5.29.3
// source: client.proto

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

// OxiaClientClient is the client API for OxiaClient service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type OxiaClientClient interface {
	// *
	// Gets all shard-to-server assignments as a stream. Each set of assignments
	// in the response stream will contain all the assignments to bring the client
	// up to date. For example, if a shard is split, the stream will return a
	// single response containing all the new shard assignments as opposed to
	// multiple stream responses, each containing a single shard assignment.
	//
	// Clients should connect to a single random server which will stream the
	// assignments for all shards on all servers.
	GetShardAssignments(ctx context.Context, in *ShardAssignmentsRequest, opts ...grpc.CallOption) (OxiaClient_GetShardAssignmentsClient, error)
	// *
	// Batches put, delete and delete_range requests.
	//
	// Clients should send this request to the shard leader. In the future,
	// this may be handled server-side in a proxy layer.
	//
	// Deprecated
	Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error)
	// *
	// Batches put, delete and delete_range requests.
	//
	// Clients should send this request to the shard leader. In the future,
	// this may be handled server-side in a proxy layer.
	WriteStream(ctx context.Context, opts ...grpc.CallOption) (OxiaClient_WriteStreamClient, error)
	// *
	// Batches get requests.
	//
	// Clients should send this request to the shard leader. In the future,
	// this may be handled server-side in a proxy layer.
	Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (OxiaClient_ReadClient, error)
	// *
	// Requests all the keys between a range of keys.
	//
	// Clients should send an equivalent request to all respective shards,
	// unless a particular partition key was specified.
	List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (OxiaClient_ListClient, error)
	// *
	// Requests all the records between a range of keys.
	//
	// Clients should send an equivalent request to all respective shards,
	// unless a particular partition key was specified.
	RangeScan(ctx context.Context, in *RangeScanRequest, opts ...grpc.CallOption) (OxiaClient_RangeScanClient, error)
	GetNotifications(ctx context.Context, in *NotificationsRequest, opts ...grpc.CallOption) (OxiaClient_GetNotificationsClient, error)
	// Creates a new client session. Sessions are kept alive by regularly sending
	// heartbeats via the KeepAlive rpc.
	CreateSession(ctx context.Context, in *CreateSessionRequest, opts ...grpc.CallOption) (*CreateSessionResponse, error)
	// Sends a heartbeat to prevent the session from timing out.
	KeepAlive(ctx context.Context, in *SessionHeartbeat, opts ...grpc.CallOption) (*KeepAliveResponse, error)
	// Closes a session and removes all ephemeral values associated with it.
	CloseSession(ctx context.Context, in *CloseSessionRequest, opts ...grpc.CallOption) (*CloseSessionResponse, error)
}

type oxiaClientClient struct {
	cc grpc.ClientConnInterface
}

func NewOxiaClientClient(cc grpc.ClientConnInterface) OxiaClientClient {
	return &oxiaClientClient{cc}
}

func (c *oxiaClientClient) GetShardAssignments(ctx context.Context, in *ShardAssignmentsRequest, opts ...grpc.CallOption) (OxiaClient_GetShardAssignmentsClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaClient_ServiceDesc.Streams[0], "/io.streamnative.oxia.proto.OxiaClient/GetShardAssignments", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaClientGetShardAssignmentsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type OxiaClient_GetShardAssignmentsClient interface {
	Recv() (*ShardAssignments, error)
	grpc.ClientStream
}

type oxiaClientGetShardAssignmentsClient struct {
	grpc.ClientStream
}

func (x *oxiaClientGetShardAssignmentsClient) Recv() (*ShardAssignments, error) {
	m := new(ShardAssignments)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaClientClient) Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error) {
	out := new(WriteResponse)
	err := c.cc.Invoke(ctx, "/io.streamnative.oxia.proto.OxiaClient/Write", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaClientClient) WriteStream(ctx context.Context, opts ...grpc.CallOption) (OxiaClient_WriteStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaClient_ServiceDesc.Streams[1], "/io.streamnative.oxia.proto.OxiaClient/WriteStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaClientWriteStreamClient{stream}
	return x, nil
}

type OxiaClient_WriteStreamClient interface {
	Send(*WriteRequest) error
	Recv() (*WriteResponse, error)
	grpc.ClientStream
}

type oxiaClientWriteStreamClient struct {
	grpc.ClientStream
}

func (x *oxiaClientWriteStreamClient) Send(m *WriteRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *oxiaClientWriteStreamClient) Recv() (*WriteResponse, error) {
	m := new(WriteResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaClientClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (OxiaClient_ReadClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaClient_ServiceDesc.Streams[2], "/io.streamnative.oxia.proto.OxiaClient/Read", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaClientReadClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type OxiaClient_ReadClient interface {
	Recv() (*ReadResponse, error)
	grpc.ClientStream
}

type oxiaClientReadClient struct {
	grpc.ClientStream
}

func (x *oxiaClientReadClient) Recv() (*ReadResponse, error) {
	m := new(ReadResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaClientClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (OxiaClient_ListClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaClient_ServiceDesc.Streams[3], "/io.streamnative.oxia.proto.OxiaClient/List", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaClientListClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type OxiaClient_ListClient interface {
	Recv() (*ListResponse, error)
	grpc.ClientStream
}

type oxiaClientListClient struct {
	grpc.ClientStream
}

func (x *oxiaClientListClient) Recv() (*ListResponse, error) {
	m := new(ListResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaClientClient) RangeScan(ctx context.Context, in *RangeScanRequest, opts ...grpc.CallOption) (OxiaClient_RangeScanClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaClient_ServiceDesc.Streams[4], "/io.streamnative.oxia.proto.OxiaClient/RangeScan", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaClientRangeScanClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type OxiaClient_RangeScanClient interface {
	Recv() (*RangeScanResponse, error)
	grpc.ClientStream
}

type oxiaClientRangeScanClient struct {
	grpc.ClientStream
}

func (x *oxiaClientRangeScanClient) Recv() (*RangeScanResponse, error) {
	m := new(RangeScanResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaClientClient) GetNotifications(ctx context.Context, in *NotificationsRequest, opts ...grpc.CallOption) (OxiaClient_GetNotificationsClient, error) {
	stream, err := c.cc.NewStream(ctx, &OxiaClient_ServiceDesc.Streams[5], "/io.streamnative.oxia.proto.OxiaClient/GetNotifications", opts...)
	if err != nil {
		return nil, err
	}
	x := &oxiaClientGetNotificationsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type OxiaClient_GetNotificationsClient interface {
	Recv() (*NotificationBatch, error)
	grpc.ClientStream
}

type oxiaClientGetNotificationsClient struct {
	grpc.ClientStream
}

func (x *oxiaClientGetNotificationsClient) Recv() (*NotificationBatch, error) {
	m := new(NotificationBatch)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *oxiaClientClient) CreateSession(ctx context.Context, in *CreateSessionRequest, opts ...grpc.CallOption) (*CreateSessionResponse, error) {
	out := new(CreateSessionResponse)
	err := c.cc.Invoke(ctx, "/io.streamnative.oxia.proto.OxiaClient/CreateSession", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaClientClient) KeepAlive(ctx context.Context, in *SessionHeartbeat, opts ...grpc.CallOption) (*KeepAliveResponse, error) {
	out := new(KeepAliveResponse)
	err := c.cc.Invoke(ctx, "/io.streamnative.oxia.proto.OxiaClient/KeepAlive", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *oxiaClientClient) CloseSession(ctx context.Context, in *CloseSessionRequest, opts ...grpc.CallOption) (*CloseSessionResponse, error) {
	out := new(CloseSessionResponse)
	err := c.cc.Invoke(ctx, "/io.streamnative.oxia.proto.OxiaClient/CloseSession", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// OxiaClientServer is the server API for OxiaClient service.
// All implementations must embed UnimplementedOxiaClientServer
// for forward compatibility
type OxiaClientServer interface {
	// *
	// Gets all shard-to-server assignments as a stream. Each set of assignments
	// in the response stream will contain all the assignments to bring the client
	// up to date. For example, if a shard is split, the stream will return a
	// single response containing all the new shard assignments as opposed to
	// multiple stream responses, each containing a single shard assignment.
	//
	// Clients should connect to a single random server which will stream the
	// assignments for all shards on all servers.
	GetShardAssignments(*ShardAssignmentsRequest, OxiaClient_GetShardAssignmentsServer) error
	// *
	// Batches put, delete and delete_range requests.
	//
	// Clients should send this request to the shard leader. In the future,
	// this may be handled server-side in a proxy layer.
	//
	// Deprecated
	Write(context.Context, *WriteRequest) (*WriteResponse, error)
	// *
	// Batches put, delete and delete_range requests.
	//
	// Clients should send this request to the shard leader. In the future,
	// this may be handled server-side in a proxy layer.
	WriteStream(OxiaClient_WriteStreamServer) error
	// *
	// Batches get requests.
	//
	// Clients should send this request to the shard leader. In the future,
	// this may be handled server-side in a proxy layer.
	Read(*ReadRequest, OxiaClient_ReadServer) error
	// *
	// Requests all the keys between a range of keys.
	//
	// Clients should send an equivalent request to all respective shards,
	// unless a particular partition key was specified.
	List(*ListRequest, OxiaClient_ListServer) error
	// *
	// Requests all the records between a range of keys.
	//
	// Clients should send an equivalent request to all respective shards,
	// unless a particular partition key was specified.
	RangeScan(*RangeScanRequest, OxiaClient_RangeScanServer) error
	GetNotifications(*NotificationsRequest, OxiaClient_GetNotificationsServer) error
	// Creates a new client session. Sessions are kept alive by regularly sending
	// heartbeats via the KeepAlive rpc.
	CreateSession(context.Context, *CreateSessionRequest) (*CreateSessionResponse, error)
	// Sends a heartbeat to prevent the session from timing out.
	KeepAlive(context.Context, *SessionHeartbeat) (*KeepAliveResponse, error)
	// Closes a session and removes all ephemeral values associated with it.
	CloseSession(context.Context, *CloseSessionRequest) (*CloseSessionResponse, error)
	mustEmbedUnimplementedOxiaClientServer()
}

// UnimplementedOxiaClientServer must be embedded to have forward compatible implementations.
type UnimplementedOxiaClientServer struct {
}

func (UnimplementedOxiaClientServer) GetShardAssignments(*ShardAssignmentsRequest, OxiaClient_GetShardAssignmentsServer) error {
	return status.Errorf(codes.Unimplemented, "method GetShardAssignments not implemented")
}
func (UnimplementedOxiaClientServer) Write(context.Context, *WriteRequest) (*WriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Write not implemented")
}
func (UnimplementedOxiaClientServer) WriteStream(OxiaClient_WriteStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method WriteStream not implemented")
}
func (UnimplementedOxiaClientServer) Read(*ReadRequest, OxiaClient_ReadServer) error {
	return status.Errorf(codes.Unimplemented, "method Read not implemented")
}
func (UnimplementedOxiaClientServer) List(*ListRequest, OxiaClient_ListServer) error {
	return status.Errorf(codes.Unimplemented, "method List not implemented")
}
func (UnimplementedOxiaClientServer) RangeScan(*RangeScanRequest, OxiaClient_RangeScanServer) error {
	return status.Errorf(codes.Unimplemented, "method RangeScan not implemented")
}
func (UnimplementedOxiaClientServer) GetNotifications(*NotificationsRequest, OxiaClient_GetNotificationsServer) error {
	return status.Errorf(codes.Unimplemented, "method GetNotifications not implemented")
}
func (UnimplementedOxiaClientServer) CreateSession(context.Context, *CreateSessionRequest) (*CreateSessionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateSession not implemented")
}
func (UnimplementedOxiaClientServer) KeepAlive(context.Context, *SessionHeartbeat) (*KeepAliveResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KeepAlive not implemented")
}
func (UnimplementedOxiaClientServer) CloseSession(context.Context, *CloseSessionRequest) (*CloseSessionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CloseSession not implemented")
}
func (UnimplementedOxiaClientServer) mustEmbedUnimplementedOxiaClientServer() {}

// UnsafeOxiaClientServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to OxiaClientServer will
// result in compilation errors.
type UnsafeOxiaClientServer interface {
	mustEmbedUnimplementedOxiaClientServer()
}

func RegisterOxiaClientServer(s grpc.ServiceRegistrar, srv OxiaClientServer) {
	s.RegisterService(&OxiaClient_ServiceDesc, srv)
}

func _OxiaClient_GetShardAssignments_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ShardAssignmentsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OxiaClientServer).GetShardAssignments(m, &oxiaClientGetShardAssignmentsServer{stream})
}

type OxiaClient_GetShardAssignmentsServer interface {
	Send(*ShardAssignments) error
	grpc.ServerStream
}

type oxiaClientGetShardAssignmentsServer struct {
	grpc.ServerStream
}

func (x *oxiaClientGetShardAssignmentsServer) Send(m *ShardAssignments) error {
	return x.ServerStream.SendMsg(m)
}

func _OxiaClient_Write_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaClientServer).Write(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.streamnative.oxia.proto.OxiaClient/Write",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaClientServer).Write(ctx, req.(*WriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaClient_WriteStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(OxiaClientServer).WriteStream(&oxiaClientWriteStreamServer{stream})
}

type OxiaClient_WriteStreamServer interface {
	Send(*WriteResponse) error
	Recv() (*WriteRequest, error)
	grpc.ServerStream
}

type oxiaClientWriteStreamServer struct {
	grpc.ServerStream
}

func (x *oxiaClientWriteStreamServer) Send(m *WriteResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *oxiaClientWriteStreamServer) Recv() (*WriteRequest, error) {
	m := new(WriteRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _OxiaClient_Read_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReadRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OxiaClientServer).Read(m, &oxiaClientReadServer{stream})
}

type OxiaClient_ReadServer interface {
	Send(*ReadResponse) error
	grpc.ServerStream
}

type oxiaClientReadServer struct {
	grpc.ServerStream
}

func (x *oxiaClientReadServer) Send(m *ReadResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _OxiaClient_List_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OxiaClientServer).List(m, &oxiaClientListServer{stream})
}

type OxiaClient_ListServer interface {
	Send(*ListResponse) error
	grpc.ServerStream
}

type oxiaClientListServer struct {
	grpc.ServerStream
}

func (x *oxiaClientListServer) Send(m *ListResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _OxiaClient_RangeScan_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RangeScanRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OxiaClientServer).RangeScan(m, &oxiaClientRangeScanServer{stream})
}

type OxiaClient_RangeScanServer interface {
	Send(*RangeScanResponse) error
	grpc.ServerStream
}

type oxiaClientRangeScanServer struct {
	grpc.ServerStream
}

func (x *oxiaClientRangeScanServer) Send(m *RangeScanResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _OxiaClient_GetNotifications_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(NotificationsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(OxiaClientServer).GetNotifications(m, &oxiaClientGetNotificationsServer{stream})
}

type OxiaClient_GetNotificationsServer interface {
	Send(*NotificationBatch) error
	grpc.ServerStream
}

type oxiaClientGetNotificationsServer struct {
	grpc.ServerStream
}

func (x *oxiaClientGetNotificationsServer) Send(m *NotificationBatch) error {
	return x.ServerStream.SendMsg(m)
}

func _OxiaClient_CreateSession_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateSessionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaClientServer).CreateSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.streamnative.oxia.proto.OxiaClient/CreateSession",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaClientServer).CreateSession(ctx, req.(*CreateSessionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaClient_KeepAlive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SessionHeartbeat)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaClientServer).KeepAlive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.streamnative.oxia.proto.OxiaClient/KeepAlive",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaClientServer).KeepAlive(ctx, req.(*SessionHeartbeat))
	}
	return interceptor(ctx, in, info, handler)
}

func _OxiaClient_CloseSession_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CloseSessionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(OxiaClientServer).CloseSession(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.streamnative.oxia.proto.OxiaClient/CloseSession",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(OxiaClientServer).CloseSession(ctx, req.(*CloseSessionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// OxiaClient_ServiceDesc is the grpc.ServiceDesc for OxiaClient service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var OxiaClient_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.streamnative.oxia.proto.OxiaClient",
	HandlerType: (*OxiaClientServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Write",
			Handler:    _OxiaClient_Write_Handler,
		},
		{
			MethodName: "CreateSession",
			Handler:    _OxiaClient_CreateSession_Handler,
		},
		{
			MethodName: "KeepAlive",
			Handler:    _OxiaClient_KeepAlive_Handler,
		},
		{
			MethodName: "CloseSession",
			Handler:    _OxiaClient_CloseSession_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "GetShardAssignments",
			Handler:       _OxiaClient_GetShardAssignments_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "WriteStream",
			Handler:       _OxiaClient_WriteStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "Read",
			Handler:       _OxiaClient_Read_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "List",
			Handler:       _OxiaClient_List_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RangeScan",
			Handler:       _OxiaClient_RangeScan_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "GetNotifications",
			Handler:       _OxiaClient_GetNotifications_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "client.proto",
}
