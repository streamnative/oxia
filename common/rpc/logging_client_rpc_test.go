// Copyright 2023-2026 The Oxia Authors
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

package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/proto"
)

var errTestClientRpc = errors.New("rpc failed")

func TestLoggingClientRpcWrite(t *testing.T) {
	response := &proto.WriteResponse{}
	client := &loggingClientRpc{client: &fakeOxiaClientClient{
		write: func(*proto.WriteRequest) (*proto.WriteResponse, error) { return response, nil },
	}}
	res, err := client.Write(context.Background(), &proto.WriteRequest{})
	assert.NoError(t, err)
	assert.Same(t, response, res)

	client = &loggingClientRpc{client: &fakeOxiaClientClient{
		write: func(*proto.WriteRequest) (*proto.WriteResponse, error) { return response, errTestClientRpc },
	}}
	res, err = client.Write(context.Background(), &proto.WriteRequest{})
	assert.ErrorIs(t, err, errTestClientRpc)
	assert.Nil(t, res)
}

func TestLoggingClientRpcGetNotifications(t *testing.T) {
	stream := &fakeStream[proto.NotificationBatch]{}
	client := &loggingClientRpc{client: &fakeOxiaClientClient{
		getNotifications: func(*proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error) {
			return stream, nil
		},
	}}
	res, err := client.GetNotifications(context.Background(), &proto.NotificationsRequest{})
	assert.NoError(t, err)
	assert.Same(t, stream, res)

	client = &loggingClientRpc{client: &fakeOxiaClientClient{
		getNotifications: func(*proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error) {
			return stream, errTestClientRpc
		},
	}}
	res, err = client.GetNotifications(context.Background(), &proto.NotificationsRequest{})
	assert.ErrorIs(t, err, errTestClientRpc)
	assert.Nil(t, res)
}

func TestLoggingClientRpcGetSequenceUpdates(t *testing.T) {
	stream := &fakeStream[proto.GetSequenceUpdatesResponse]{}
	client := &loggingClientRpc{client: &fakeOxiaClientClient{
		getSequenceUpdates: func(*proto.GetSequenceUpdatesRequest) (proto.OxiaClient_GetSequenceUpdatesClient, error) {
			return stream, nil
		},
	}}
	res, err := client.GetSequenceUpdates(context.Background(), &proto.GetSequenceUpdatesRequest{})
	assert.NoError(t, err)
	assert.Same(t, stream, res)

	client = &loggingClientRpc{client: &fakeOxiaClientClient{
		getSequenceUpdates: func(*proto.GetSequenceUpdatesRequest) (proto.OxiaClient_GetSequenceUpdatesClient, error) {
			return stream, errTestClientRpc
		},
	}}
	res, err = client.GetSequenceUpdates(context.Background(), &proto.GetSequenceUpdatesRequest{})
	assert.ErrorIs(t, err, errTestClientRpc)
	assert.Nil(t, res)
}

// fakeOxiaClientClient answers the calls a test sets a hook for; the other
// calls panic on the nil embedded client.
type fakeOxiaClientClient struct {
	proto.OxiaClientClient
	write              func(*proto.WriteRequest) (*proto.WriteResponse, error)
	getNotifications   func(*proto.NotificationsRequest) (proto.OxiaClient_GetNotificationsClient, error)
	getSequenceUpdates func(*proto.GetSequenceUpdatesRequest) (proto.OxiaClient_GetSequenceUpdatesClient, error)
}

func (f *fakeOxiaClientClient) Write(
	_ context.Context, in *proto.WriteRequest, _ ...grpc.CallOption,
) (*proto.WriteResponse, error) {
	return f.write(in)
}

func (f *fakeOxiaClientClient) GetNotifications(
	_ context.Context, in *proto.NotificationsRequest, _ ...grpc.CallOption,
) (proto.OxiaClient_GetNotificationsClient, error) {
	return f.getNotifications(in)
}

func (f *fakeOxiaClientClient) GetSequenceUpdates(
	_ context.Context, in *proto.GetSequenceUpdatesRequest, _ ...grpc.CallOption,
) (proto.OxiaClient_GetSequenceUpdatesClient, error) {
	return f.getSequenceUpdates(in)
}

type fakeStream[T any] struct {
	grpc.ServerStreamingClient[T]
}
