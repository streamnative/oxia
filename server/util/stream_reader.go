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

package util

import (
	"github.com/rs/zerolog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"oxia/common"
)

type Stream[T any] interface {
	Recv() (*T, error)
}

type Sender[U any] interface {
	Send(message *U) error
}

type StreamReader interface {
	Run() error
}

type streamReader[T any, U any] struct {
	closeCh       chan error
	stream        Stream[T]
	sender        Sender[U]
	labels        map[string]string
	ctx           context.Context
	handleMessage func(*T) (*U, error)
	log           zerolog.Logger
}

func (s *streamReader[T, U]) Run() error {
	go common.DoWithLabels(s.labels, func() { s.handleServerStream() })

	select {
	case err := <-s.closeCh:
		return err

	case <-s.ctx.Done():
		// Server is closing
		return nil
	}
}

func (s *streamReader[T, U]) handleServerStream() {
	for {
		if message, err := s.stream.Recv(); err != nil {
			s.close(err)
			return
		} else if message == nil {
			s.close(nil)
			return
		} else if res, err := s.handleMessage(message); err != nil {
			s.close(err)
			return
		} else if s.sender == nil {
			continue
		} else if err = s.sender.Send(res); err != nil {
			s.close(err)
			return
		}
	}
}

func (s *streamReader[T, U]) close(err error) {
	if err != nil && err != io.EOF && status.Code(err) != codes.Canceled {
		s.log.Warn().Err(err).
			Msg("error while handling stream")
	}

	if s.closeCh != nil {
		s.closeCh <- err
		close(s.closeCh)
		s.closeCh = nil
	}
}

func ReadStream[T any](
	stream Stream[T],
	handleMessage func(*T) error,
	labels map[string]string,
	ctx context.Context,
	log zerolog.Logger) StreamReader {
	return ProcessStream[T, any](
		stream,
		nil,
		func(message *T) (*any, error) {
			return nil, handleMessage(message)
		},
		labels,
		ctx,
		log,
	)
}

func ProcessStream[T any, U any](
	stream Stream[T],
	sender Sender[U],
	handleMessage func(*T) (*U, error),
	labels map[string]string,
	ctx context.Context,
	log zerolog.Logger) StreamReader {
	reader := &streamReader[T, U]{
		closeCh:       make(chan error),
		stream:        stream,
		sender:        sender,
		handleMessage: handleMessage,
		labels:        labels,
		ctx:           ctx,
		log:           log,
	}

	return reader
}
