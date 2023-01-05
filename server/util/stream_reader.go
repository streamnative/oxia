package util

import (
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
	Close(error)
	Run() <-chan error
}

type streamReader[T any, U any] struct {
	closeCh       chan error
	stream        Stream[T]
	labels        map[string]string
	handleMessage func(*T) (*U, error)
	handleError   func(error)
}

func (s *streamReader[T, U]) Run() <-chan error {
	go common.DoWithLabels(s.labels, func() { s.handleServerStream() })

	return s.closeCh
}

func (s *streamReader[T, U]) handleServerStream() {
	sender, isSender := s.stream.(Sender[U])
	for {
		if message, err := s.stream.Recv(); err != nil {
			s.Close(err)
			return
		} else if message == nil {
			s.Close(nil)
			return
		} else if res, err := s.handleMessage(message); err != nil {
			s.Close(err)
			return
		} else if !isSender {
			continue
		} else if err = sender.Send(res); err != nil {
			s.Close(err)
			return
		}
	}
}

func (s *streamReader[T, U]) Close(err error) {
	if err != nil && err != io.EOF && status.Code(err) != codes.Canceled {
		s.handleError(err)
	}

	if s.closeCh != nil {
		s.closeCh <- err
		close(s.closeCh)
		s.closeCh = nil
	}
}

func ReadStream[T any, U any](
	stream Stream[T],
	handleMessage func(*T) (*U, error),
	handleError func(error),
	labels map[string]string) StreamReader {
	reader := &streamReader[T, U]{
		closeCh:       make(chan error),
		stream:        stream,
		handleMessage: handleMessage,
		handleError:   handleError,
		labels:        labels,
	}

	return reader
}
