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
