package server

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"net/url"
	"oxia/proto"
	"sync"
	"time"
)

// --- Session

type session struct {
	sync.Mutex
	id          SessionId
	shardId     uint32
	timeout     time.Duration
	sm          *sessionManager
	attached    bool
	heartbeatCh chan *proto.SessionHeartbeat
	closeCh     chan error
	log         zerolog.Logger
}

func startSession(sessionId SessionId, sessionMetadata *proto.SessionMetadata, sm *sessionManager) *session {
	s := &session{
		Mutex:       sync.Mutex{},
		id:          sessionId,
		timeout:     time.Duration(sessionMetadata.TimeoutMS) * time.Millisecond,
		sm:          sm,
		heartbeatCh: make(chan *proto.SessionHeartbeat, 1),
		closeCh:     make(chan error),
		log: sm.log.With().
			Str("session-id", hexId(sessionId)).Logger(),
	}
	go s.waitForHeartbeats()
	return s
}

func (s *session) closeChannels() {
	if s.closeCh != nil {
		close(s.closeCh)
		s.closeCh = nil
	}
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}
}

func (s *session) delete() error {
	// Delete ephemeral data associated with this session
	sessionKey := SessionKey(s.id)
	// Read "index"
	list, err := s.sm.controller.Read(&proto.ReadRequest{
		ShardId: &s.shardId,
		Lists: []*proto.ListRequest{{
			StartInclusive: sessionKey,
			EndExclusive:   sessionKey + "/",
		}},
	})
	if err != nil {
		return err
	}
	// Delete ephemerals
	var deletes []*proto.DeleteRequest
	for _, key := range list.Lists[0].Keys {
		unescapedKey, err := url.PathUnescape(key[:len(sessionKey)])
		if err != nil {
			// TODO maybe only log the error and continue. Although this error should never happen
			return err
		}
		if unescapedKey != "" {
			deletes = append(deletes, &proto.DeleteRequest{
				Key: unescapedKey,
			})
		}
	}
	_, err = s.sm.controller.Write(&proto.WriteRequest{
		ShardId: &s.shardId,
		Puts:    nil,
		Deletes: deletes,
		// Delete the index and the session keys
		DeleteRanges: []*proto.DeleteRangeRequest{
			{
				StartInclusive: sessionKey,
				EndExclusive:   sessionKey + "/",
			},
		},
	})
	return err

}

func (s *session) heartbeat(heartbeat *proto.SessionHeartbeat) {
	s.Lock()
	defer s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- heartbeat
	}
}

func (s *session) waitForHeartbeats() {
	s.Lock()
	heartbeatChannel := s.heartbeatCh
	s.Unlock()
	s.log.Debug().Msg("Waiting for heartbeats")
	timeout := s.timeout
	for {
		select {

		case heartbeat := <-heartbeatChannel:
			if heartbeat == nil {
				// The channel is closed, so the session must be closing
				return
			}
		case <-time.After(timeout):
			s.log.Info().
				Msg("Session timed out")

			s.Lock()
			s.closeChannels()
			err := s.delete()

			if err != nil {
				s.log.Error().Err(err).
					Msg("Failed to delete session")
			}
			s.Unlock()
		}
	}
}

func (s *session) receiveHeartbeats(stream proto.OxiaClient_KeepAliveServer) {

	for {
		heartbeat, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// closing already
			return
		} else if err != nil {
			s.closeCh <- fmt.Errorf("session (sessionId=%d) already attached", s.id)
			return
		}
		if heartbeat == nil {
			// closing already
			return
		}
		s.heartbeat(heartbeat)
	}

}
