package server

import (
	"errors"
	"github.com/rs/zerolog"
	"io"
	"net/url"
	"oxia/common"
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
		id:          sessionId,
		timeout:     time.Duration(sessionMetadata.TimeoutMs) * time.Millisecond,
		sm:          sm,
		heartbeatCh: make(chan *proto.SessionHeartbeat, 1),
		closeCh:     make(chan error),
		log: sm.log.With().
			Str("component", "session").
			Str("session-id", hexId(sessionId)).Logger(),
	}
	go s.waitForHeartbeats()
	s.log.Info().Msg("Session started")
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
	s.log.Debug().Msg("Session channels closed")
	s.sm.Lock()
	delete(s.sm.sessions, s.id)
	s.sm.Unlock()
}

func (s *session) delete() error {
	// Delete ephemeral data associated with this session
	sessionKey := SessionKey(s.id)
	// Read "index"
	list, err := s.sm.leaderController.Read(&proto.ReadRequest{
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
	s.log.Debug().Strs("keys", list.Lists[0].Keys).Msg("Keys to delete")
	for _, key := range list.Lists[0].Keys {
		unescapedKey, err := url.PathUnescape(key[len(sessionKey):])
		if err != nil {
			s.log.Error().Err(err).Str("key", sessionKey).Msg("Invalid session key")
			continue
		}
		if unescapedKey != "" {
			deletes = append(deletes, &proto.DeleteRequest{
				Key: unescapedKey,
			})
		}
	}
	_, err = s.sm.leaderController.Write(&proto.WriteRequest{
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
	s.log.Debug().Msg("Session deleted")
	return err

}

func (s *session) attach(server proto.OxiaClient_KeepAliveServer) (chan error, error) {
	s.Lock()
	if s.attached {
		s.log.Error().Msg("Session already attached")
		s.Unlock()
		return nil, common.ErrorInvalidSession
	}
	s.attached = true
	ch := s.closeCh
	s.Unlock()
	go s.receiveHeartbeats(server)
	return ch, nil
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
	for {
		var timer = time.NewTimer(s.timeout)
		var timeoutCh = timer.C
		select {

		case heartbeat := <-heartbeatChannel:
			if heartbeat == nil {
				// The channel is closed, so the session must be closing
				return
			}
			timer.Reset(s.timeout)
		case <-timeoutCh:
			s.log.Info().
				Msg("Session expired")

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
			s.closeCh <- err
			return
		}
		if heartbeat == nil {
			// closing already
			return
		}
		s.heartbeat(heartbeat)
	}

}
