package server

import (
	"errors"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"
	"io"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
	"time"
)

func TestSessionKey(t *testing.T) {
	id := SessionId(0xC0DE)
	sessionKey := SessionKey(id)
	assert.Equal(t, "__oxia/session/000000000000c0de/", sessionKey)
	parsed, err := KeyToId(sessionKey)
	assert.NoError(t, err)
	assert.Equal(t, id, parsed)

	for _, key := range []string{"__oxia/session/", "too_short", "__oxia/session/000000000000dead5", "__oxia/session/000000000000woof/"} {
		_, err = KeyToId(key)
		assert.Error(t, err)
	}

}

type mockWriteBatch map[string]any

var _ kv.WriteBatch = (*mockWriteBatch)(nil)

type mockCloser struct{}

var _ io.Closer = (*mockCloser)(nil)

func (m mockCloser) Close() error {
	return nil
}

func (m mockWriteBatch) Close() error {
	return nil
}

func (m mockWriteBatch) Put(key string, payload []byte) error {
	val, found := m[key]
	if found {
		if valAsError, wasError := val.(error); wasError {
			return valAsError
		}
	}
	m[key] = payload
	return nil
}

func (m mockWriteBatch) Delete(key string) error {
	delete(m, key)
	return nil
}

func (m mockWriteBatch) Get(key string) ([]byte, io.Closer, error) {
	val, found := m[key]
	if !found {
		return nil, nil, kv.ErrorKeyNotFound
	}
	err, wasError := val.(error)
	if wasError {
		return nil, nil, err
	}
	return val.([]byte), &mockCloser{}, nil

}

func (m mockWriteBatch) DeleteRange(_, _ string) error {
	return nil
}

func (m mockWriteBatch) KeyRangeScan(_, _ string) kv.KeyIterator {
	return nil
}

func (m mockWriteBatch) Commit() error {
	return nil
}

func TestSessionUpdateOperationCallback_OnPut(t *testing.T) {
	sessionId := uint64(12345)
	version := int64(2)

	noSessionPutRequest := &proto.PutRequest{
		Key:     "a/b/c",
		Payload: []byte("b"),
	}
	sessionPutRequest := &proto.PutRequest{
		Key:             "a/b/c",
		Payload:         []byte("b"),
		ExpectedVersion: &version,
		SessionId:       &sessionId,
	}

	writeBatch := mockWriteBatch{}

	status, err := SessionUpdateOperationCallback.OnPut(writeBatch, noSessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	assert.Equal(t, len(writeBatch), 0)

	writeBatch = mockWriteBatch{
		"a/b/c": []byte{},
		SessionKey(SessionId(sessionId-1)) + "a%2Fb%2Fc": []byte{},
	}

	se := &proto.StorageEntry{
		Payload:               []byte("payload"),
		Version:               0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &sessionId,
	}

	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, noSessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	_, oldKeyFound := writeBatch[SessionKey(SessionId(sessionId))+"a"]
	assert.False(t, oldKeyFound)

	writeBatch = mockWriteBatch{
		"a/b/c": []byte{},
		SessionKey(SessionId(sessionId-1)) + "a%2Fb%2Fc": []byte{},
		SessionKey(SessionId(sessionId - 1)):             []byte{},
		SessionKey(SessionId(sessionId)):                 []byte{},
	}

	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	_, oldKeyFound = writeBatch[SessionKey(SessionId(sessionId-1))+"a"]
	assert.False(t, oldKeyFound)
	_, newKeyFound := writeBatch[SessionKey(SessionId(sessionId))+"a"]
	assert.False(t, newKeyFound)

	writeBatch = mockWriteBatch{}
	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)

	expectedErr := errors.New("error coming from the DB on read")
	writeBatch = mockWriteBatch{
		SessionKey(SessionId(sessionId)): expectedErr,
	}
	_, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, nil)
	assert.ErrorIs(t, err, expectedErr)

	writeBatch = mockWriteBatch{
		SessionKey(SessionId(sessionId)): []byte{},
	}
	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	sessionKey := SessionKey(SessionId(sessionId)) + "a%2Fb%2Fc"
	_, found := writeBatch[sessionKey]
	assert.True(t, found)

	expectedErr = errors.New("error coming from the DB on write")
	writeBatch = mockWriteBatch{
		SessionKey(SessionId(sessionId)): []byte{},
		sessionKey:                       expectedErr,
	}
	_, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, nil)
	assert.ErrorIs(t, err, expectedErr)
}

func storageEntry(t *testing.T, sessionId uint64) []byte {
	entry := &proto.StorageEntry{
		Payload:               nil,
		Version:               0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &sessionId,
	}
	bytes, err := pb.Marshal(entry)
	assert.NoError(t, err)
	return bytes
}

func TestSessionUpdateOperationCallback_OnDelete(t *testing.T) {
	sessionId := uint64(12345)

	writeBatch := mockWriteBatch{
		"a/b/c": storageEntry(t, sessionId),
		SessionKey(SessionId(sessionId)) + "a%2Fb%2Fc": []byte{},
	}

	err := SessionUpdateOperationCallback.OnDelete(writeBatch, "a/b/c")
	assert.NoError(t, err)
	_, found := writeBatch[SessionKey(SessionId(sessionId))+"a%2Fb%2Fc"]
	assert.False(t, found)

}

func TestSessionManager(t *testing.T) {
	// Invalid session timeout
	sManager, controller := createSessionManager(t)
	_, err := sManager.CreateSession(&proto.CreateSessionRequest{
		ShardId:          1,
		SessionTimeoutMs: 60 * 1000,
	})
	assert.ErrorIs(t, err, ErrorInvalidSessionTimeout)

	// Create and close a session, check if its persisted
	createResp, err := sManager.CreateSession(&proto.CreateSessionRequest{
		ShardId:          1,
		SessionTimeoutMs: 5 * 1000,
	})
	assert.NoError(t, err)
	sessionId := createResp.SessionId
	meta := getSessionMetadata(t, controller, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint64(5000), meta.TimeoutMS)

	_, err = sManager.CloseSession(&proto.CloseSessionRequest{
		ShardId:   1,
		SessionId: sessionId,
	})
	assert.NoError(t, err)
	assert.Nil(t, getSessionMetadata(t, controller, sessionId))

	// Create a session, watch it time out
	createResp, err = sManager.CreateSession(&proto.CreateSessionRequest{
		ShardId:          1,
		SessionTimeoutMs: 50,
	})
	assert.NoError(t, err)
	newSessionId := createResp.SessionId
	assert.NotEqual(t, sessionId, newSessionId)
	sessionId = newSessionId
	meta = getSessionMetadata(t, controller, sessionId)
	assert.NotNil(t, meta)

	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, controller, sessionId) == nil
	}, time.Second, 30*time.Millisecond)

	assert.NoError(t, controller.Close())

}

func getSessionMetadata(t *testing.T, controller *leaderController, sessionId uint64) *proto.SessionMetadata {
	shard := uint32(1)
	resp, err := controller.db.ProcessRead(&proto.ReadRequest{
		ShardId: &shard,
		Gets: []*proto.GetRequest{{
			Key:            SessionKey(SessionId(sessionId)),
			IncludePayload: true,
		}},
		Lists: nil,
	})
	assert.NoError(t, err)

	found := resp.Gets[0].Status == proto.Status_OK
	if !found {
		return nil
	}
	meta := proto.SessionMetadata{}
	err = pb.Unmarshal(resp.Gets[0].Payload, &meta)
	assert.NoError(t, err)
	return &meta
}

func createSessionManager(t *testing.T) (*sessionManager, *leaderController) {
	var shard uint32 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewInMemoryWalFactory()
	var lc LeaderController
	lc, err = NewLeaderController(shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)
	_, err = lc.Fence(&proto.FenceRequest{ShardId: shard, Epoch: 1})
	assert.NoError(t, err)
	_, err = lc.BecomeLeader(&proto.BecomeLeaderRequest{
		ShardId:           shard,
		Epoch:             1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)
	return lc.(*leaderController).sessionManager.(*sessionManager), lc.(*leaderController)
}
