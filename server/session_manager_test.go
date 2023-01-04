package server

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"testing"
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

func (m mockWriteBatch) Delete(_ string) error {
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

func TestSessionUpdateOperationCallback(t *testing.T) {

	noSessionPutRequest := &proto.PutRequest{
		Key:     "a",
		Payload: []byte("b"),
	}
	writeBatch := mockWriteBatch{}

	status, err := SessionUpdateOperationCallback.OnPut(writeBatch, noSessionPutRequest)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	assert.Equal(t, len(writeBatch), 0)

	sessionId := uint64(12345)
	version := int64(2)
	writeBatch = mockWriteBatch{}
	sessionPutRequest := &proto.PutRequest{
		Key:             "a/b/c",
		Payload:         []byte("b"),
		ExpectedVersion: &version,
		SessionId:       &sessionId,
	}
	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)

	expectedErr := errors.New("error coming from the DB on read")
	writeBatch = mockWriteBatch{
		SessionKey(SessionId(sessionId)): expectedErr,
	}
	_, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest)
	assert.ErrorIs(t, err, expectedErr)

	writeBatch = mockWriteBatch{
		SessionKey(SessionId(sessionId)): []byte{},
	}
	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest)
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
	_, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest)
	assert.ErrorIs(t, err, expectedErr)
}

func TestNewSessionManager(t *testing.T) {
	withSessionManager(t, func(sManager SessionManager, controller LeaderController) {

	})
}

func withSessionManager(t *testing.T, f func(SessionManager, LeaderController)) {
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
	f(lc.(*leaderController).sessionManager, lc)
	assert.NoError(t, lc.Close())
}
