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

package server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/streamnative/oxia/server/wal"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
)

func TestSessionKey(t *testing.T) {
	id := SessionId(0xC0DE)
	sessionKey := SessionKey(id)
	assert.Equal(t, "__oxia/session/000000000000c0de", sessionKey)
	parsed, err := KeyToId(sessionKey)
	assert.NoError(t, err)
	assert.Equal(t, id, parsed)

	for _, key := range []string{"__oxia/session/", "too_short"} {
		_, err = KeyToId(key)
		assert.Error(t, err, key)
	}
}

type mockWriteBatch map[string]any

func (m mockWriteBatch) Count() int {
	return 0
}

func (m mockWriteBatch) Size() int {
	return 0
}

var _ kv.WriteBatch = (*mockWriteBatch)(nil)

type mockCloser struct{}

var _ io.Closer = (*mockCloser)(nil)

func (m mockCloser) Close() error {
	return nil
}

func (m mockWriteBatch) Close() error {
	return nil
}

func (m mockWriteBatch) Put(key string, value []byte) error {
	val, found := m[key]
	if found {
		if valAsError, wasError := val.(error); wasError {
			return valAsError
		}
	}
	m[key] = value
	return nil
}

func (m mockWriteBatch) Delete(key string) error {
	delete(m, key)
	return nil
}

func (m mockWriteBatch) Get(key string) ([]byte, io.Closer, error) {
	val, found := m[key]
	if !found {
		return nil, nil, kv.ErrKeyNotFound
	}
	err, wasError := val.(error)
	if wasError {
		return nil, nil, err
	}
	return val.([]byte), &mockCloser{}, nil
}

func (m mockWriteBatch) FindLower(key string) (string, error) {
	return "", errors.New("not implemented")
}

func (m mockWriteBatch) DeleteRange(_, _ string) error {
	return nil
}

func (m mockWriteBatch) KeyRangeScan(_, _ string) (kv.KeyIterator, error) {
	return nil, kv.ErrKeyNotFound
}

func (m mockWriteBatch) RangeScan(_, _ string) (kv.KeyValueIterator, error) {
	return nil, kv.ErrKeyNotFound
}

func (m mockWriteBatch) Commit() error {
	return nil
}

func TestSessionUpdateOperationCallback_OnPut(t *testing.T) {
	sessionId := int64(12345)
	versionId := int64(2)

	noSessionPutRequest := &proto.PutRequest{
		Key:   "a/b/c",
		Value: []byte("b"),
	}
	sessionPutRequest := &proto.PutRequest{
		Key:               "a/b/c",
		Value:             []byte("b"),
		ExpectedVersionId: &versionId,
		SessionId:         &sessionId,
	}

	writeBatch := mockWriteBatch{}

	status, err := SessionUpdateOperationCallback.OnPut(writeBatch, noSessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	assert.Equal(t, len(writeBatch), 0)

	writeBatch = mockWriteBatch{
		"a/b/c": []byte{},
		ShadowKey(SessionId(sessionId-1), "a/b/c"): []byte{},
	}

	se := &proto.StorageEntry{
		Value:                 []byte("value"),
		VersionId:             0,
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
		ShadowKey(SessionId(sessionId-1), "a/b/c"): []byte{},
		SessionKey(SessionId(sessionId - 1)):       []byte{},
		SessionKey(SessionId(sessionId)):           []byte{},
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

	// session (sessionID -1) entry
	tmpSessionId := sessionId - 1
	se = &proto.StorageEntry{
		Value:                 []byte("value"),
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &tmpSessionId,
	}
	// sessionID has expired
	writeBatch = mockWriteBatch{
		"a/b/c": []byte{}, // real data
		ShadowKey(SessionId(sessionId-1), "a/b/c"): []byte{}, // shadow key
		SessionKey(SessionId(sessionId - 1)):       []byte{}, // session
	}
	// try to use current session override the (sessionID -1)
	sessionPutRequest = &proto.PutRequest{
		Key:       "a/b/c",
		Value:     []byte("b"),
		SessionId: &sessionId,
	}

	status, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)
	_, closer, err := writeBatch.Get(ShadowKey(SessionId(sessionId-1), "a/b/c"))
	assert.NoError(t, err)
	closer.Close()

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
	sessionShadowKey := ShadowKey(SessionId(sessionId), "a/b/c")
	_, found := writeBatch[sessionShadowKey]
	assert.True(t, found)

	expectedErr = errors.New("error coming from the DB on write")
	writeBatch = mockWriteBatch{
		SessionKey(SessionId(sessionId)): []byte{},
		sessionShadowKey:                 expectedErr,
	}
	_, err = SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, nil)
	assert.ErrorIs(t, err, expectedErr)
}

func storageEntry(t *testing.T, sessionId int64) []byte {
	t.Helper()

	entry := &proto.StorageEntry{
		Value:                 nil,
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &sessionId,
	}
	bytes, err := pb.Marshal(entry)
	assert.NoError(t, err)
	return bytes
}

func TestSessionUpdateOperationCallback_OnDelete(t *testing.T) {
	sessionId := int64(12345)

	writeBatch := mockWriteBatch{
		"a/b/c": storageEntry(t, sessionId),
		SessionKey(SessionId(sessionId)) + "/a%2Fb%2Fc": []byte{},
	}

	err := SessionUpdateOperationCallback.OnDelete(writeBatch, "a/b/c")
	assert.NoError(t, err)
	_, found := writeBatch[SessionKey(SessionId(sessionId))+"/a%2Fb%2Fc"]
	assert.False(t, found)
}

func TestSessionManager(t *testing.T) {
	shardId := int64(1)
	// Invalid session timeout
	kvf, walf, sManager, lc := createSessionManager(t)
	_, err := sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32((1 * time.Hour).Milliseconds()),
	})
	assert.ErrorIs(t, err, common.ErrorInvalidSessionTimeout)
	_, err = sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32((1 * time.Second).Milliseconds()),
	})
	assert.ErrorIs(t, err, common.ErrorInvalidSessionTimeout)

	// Create and close a session, check if its persisted
	createResp, err := sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: 5 * 1000,
	})
	assert.NoError(t, err)
	sessionId := createResp.SessionId
	meta := getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint32(5000), meta.TimeoutMs)

	_, err = sManager.CloseSession(&proto.CloseSessionRequest{
		Shard:     shardId,
		SessionId: sessionId,
	})
	assert.NoError(t, err)
	assert.Nil(t, getSessionMetadata(t, lc, sessionId))

	// Create a session, watch it time out
	createResp, err = sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
	}, 0)
	assert.NoError(t, err)
	newSessionId := createResp.SessionId
	assert.NotEqual(t, sessionId, newSessionId)
	sessionId = newSessionId
	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)

	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId) == nil
	}, time.Second, 30*time.Millisecond)

	// Create a session, keep it alive
	createResp, err = sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
	}, 0)
	assert.NoError(t, err)
	sessionId = createResp.SessionId
	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	keepAlive(t, sManager, sessionId, err, 30*time.Millisecond, 6)
	time.Sleep(200 * time.Millisecond)
	assert.NotNil(t, getSessionMetadata(t, lc, sessionId))

	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId) == nil
	}, 10*time.Second, 30*time.Millisecond)

	// Create a session, put an ephemeral value
	createResp, err = sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
	}, 0)
	assert.NoError(t, err)
	sessionId = createResp.SessionId
	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	keepAlive(t, sManager, sessionId, err, 30*time.Millisecond, 6)

	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "a/b",
			Value:     []byte("a/b"),
			SessionId: &sessionId,
		}},
	})
	assert.NoError(t, err)
	assert.Equal(t, "a/b", getData(t, lc, "a/b"))
	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId) == nil &&
			getData(t, lc, "a/b") == ""
	}, 10*time.Second, 30*time.Millisecond)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvf.Close())
	assert.NoError(t, walf.Close())
}

func TestMultipleSessionsExpiry(t *testing.T) {
	shardId := int64(1)
	// Invalid session timeout
	kvf, walf, sManager, lc := createSessionManager(t)

	// Create 2 sessions
	createResp1, err := sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(3000),
		ClientIdentity:   "session-1",
	}, 0)
	assert.NoError(t, err)
	sessionId1 := createResp1.SessionId

	createResp2, err := sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
		ClientIdentity:   "session-2",
	}, 0)
	assert.NoError(t, err)
	sessionId2 := createResp2.SessionId

	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "/ephemeral-1",
			Value:     []byte("hello"),
			SessionId: &sessionId1,
		}},
	})
	assert.NoError(t, err)

	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "/ephemeral-2",
			Value:     []byte("hello"),
			SessionId: &sessionId2,
		}},
	})
	assert.NoError(t, err)

	// Let session-2 expire and verify its key was deleted
	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId2) == nil
	}, 10*time.Second, 30*time.Millisecond)

	readCh := lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shardId,
		Gets: []*proto.GetRequest{{
			Key:          "/ephemeral-1",
			IncludeValue: true,
		}, {
			Key:          "/ephemeral-2",
			IncludeValue: true,
		}},
	})

	// ephemeral-1
	rr, ok := <-readCh
	assert.True(t, ok)
	assert.NoError(t, rr.Err)
	assert.Equal(t, proto.Status_OK, rr.Response.Status)

	// ephemeral-2
	rr, ok = <-readCh
	assert.True(t, ok)
	assert.NoError(t, rr.Err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, rr.Response.Status)

	// Now Let session-1 expire and verify its key was deleted
	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId1) == nil
	}, 10*time.Second, 30*time.Millisecond)

	readCh = lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shardId,
		Gets: []*proto.GetRequest{{
			Key:          "/ephemeral-1",
			IncludeValue: true,
		}, {
			Key:          "/ephemeral-2",
			IncludeValue: true,
		}},
	})

	// ephemeral-1
	rr, ok = <-readCh
	assert.True(t, ok)
	assert.NoError(t, rr.Err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, rr.Response.Status)

	// ephemeral-2
	rr, ok = <-readCh
	assert.True(t, ok)
	assert.NoError(t, rr.Err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, rr.Response.Status)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvf.Close())
	assert.NoError(t, walf.Close())
}

func TestSessionManagerReopening(t *testing.T) {
	shardId := int64(1)
	// Invalid session timeout
	walf, kvf, sManager, lc := createSessionManager(t)

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:   "/ledgers",
			Value: []byte("a"),
		}, {
			Key:   "/admin",
			Value: []byte("a"),
		}, {
			Key:   "/test",
			Value: []byte("a"),
		}},
	})
	assert.NoError(t, err)

	// Create session and reopen the session manager
	createResp, err := sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: 5 * 1000,
	})
	assert.NoError(t, err)
	sessionId := createResp.SessionId
	meta := getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint32(5000), meta.TimeoutMs)

	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "/a/b",
			Value:     []byte("/a/b"),
			SessionId: &sessionId,
		}},
	})
	assert.NoError(t, err)
	assert.Equal(t, "/a/b", getData(t, lc, "/a/b"))

	lc = reopenLeaderController(t, walf, kvf, lc)

	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint32(5000), meta.TimeoutMs)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvf.Close())
	assert.NoError(t, walf.Close())
}

func getData(t *testing.T, lc *leaderController, key string) string {
	t.Helper()

	resp, err := lc.db.Get(&proto.GetRequest{
		Key:          key,
		IncludeValue: true,
	})
	assert.NoError(t, err)
	if resp.Status != proto.Status_KEY_NOT_FOUND {
		return string(resp.Value)
	}
	return ""
}

func keepAlive(t *testing.T, sManager *sessionManager, sessionId int64, err error, sleepTime time.Duration, heartbeatCount int) {
	t.Helper()

	go func() {
		assert.NoError(t, err)
		for i := 0; i < heartbeatCount; i++ {
			time.Sleep(sleepTime)
			assert.NoError(t, sManager.KeepAlive(sessionId))
		}
	}()
}

func getSessionMetadata(t *testing.T, lc *leaderController, sessionId int64) *proto.SessionMetadata {
	t.Helper()

	resp, err := lc.db.Get(&proto.GetRequest{
		Key:          SessionKey(SessionId(sessionId)),
		IncludeValue: true,
	})
	assert.NoError(t, err)

	found := resp.Status == proto.Status_OK
	if !found {
		return nil
	}
	meta := proto.SessionMetadata{}
	err = pb.Unmarshal(resp.Value, &meta)
	assert.NoError(t, err)
	return &meta
}

func createSessionManager(t *testing.T) (kv.Factory, wal.Factory, *sessionManager, *leaderController) {
	t.Helper()

	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)
	lc, err := NewLeaderController(Config{NotificationsRetentionTime: 10 * time.Second}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	assert.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	sessionManager := lc.(*leaderController).sessionManager.(*sessionManager)
	assert.NoError(t, sessionManager.ctx.Err())
	return kvFactory, walFactory, sessionManager, lc.(*leaderController)
}

func reopenLeaderController(t *testing.T, kvFactory kv.Factory, walFactory wal.Factory, oldlc *leaderController) *leaderController {
	t.Helper()

	var shard int64 = 1

	assert.NoError(t, oldlc.Close())

	var err error
	lc, err := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	assert.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	return lc.(*leaderController)
}

func TestSession_PutWithExpiredSession(t *testing.T) {
	var oldSessionId int64 = 100
	var newSessionId int64 = 101

	se := &proto.StorageEntry{
		Value:                 []byte("value"),
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &oldSessionId,
	}
	// sessionID has expired
	writeBatch := mockWriteBatch{
		"a/b/c": []byte{}, // real data
		ShadowKey(SessionId(oldSessionId), "a/b/c"): []byte{}, // shadow key
		SessionKey(SessionId(oldSessionId)):         []byte{}, // session
	}
	// try to use current session override the (sessionID -1)
	sessionPutRequest := &proto.PutRequest{
		Key:       "a/b/c",
		Value:     []byte("b"),
		SessionId: &newSessionId,
	}

	status, err := SessionUpdateOperationCallback.OnPut(writeBatch, sessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)

	_, closer, err := writeBatch.Get(ShadowKey(SessionId(oldSessionId), "a/b/c"))
	assert.NoError(t, err)
	assert.NoError(t, closer.Close())
}
