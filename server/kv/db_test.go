package kv

import (
	"github.com/stretchr/testify/assert"
	"oxia/proto"
	"testing"

	pb "google.golang.org/protobuf/proto"
)

func TestDBSimple(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	db, err := NewDB(1, factory)
	assert.NoError(t, err)

	req := &proto.BatchRequest{
		Put: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:             "a",
				Payload:         []byte("0"),
				ExpectedVersion: nil,
			},
			{ // Should succeed: asserting that the key does not exist
				Key:             "b",
				Payload:         []byte("1"),
				ExpectedVersion: pb.Int64(-1),
			},
			{ // Should fail: the version would mean that the key exists
				Key:             "c",
				Payload:         []byte("2"),
				ExpectedVersion: pb.Int64(0),
			},
			{ // Should fail: the version would mean that the key exists
				Key:             "d",
				Payload:         []byte("3"),
				ExpectedVersion: pb.Int64(0),
			},
			{ // Should succeed: asserting that the key does not exist
				Key:             "c",
				Payload:         []byte("1"),
				ExpectedVersion: pb.Int64(-1),
			},
		},
		Delete: []*proto.DeleteRequest{
			{ // Should fail, non-existing key
				Key:             "non-existing",
				ExpectedVersion: pb.Int64(-1),
			},
			{ // Should fail, version mismatch
				Key:             "c",
				ExpectedVersion: pb.Int64(1),
			},
			{ // Should succeed, the key was inserted in the batch
				Key:             "c",
				ExpectedVersion: pb.Int64(0),
			},
			{ // Should fail: the key was already just deleted
				Key:             "c",
				ExpectedVersion: nil,
			},
		},
	}

	res, err := db.ProcessBatch(req)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(res.GetPut()))
	r0 := res.GetPut()[0]
	assert.Equal(t, proto.Error_NO_ERROR, r0.Error)
	assert.EqualValues(t, 0, r0.Stat.Version)

	r1 := res.GetPut()[1]
	assert.Equal(t, proto.Error_NO_ERROR, r1.Error)
	assert.EqualValues(t, 0, r1.Stat.Version)

	r2 := res.GetPut()[2]
	assert.Equal(t, proto.Error_BAD_VERSION, r2.Error)
	assert.Nil(t, r2.Stat)

	r3 := res.GetPut()[3]
	assert.Equal(t, proto.Error_BAD_VERSION, r3.Error)
	assert.Nil(t, r3.Stat)

	r4 := res.GetPut()[4]
	assert.Equal(t, proto.Error_NO_ERROR, r4.Error)
	assert.EqualValues(t, 0, r4.Stat.Version)

	assert.Equal(t, 4, len(res.GetDelete()))
	r5 := res.GetDelete()[0]
	assert.Equal(t, proto.Error_KEY_NOT_FOUND, r5.Error)

	r6 := res.GetDelete()[1]
	assert.Equal(t, proto.Error_BAD_VERSION, r6.Error)

	r7 := res.GetDelete()[2]
	assert.Equal(t, proto.Error_NO_ERROR, r7.Error)

	r8 := res.GetDelete()[3]
	assert.Equal(t, proto.Error_KEY_NOT_FOUND, r8.Error)

	/// Second batch

	req = &proto.BatchRequest{
		Put: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:             "a",
				Payload:         []byte("00"),
				ExpectedVersion: nil,
			},
			{ // Should succeed: The version currently in the store is 0, the update just above will change
				// the version, though we do the verification before the batch is applied
				Key:             "a",
				Payload:         []byte("111"),
				ExpectedVersion: pb.Int64(0),
			},
			{ // Should fail: the key already exists
				Key:             "b",
				Payload:         []byte("2"),
				ExpectedVersion: pb.Int64(-1),
			},
			{ // Should succeed: the version is correct
				Key:             "b",
				Payload:         []byte("2"),
				ExpectedVersion: pb.Int64(0),
			},
		},
		Get: []*proto.GetRequest{
			{
				Key:            "a",
				IncludePayload: true,
			},
			{
				Key:            "a",
				IncludePayload: false,
			},
			{
				Key:            "b",
				IncludePayload: true,
			},
			{
				Key:            "c",
				IncludePayload: true,
			},
		},
		Delete: []*proto.DeleteRequest{
			{ // Should fail: the key was not inserted before the batch
				Key:             "a",
				ExpectedVersion: nil,
			},
			{ // Should fail: the key was not inserted before the batch
				Key:             "c",
				ExpectedVersion: pb.Int64(-1),
			},
			{ // Should fail: the key was not inserted before the batch
				Key:             "c",
				ExpectedVersion: pb.Int64(0),
			},
		},
	}

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBSameKeyMutations(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	db, err := NewDB(1, factory)
	assert.NoError(t, err)

	req := &proto.BatchRequest{
		Put: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:             "k1",
				Payload:         []byte("v0"),
				ExpectedVersion: nil,
			},
		},
	}

	res, err := db.ProcessBatch(req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(res.GetPut()))
	r0 := res.GetPut()[0]
	assert.Equal(t, proto.Error_NO_ERROR, r0.Error)
	assert.EqualValues(t, 0, r0.Stat.Version)

	/// Second batch

	req = &proto.BatchRequest{
		Put: []*proto.PutRequest{
			{ // Should succeed: version is correct
				Key:             "k1",
				Payload:         []byte("v1"),
				ExpectedVersion: pb.Int64(0),
			},
			{ // Should fail: version has now changed to 1
				Key:             "k1",
				Payload:         []byte("v2"),
				ExpectedVersion: pb.Int64(0),
			},
		},
		Get: []*proto.GetRequest{
			{ // Should return version v1
				Key:            "k1",
				IncludePayload: true,
			},
			{ // Should return version v1, with no value
				Key:            "k1",
				IncludePayload: false,
			},
			{ // Should fail since the key is not there
				Key:            "non-existing",
				IncludePayload: true,
			},
		},
		Delete: []*proto.DeleteRequest{
			{ // Should fail: the key was not inserted before the batch
				Key:             "k1",
				ExpectedVersion: pb.Int64(0),
			},
		},
	}

	res, err = db.ProcessBatch(req)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(res.GetPut()))
	r0 = res.GetPut()[0]
	assert.Equal(t, proto.Error_NO_ERROR, r0.Error)
	assert.EqualValues(t, 1, r0.Stat.Version)

	r1 := res.GetPut()[1]
	assert.Equal(t, proto.Error_BAD_VERSION, r1.Error)
	assert.Nil(t, r1.Stat)

	r2 := res.GetGet()[0]
	assert.Equal(t, proto.Error_NO_ERROR, r2.Error)
	assert.EqualValues(t, 1, r2.Stat.Version)
	assert.Equal(t, "v1", string(r2.Payload))

	r3 := res.GetGet()[1]
	assert.Equal(t, proto.Error_NO_ERROR, r3.Error)
	assert.EqualValues(t, 1, r3.Stat.Version)
	assert.Nil(t, r3.Payload)

	r4 := res.GetGet()[2]
	assert.Equal(t, proto.Error_KEY_NOT_FOUND, r4.Error)
	assert.Nil(t, r4.Stat)
	assert.Nil(t, r4.Payload)

	r5 := res.GetDelete()[0]
	assert.Equal(t, proto.Error_BAD_VERSION, r5.Error)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}
