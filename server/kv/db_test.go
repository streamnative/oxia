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

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
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
		Deletes: []*proto.DeleteRequest{
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

	res, err := db.ProcessWrite(req)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(res.Puts))
	r0 := res.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Stat.Version)

	r1 := res.Puts[1]
	assert.Equal(t, proto.Status_OK, r1.Status)
	assert.EqualValues(t, 0, r1.Stat.Version)

	r2 := res.Puts[2]
	assert.Equal(t, proto.Status_BAD_VERSION, r2.Status)
	assert.Nil(t, r2.Stat)

	r3 := res.Puts[3]
	assert.Equal(t, proto.Status_BAD_VERSION, r3.Status)
	assert.Nil(t, r3.Stat)

	r4 := res.Puts[4]
	assert.Equal(t, proto.Status_OK, r4.Status)
	assert.EqualValues(t, 0, r4.Stat.Version)

	assert.Equal(t, 4, len(res.Deletes))
	r5 := res.Deletes[0]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r5.Status)

	r6 := res.Deletes[1]
	assert.Equal(t, proto.Status_BAD_VERSION, r6.Status)

	r7 := res.Deletes[2]
	assert.Equal(t, proto.Status_OK, r7.Status)

	r8 := res.Deletes[3]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r8.Status)

	/// Second batch

	req = &proto.WriteRequest{
		Puts: []*proto.PutRequest{
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
		Deletes: []*proto.DeleteRequest{
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

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:             "k1",
				Payload:         []byte("v0"),
				ExpectedVersion: nil,
			},
		},
	}

	writeRes, err := db.ProcessWrite(writeReq)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(writeRes.Puts))
	r0 := writeRes.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Stat.Version)

	/// Second batch

	writeReq = &proto.WriteRequest{
		Puts: []*proto.PutRequest{
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
		Deletes: []*proto.DeleteRequest{
			{ // Should fail: the key was not inserted before the batch
				Key:             "k1",
				ExpectedVersion: pb.Int64(0),
			},
		},
	}

	writeRes, err = db.ProcessWrite(writeReq)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(writeRes.Puts))
	r0 = writeRes.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 1, r0.Stat.Version)

	r1 := writeRes.Puts[1]
	assert.Equal(t, proto.Status_BAD_VERSION, r1.Status)
	assert.Nil(t, r1.Stat)

	r2 := writeRes.Deletes[0]
	assert.Equal(t, proto.Status_BAD_VERSION, r2.Status)

	readReq := &proto.ReadRequest{
		Gets: []*proto.GetRequest{
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
	}

	readRes, err := db.ProcessRead(readReq)
	assert.NoError(t, err)

	r3 := readRes.Gets[0]
	assert.Equal(t, proto.Status_OK, r3.Status)
	assert.EqualValues(t, 1, r3.Stat.Version)
	assert.Equal(t, "v1", string(r3.Payload))

	r4 := readRes.Gets[1]
	assert.Equal(t, proto.Status_OK, r4.Status)
	assert.EqualValues(t, 1, r4.Stat.Version)
	assert.Nil(t, r4.Payload)

	r5 := readRes.Gets[2]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r5.Status)
	assert.Nil(t, r5.Stat)
	assert.Nil(t, r5.Payload)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBGetRange(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	db, err := NewDB(1, factory)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("a"),
		}, {
			Key:     "b",
			Payload: []byte("b"),
		}, {
			Key:     "c",
			Payload: []byte("c"),
		}, {
			Key:     "d",
			Payload: []byte("d"),
		}, {
			Key:     "e",
			Payload: []byte("e"),
		}},
	}

	writeRes, err := db.ProcessWrite(writeReq)
	assert.NoError(t, err)

	readReq := &proto.ReadRequest{
		GetRanges: []*proto.GetRangeRequest{{
			StartInclusive: "a",
			EndExclusive:   "c",
		}, {
			StartInclusive: "a",
			EndExclusive:   "d",
		}, {
			StartInclusive: "xyz",
			EndExclusive:   "zzz",
		}},
	}

	readRes, err := db.ProcessRead(readReq)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(writeRes.Puts))

	assert.Equal(t, 3, len(readRes.GetRanges))

	// ["a", "c")
	r0 := readRes.GetRanges[0]
	assert.Equal(t, 2, len(r0.Keys))
	assert.Equal(t, "a", r0.Keys[0])
	assert.Equal(t, "b", r0.Keys[1])

	// ["a", "d")
	r1 := readRes.GetRanges[1]
	assert.Equal(t, 3, len(r1.Keys))
	assert.Equal(t, "a", r1.Keys[0])
	assert.Equal(t, "b", r1.Keys[1])
	assert.Equal(t, "c", r1.Keys[2])

	// ["xyz", "zzz")
	r2 := readRes.GetRanges[2]
	assert.Equal(t, 0, len(r2.Keys))

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBDeleteRange(t *testing.T) {
	factory := NewPebbleKVFactory(testKVOptions)
	db, err := NewDB(1, factory)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:     "a",
			Payload: []byte("a"),
		}, {
			Key:     "b",
			Payload: []byte("b"),
		}, {
			Key:     "c",
			Payload: []byte("c"),
		}, {
			Key:     "d",
			Payload: []byte("d"),
		}, {
			Key:     "e",
			Payload: []byte("e"),
		}},
	}

	_, err = db.ProcessWrite(writeReq)
	assert.NoError(t, err)

	writeReq = &proto.WriteRequest{
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "b",
			EndExclusive:   "c",
		}, {
			StartInclusive: "b",
			EndExclusive:   "e",
		}},
	}

	writeRes, err := db.ProcessWrite(writeReq)
	assert.NoError(t, err)

	readReq := &proto.ReadRequest{
		GetRanges: []*proto.GetRangeRequest{{
			StartInclusive: "a",
			EndExclusive:   "z",
		}},
	}

	readRes, err := db.ProcessRead(readReq)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(writeRes.DeleteRanges))
	assert.Equal(t, proto.Status_OK, writeRes.DeleteRanges[0].Status)
	assert.Equal(t, proto.Status_OK, writeRes.DeleteRanges[1].Status)

	assert.Equal(t, 1, len(readRes.GetRanges))

	// ["a", "z")
	r1 := readRes.GetRanges[0]
	assert.Equal(t, 2, len(r1.Keys))
	assert.Equal(t, "a", r1.Keys[0])
	assert.Equal(t, "e", r1.Keys[1])

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}
