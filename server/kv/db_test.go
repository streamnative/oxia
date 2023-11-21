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

package kv

import (
	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/wal"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"
	"testing"
)

func TestDBSimple(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:               "a",
				Value:             []byte("0"),
				ExpectedVersionId: nil,
			},
			{ // Should succeed: asserting that the key does not exist
				Key:               "b",
				Value:             []byte("1"),
				ExpectedVersionId: pb.Int64(-1),
			},
			{ // Should fail: the version would mean that the key exists
				Key:               "c",
				Value:             []byte("2"),
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should fail: the version would mean that the key exists
				Key:               "d",
				Value:             []byte("3"),
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should succeed: asserting that the key does not exist
				Key:               "c",
				Value:             []byte("1"),
				ExpectedVersionId: pb.Int64(-1),
			},
		},
		Deletes: []*proto.DeleteRequest{
			{ // Should fail, non-existing key
				Key:               "non-existing",
				ExpectedVersionId: pb.Int64(-1),
			},
			{ // Should fail, version mismatch
				Key:               "c",
				ExpectedVersionId: pb.Int64(1),
			},
			{ // Should succeed, the key was inserted in the batch
				Key:               "c",
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should fail: the key was already just deleted
				Key:               "c",
				ExpectedVersionId: nil,
			},
		},
	}

	res, err := db.ProcessWrite(req, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(res.Puts))
	r0 := res.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.EqualValues(t, 0, r0.Version.ModificationsCount)

	r1 := res.Puts[1]
	assert.Equal(t, proto.Status_OK, r1.Status)
	assert.EqualValues(t, 0, r1.Version.VersionId)
	assert.EqualValues(t, 0, r1.Version.ModificationsCount)

	r2 := res.Puts[2]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r2.Status)
	assert.Nil(t, r2.Version)

	r3 := res.Puts[3]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r3.Status)
	assert.Nil(t, r3.Version)

	r4 := res.Puts[4]
	assert.Equal(t, proto.Status_OK, r4.Status)
	assert.EqualValues(t, 0, r4.Version.VersionId)
	assert.EqualValues(t, 0, r4.Version.ModificationsCount)

	assert.Equal(t, 4, len(res.Deletes))
	r5 := res.Deletes[0]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r5.Status)

	r6 := res.Deletes[1]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r6.Status)

	r7 := res.Deletes[2]
	assert.Equal(t, proto.Status_OK, r7.Status)

	r8 := res.Deletes[3]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r8.Status)

	// TODO: Add the request call and the verification
	/// Second batch
	//req = &proto.WriteRequest{
	//	Puts: []*proto.PutRequest{
	//		{ // Should succeed: no version check
	//			Key:             "a",
	//			Value:         []byte("00"),
	//			ExpectedVersion: nil,
	//		},
	//		{ // Should succeed: The version currently in the store is 0, the update just above will change
	//			// the version, though we do the verification before the batch is applied
	//			Key:             "a",
	//			Value:         []byte("111"),
	//			ExpectedVersion: pb.Int64(0),
	//		},
	//		{ // Should fail: the key already exists
	//			Key:             "b",
	//			Value:         []byte("2"),
	//			ExpectedVersion: pb.Int64(-1),
	//		},
	//		{ // Should succeed: the version is correct
	//			Key:             "b",
	//			Value:         []byte("2"),
	//			ExpectedVersion: pb.Int64(0),
	//		},
	//	},
	//	Deletes: []*proto.DeleteRequest{
	//		{ // Should fail: the key was not inserted before the batch
	//			Key:             "a",
	//			ExpectedVersion: nil,
	//		},
	//		{ // Should fail: the key was not inserted before the batch
	//			Key:             "c",
	//			ExpectedVersion: pb.Int64(-1),
	//		},
	//		{ // Should fail: the key was not inserted before the batch
	//			Key:             "c",
	//			ExpectedVersion: pb.Int64(0),
	//		},
	//	},
	//}

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBSameKeyMutations(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:               "k1",
				Value:             []byte("v0"),
				ExpectedVersionId: nil,
			},
		},
	}

	t0 := now()
	writeRes, err := db.ProcessWrite(writeReq, 0, t0, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(writeRes.Puts))
	r0 := writeRes.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.Equal(t, t0, r0.Version.CreatedTimestamp)
	assert.Equal(t, t0, r0.Version.ModifiedTimestamp)

	/// Second batch

	writeReq = &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: version is correct
				Key:               "k1",
				Value:             []byte("v1"),
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should fail: version has now changed to 1
				Key:               "k1",
				Value:             []byte("v2"),
				ExpectedVersionId: pb.Int64(0),
			},
		},
		Deletes: []*proto.DeleteRequest{
			{ // Should fail: the key was not inserted before the batch
				Key:               "k1",
				ExpectedVersionId: pb.Int64(0),
			},
		},
	}

	t1 := now()
	writeRes, err = db.ProcessWrite(writeReq, 1, t1, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(writeRes.Puts))
	r0 = writeRes.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 1, r0.Version.VersionId)
	assert.EqualValues(t, 1, r0.Version.ModificationsCount)
	assert.Equal(t, t0, r0.Version.CreatedTimestamp)
	assert.Equal(t, t1, r0.Version.ModifiedTimestamp)

	r1 := writeRes.Puts[1]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r1.Status)
	assert.Nil(t, r1.Version)

	r2 := writeRes.Deletes[0]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r2.Status)

	getRes, err := db.Get(&proto.GetRequest{
		// Should return version v1
		Key:          "k1",
		IncludeValue: true,
	})
	assert.NoError(t, err)

	assert.Equal(t, proto.Status_OK, getRes.Status)
	assert.EqualValues(t, 1, getRes.Version.VersionId)
	assert.Equal(t, "v1", string(getRes.Value))
	assert.Equal(t, t0, getRes.Version.CreatedTimestamp)
	assert.Equal(t, t1, getRes.Version.ModifiedTimestamp)

	getRes, err = db.Get(&proto.GetRequest{
		// Should return version v1, with no value
		Key:          "k1",
		IncludeValue: false,
	})
	assert.NoError(t, err)

	assert.Equal(t, proto.Status_OK, getRes.Status)
	assert.EqualValues(t, 1, getRes.Version.VersionId)
	assert.Nil(t, getRes.Value)
	assert.Equal(t, t0, getRes.Version.CreatedTimestamp)
	assert.Equal(t, t1, getRes.Version.ModifiedTimestamp)

	getRes, err = db.Get(&proto.GetRequest{
		// Should fail since the key is not there
		Key:          "non-existing",
		IncludeValue: true,
	})
	assert.NoError(t, err)

	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)
	assert.Nil(t, getRes.Version)
	assert.Nil(t, getRes.Value)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBList(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}, {
			Key:   "b",
			Value: []byte("b"),
		}, {
			Key:   "c",
			Value: []byte("c"),
		}, {
			Key:   "d",
			Value: []byte("d"),
		}, {
			Key:   "e",
			Value: []byte("e"),
		}},
	}

	writeRes, err := db.ProcessWrite(writeReq, wal.InvalidOffset, now(), NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(writeRes.Puts))

	listReq1 := &proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "c",
	}
	listReq2 := &proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "d",
	}
	listReq3 := &proto.ListRequest{
		StartInclusive: "xyz",
		EndExclusive:   "zzz",
	}

	// ["a", "c")
	keys1 := keyIteratorToSlice(db.List(listReq1))
	assert.Len(t, keys1, 2)
	assert.Equal(t, "a", keys1[0])
	assert.Equal(t, "b", keys1[1])

	// ["a", "d")
	keys2 := keyIteratorToSlice(db.List(listReq2))
	assert.Len(t, keys2, 3)
	assert.Equal(t, "a", keys2[0])
	assert.Equal(t, "b", keys2[1])
	assert.Equal(t, "c", keys2[2])

	// ["xyz", "zzz")
	keys3 := keyIteratorToSlice(db.List(listReq3))
	assert.Len(t, keys3, 0)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func keyIteratorToSlice(it KeyIterator, err error) []string {
	assert.NoError(nil, err)
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}
	_ = it.Close()
	return keys
}

func TestDBDeleteRange(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}, {
			Key:   "b",
			Value: []byte("b"),
		}, {
			Key:   "c",
			Value: []byte("c"),
		}, {
			Key:   "d",
			Value: []byte("d"),
		}, {
			Key:   "e",
			Value: []byte("e"),
		}},
	}

	_, err = db.ProcessWrite(writeReq, wal.InvalidOffset, 0, NoOpCallback)
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

	writeRes, err := db.ProcessWrite(writeReq, wal.InvalidOffset, 0, NoOpCallback)
	assert.NoError(t, err)

	keys := make([]string, 0)
	listIt, err := db.List(&proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "z",
	})

	assert.NoError(t, err)
	for ; listIt.Valid(); listIt.Next() {
		keys = append(keys, listIt.Key())
	}
	err = listIt.Close()
	assert.NoError(t, err)

	assert.Equal(t, 2, len(writeRes.DeleteRanges))
	assert.Equal(t, proto.Status_OK, writeRes.DeleteRanges[0].Status)
	assert.Equal(t, proto.Status_OK, writeRes.DeleteRanges[1].Status)

	// ["a", "z")
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, "a", keys[0])
	assert.Equal(t, "e", keys[1])

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_ReadCommitOffset(t *testing.T) {
	offset := int64(13)

	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	commitOffset, err := db.ReadCommitOffset()
	assert.NoError(t, err)
	assert.Equal(t, wal.InvalidOffset, commitOffset)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}},
	}
	_, err = db.ProcessWrite(writeReq, offset, 0, NoOpCallback)
	assert.NoError(t, err)

	commitOffset, err = db.ReadCommitOffset()
	assert.NoError(t, err)
	assert.Equal(t, offset, commitOffset)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDb_UpdateTerm(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	term, err := db.ReadTerm()
	assert.NoError(t, err)
	assert.Equal(t, wal.InvalidOffset, term)

	err = db.UpdateTerm(1)
	assert.NoError(t, err)

	term, err = db.ReadTerm()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, term)

	assert.NoError(t, db.Close())

	// Reopen and verify the term is maintained
	db, err = NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	term, err = db.ReadTerm()
	assert.NoError(t, err)
	assert.Equal(t, wal.InvalidOffset, term)

	assert.NoError(t, factory.Close())
}

func TestDB_Delete(t *testing.T) {
	offset := int64(13)

	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	db, err := NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}},
	}
	_, err = db.ProcessWrite(writeReq, offset, 0, NoOpCallback)
	assert.NoError(t, err)

	assert.NoError(t, db.Delete())

	// Reopen and verify the db is empty
	db, err = NewDB(common.DefaultNamespace, 1, factory, 0, common.SystemClock)
	assert.NoError(t, err)

	getRes, err := db.Get(&proto.GetRequest{
		Key:          "a",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)

	assert.NoError(t, factory.Close())
}
