package server

//func TestUninitializedAssignmentDispatcher(t *testing.T) {
//	dispatcher := NewShardAssignmentDispatcher()
//	mockClient :=
//	assert.False(t, dispatcher.Initialized())
//	err := dispatcher.AddClient(mockClient)
//	assert.Error(t, err)
//}
//
//func TestShardAssignmentDispatcher_Initialized(t *testing.T) {
//	dispatcher := NewShardAssignmentDispatcher()
//	defer assert.NoError(t, dispatcher.Close())
//	coordinatorStream :=
//	go dispatcher.ShardAssignment(coordinatorStream)
//	assert.False(t, dispatcher.Initialized())
//	coordinatorStream.AddRequest(&proto.ShardAssignmentsResponse{
//		Assignments: []*proto.ShardAssignment{
//			{
//				ShardId: 0,
//				Leader:  "server1",
//				ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
//					Int32HashRange: &proto.Int32HashRange{
//						MinHashInclusive: 0,
//						MaxHashExclusive: 100,
//					},
//				},
//			}, {
//				ShardId: 82,
//				Leader:  "server2",
//				ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
//					Int32HashRange: &proto.Int32HashRange{
//						MinHashInclusive: 100,
//						MaxHashExclusive: math.MaxUint32,
//					},
//				},
//			},
//		},
//		ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
//	})
//	assert.Eventually(t, func() bool { return dispatcher.Initialized() }, 1*time.Second, 10*time.Millisecond)
//	mockClient := &mockServerStream[any, *proto.ShardAssignmentsResponse]{}
//	err := dispatcher.AddClient(mockClient)
//	assert.NoError(t, err)
//}
