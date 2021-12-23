package operator

import "oxia/proto"

func computeAssignments(availableNodes []string, replicationFactor uint32, shards uint32) *proto.ClusterStatus {
	// Do a round-robin assignment of leaders and followers across the shards

	cs := &proto.ClusterStatus{
		ReplicationFactor: 1,
		ShardsStatus:      []*proto.ShardStatus{},
	}

	nodesCount := len(availableNodes)
	s := int(shards)
	r := int(replicationFactor)

	for i := 0; i < s; i++ {
		ss := &proto.ShardStatus{
			Shard: uint32(i),
			Leader: &proto.ServerAddress{
				InternalUrl: availableNodes[i%nodesCount],
				//PublicUrl:   "unknown",
			},
		}

		for j := 1; j < r; j++ {
			ss.Followers = append(ss.Followers, &proto.ServerAddress{
				InternalUrl: availableNodes[(i+j)%nodesCount],
				//PublicUrl:   "unknown",
			})
		}

		cs.ShardsStatus = append(cs.ShardsStatus, ss)
	}

	return cs
}
