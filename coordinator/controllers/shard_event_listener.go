package controllers

import "github.com/streamnative/oxia/coordinator/model"

type ShardEventListener interface {
	LeaderElected(shard int64, leader model.Server, followers []model.Server)

	ShardDeleted(shard int64)
}
