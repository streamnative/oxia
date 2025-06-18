package controllers

import "github.com/oxia-db/oxia/coordinator/model"

type ShardEventListener interface {
	LeaderElected(shard int64, leader model.Server, followers []model.Server)

	ShardDeleted(shard int64)
}
