package controllers

import "github.com/oxia-db/oxia/coordinator/model"

type NodeEventListener interface {
	NodeBecameUnavailable(node model.Server)
}
