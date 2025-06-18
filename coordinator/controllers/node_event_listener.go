package controllers

import "github.com/streamnative/oxia/coordinator/model"

type NodeEventListener interface {
	NodeBecameUnavailable(node model.Server)
}
