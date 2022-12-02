package coordinator

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/proto"
)

// The NodeController takes care of checking the health-status of each node
// and to push all the service discovery updates
type NodeController interface {
	io.Closer
}

type nodeController struct {
	addr                     string
	shardAssignmentsProvider ShardAssignmentsProvider
	clientPool               common.ClientPool
	log                      zerolog.Logger
	closed                   bool
}

func NewNodeController(addr string, shardAssignmentsProvider ShardAssignmentsProvider, clientPool common.ClientPool) NodeController {
	nc := &nodeController{
		addr:                     addr,
		shardAssignmentsProvider: shardAssignmentsProvider,
		clientPool:               clientPool,
		log: log.With().
			Str("component", "node-controller").
			Str("addr", addr).
			Logger(),
	}

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller",
		"addr": nc.addr,
	}, func() {
		nc.healthCheck()
	})

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller-send-updates",
		"addr": nc.addr,
	}, func() {
		nc.sendAssignmentsUpdatesWithRetries()
	})
	return nc
}

func (n *nodeController) healthCheck() {
	// TODO: implement node health-check
}

func (n *nodeController) sendAssignmentsUpdatesWithRetries() {
	for !n.closed {
		err := n.sendAssignmentsUpdates()
		if err != nil {
			n.log.Warn().Err(err).
				Msg("Failed to send assignments updates to storage node")

			// TODO: add backoff logic after failures
			continue
		}
	}
}

func (n *nodeController) sendAssignmentsUpdates() error {
	rpc, err := n.clientPool.GetControlRpc(n.addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.DefaultRpcTimeout)
	defer cancel()

	stream, err := rpc.ShardAssignment(ctx)
	if err != nil {
		return err
	}

	var assignments *proto.ShardAssignmentsResponse
	for !n.closed {

		assignments = n.shardAssignmentsProvider.WaitForNextUpdate(assignments)
		if assignments == nil {
			continue
		}

		n.log.Debug().
			Interface("assignments", assignments).
			Msg("Sending assignments")

		if err := stream.Send(assignments); err != nil {
			n.log.Debug().Err(err).
				Msg("Failed to send assignments")
			return err
		}
	}

	return nil
}

func (n *nodeController) Close() error {
	n.closed = true
	return nil
}
