package impl

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/health/grpc_health_v1"
	"io"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/coordinator/model"
	"oxia/proto"
	"sync"
	"sync/atomic"
	"time"
)

type NodeStatus uint32

const (
	Running NodeStatus = iota
	NotRunning
)

const (
	healthCheckProbeInterval = 2 * time.Second
	healthCheckProbeTimeout  = 2 * time.Second
)

// The NodeController takes care of checking the health-status of each node
// and to push all the service discovery updates
type NodeController interface {
	io.Closer

	Status() NodeStatus
}

type nodeController struct {
	sync.Mutex
	addr                     model.ServerAddress
	status                   NodeStatus
	shardAssignmentsProvider ShardAssignmentsProvider
	nodeAvailabilityListener NodeAvailabilityListener
	rpc                      RpcProvider
	log                      zerolog.Logger
	closed                   atomic.Bool
	ctx                      context.Context
	cancel                   context.CancelFunc

	nodeIsRunningGauge metrics.Gauge
	failedHealthChecks metrics.Counter
}

func NewNodeController(addr model.ServerAddress,
	shardAssignmentsProvider ShardAssignmentsProvider,
	nodeAvailabilityListener NodeAvailabilityListener,
	rpc RpcProvider) NodeController {

	labels := map[string]any{"node": addr.Internal}
	nc := &nodeController{
		addr:                     addr,
		shardAssignmentsProvider: shardAssignmentsProvider,
		nodeAvailabilityListener: nodeAvailabilityListener,
		rpc:                      rpc,
		status:                   Running,
		log: log.With().
			Str("component", "node-controller").
			Interface("addr", addr).
			Logger(),

		failedHealthChecks: metrics.NewCounter("oxia_coordinator_node_health_checks_failed",
			"The number of failed health checks to a node", "count", labels),
	}

	nc.ctx, nc.cancel = context.WithCancel(context.Background())

	nc.nodeIsRunningGauge = metrics.NewGauge("oxia_coordinator_node_running",
		"Whether the node is considered to be running by the coordinator", "count", labels, func() int64 {
			if nc.status == Running {
				return 1
			}
			return 0
		})

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller",
		"addr": nc.addr.Internal,
	}, nc.healthCheckWithRetries)

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller-send-updates",
		"addr": nc.addr.Internal,
	}, nc.sendAssignmentsUpdatesWithRetries)
	return nc
}

func (n *nodeController) Status() NodeStatus {
	n.Lock()
	defer n.Unlock()
	return n.status
}

func (n *nodeController) healthCheckWithRetries() {
	backOff := common.NewBackOffWithInitialInterval(n.ctx, 10*time.Second)
	_ = backoff.RetryNotify(func() error {
		return n.healthCheck(backOff)
	}, backOff, func(err error, duration time.Duration) {
		n.log.Warn().Err(err).
			Dur("retry-after", duration).
			Msg("Storage node health check failed")

		n.Lock()
		defer n.Unlock()
		if n.status == Running {
			n.status = NotRunning
			n.failedHealthChecks.Inc()
			n.nodeAvailabilityListener.NodeBecameUnavailable(n.addr)
		}
	})
}

func (n *nodeController) healthCheck(backoff backoff.BackOff) error {
	health, err := n.rpc.GetHealthClient(n.addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(n.ctx)
	defer cancel()

	go common.DoWithLabels(map[string]string{
		"oxia": "node-controller-health-check-ping",
		"addr": n.addr.Internal,
	}, func() {
		ticker := time.NewTicker(healthCheckProbeInterval)

		for {
			select {
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(ctx, healthCheckProbeTimeout)

				res, err := health.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
				pingCancel()
				if err2 := n.processHealthCheckResponse(res, err); err2 != nil {
					n.log.Warn().
						Msg("Node stopped responding to ping")
					cancel()
					return
				}

			case <-ctx.Done():
				return
			}
		}
	})

	watch, err := health.Watch(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		res, err := watch.Recv()

		if err2 := n.processHealthCheckResponse(res, err); err2 != nil {
			return err2
		}

		backoff.Reset()
	}

	return ctx.Err()
}

func (n *nodeController) processHealthCheckResponse(res *grpc_health_v1.HealthCheckResponse, err error) error {
	if err != nil {
		return err
	}

	if res.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return errors.New("node is not actively serving")
	}

	n.Lock()
	if n.status == NotRunning {
		n.log.Info().
			Msg("Storage node is back online")
	}
	n.status = Running
	n.Unlock()

	return nil
}

func (n *nodeController) sendAssignmentsUpdatesWithRetries() {
	backOff := common.NewBackOffWithInitialInterval(n.ctx, 10*time.Second)

	_ = backoff.RetryNotify(func() error {
		return n.sendAssignmentsUpdates(backOff)
	}, backOff, func(err error, duration time.Duration) {
		n.log.Warn().Err(err).
			Dur("retry-after", duration).
			Msg("Failed to send assignments updates to storage node")
	})
}

func (n *nodeController) sendAssignmentsUpdates(backoff backoff.BackOff) error {
	stream, err := n.rpc.GetShardAssignmentStream(n.ctx, n.addr)
	if err != nil {
		return err
	}

	var assignments *proto.ShardAssignmentsResponse
	for !n.closed.Load() {

		assignments, err = n.shardAssignmentsProvider.WaitForNextUpdate(stream.Context(), assignments)
		if err != nil {
			return err
		}

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

		backoff.Reset()
	}

	return nil
}

func (n *nodeController) Close() error {
	n.closed.Store(true)
	n.nodeIsRunningGauge.Unregister()
	n.cancel()
	return nil
}
