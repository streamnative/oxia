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

package controllers

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/streamnative/oxia/common/metric"
	"github.com/streamnative/oxia/common/process"
	time2 "github.com/streamnative/oxia/common/time"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/rpc"
	"github.com/streamnative/oxia/proto"
)

type NodeStatus uint32

const (
	Running NodeStatus = iota
	NotRunning
	Draining //
)

const (
	healthCheckProbeInterval   = 2 * time.Second
	healthCheckProbeTimeout    = 2 * time.Second
	defaultInitialRetryBackoff = 10 * time.Second
)

type ShardAssignmentsProvider interface {
	WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error)
}

// The NodeController takes care of checking the health-status of each node
// and to push all the service discovery updates.
type NodeController interface {
	io.Closer

	Status() NodeStatus

	SetStatus(status NodeStatus)
}

type nodeController struct {
	sync.Mutex
	server                   model.Server
	status                   NodeStatus
	shardAssignmentsProvider ShardAssignmentsProvider
	nodeAvailabilityListener NodeEventListener
	rpc                      rpc.Provider
	log                      *slog.Logger

	// This is the overall context for the node controller
	// it gets created once and canceled when the controller is closed
	ctx    context.Context
	cancel context.CancelFunc

	// This context is re-created each time we have a health-check failure
	healthCheckCtx    context.Context
	healthCheckCancel context.CancelFunc

	initialRetryBackoff time.Duration

	nodeIsRunningGauge metric.Gauge
	failedHealthChecks metric.Counter
}

func NewNodeController(addr model.Server,
	shardAssignmentsProvider ShardAssignmentsProvider,
	nodeAvailabilityListener NodeEventListener,
	rpcProvider rpc.Provider) NodeController {
	return newNodeController(addr, shardAssignmentsProvider, nodeAvailabilityListener, rpcProvider, defaultInitialRetryBackoff)
}

func newNodeController(server model.Server,
	shardAssignmentsProvider ShardAssignmentsProvider,
	nodeAvailabilityListener NodeEventListener,
	rpcProvider rpc.Provider,
	initialRetryBackoff time.Duration) NodeController {
	labels := map[string]any{"node": server.GetIdentifier()}
	nc := &nodeController{
		server:                   server,
		shardAssignmentsProvider: shardAssignmentsProvider,
		nodeAvailabilityListener: nodeAvailabilityListener,
		rpc:                      rpcProvider,
		status:                   Running,
		log: slog.With(
			slog.String("component", "node-controller"),
			slog.Any("server", server),
		),
		initialRetryBackoff: initialRetryBackoff,

		failedHealthChecks: metric.NewCounter("oxia_coordinator_node_health_checks_failed",
			"The number of failed health checks to a node", "count", labels),
	}

	nc.ctx, nc.cancel = context.WithCancel(context.Background())
	nc.healthCheckCtx, nc.healthCheckCancel = context.WithCancel(nc.ctx)

	nc.nodeIsRunningGauge = metric.NewGauge("oxia_coordinator_node_running",
		"Whether the node is considered to be running by the coordinator", "count", labels, func() int64 {
			if nc.status == Running {
				return 1
			}
			return 0
		})

	go process.DoWithLabels(
		nc.ctx,
		map[string]string{
			"oxia":   "node-controller",
			"server": nc.server.GetIdentifier(),
		},
		nc.healthCheckWithRetries,
	)

	go process.DoWithLabels(
		nc.ctx,
		map[string]string{
			"oxia":   "node-controller-send-updates",
			"server": nc.server.GetIdentifier(),
		},
		nc.sendAssignmentsUpdatesWithRetries,
	)

	nc.log.Info("Started node controller")
	return nc
}

func (n *nodeController) Status() NodeStatus {
	n.Lock()
	defer n.Unlock()
	return n.status
}

func (n *nodeController) SetStatus(status NodeStatus) {
	n.Lock()
	defer n.Unlock()
	n.status = status
	n.log.Info("Changed status", slog.Any("status", status))
}

func (n *nodeController) healthCheckWithRetries() {
	backOff := time2.NewBackOffWithInitialInterval(n.ctx, n.initialRetryBackoff)
	_ = backoff.RetryNotify(func() error {
		return n.healthCheck(backOff)
	}, backOff, func(err error, duration time.Duration) {
		if n.Status() == Draining {
			// Stop the health check and close
			_ = n.Close()
			n.nodeAvailabilityListener.NodeBecameUnavailable(n.server)
			return
		}

		n.log.Warn(
			"Storage node health check failed",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)

		n.Lock()
		defer n.Unlock()
		if n.status == Running {
			n.status = NotRunning
			n.failedHealthChecks.Inc()
			n.nodeAvailabilityListener.NodeBecameUnavailable(n.server)
		}
	})
}

func (n *nodeController) healthCheckLoop(ctx context.Context, health grpc_health_v1.HealthClient) {
	ticker := time.NewTicker(healthCheckProbeInterval)

	for {
		select {
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, healthCheckProbeTimeout)

			res, err := health.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
			pingCancel()
			if err2 := n.processHealthCheckResponse(res, err); err2 != nil {
				n.log.Warn("Node stopped responding to ping")
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (n *nodeController) healthCheck(backoff backoff.BackOff) error {
	n.log.Debug("Start new health check cycle")
	health, closer, err := n.rpc.GetHealthClient(n.server)
	if err != nil {
		n.log.Debug("Failed to get health check client", slog.Any("error", err))
		return err
	}

	defer closer.Close()

	ctx, cancel := context.WithCancel(n.ctx)
	defer cancel()

	go process.DoWithLabels(
		ctx,
		map[string]string{
			"oxia":   "node-controller-health-check-ping",
			"server": n.server.GetIdentifier(),
		},
		func() {
			defer cancel()
			n.healthCheckLoop(ctx, health)
		},
	)

	watch, err := health.Watch(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		res, err := watch.Recv()

		if err2 := n.processHealthCheckResponse(res, err); err2 != nil {
			n.log.Warn("Node watcher check failed", slog.Any("error", err2))
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
		n.log.Info("Storage node is back online")

		// To avoid the send assignments stream to miss the notification about the current
		// node went down, we interrupt the current stream when the ping on the node fails
		n.rpc.ClearPooledConnections(n.server)
		n.healthCheckCancel()
		n.log.Debug("Cancelled the send assignments stream")
		n.healthCheckCtx, n.healthCheckCancel = context.WithCancel(n.ctx)
	}

	n.status = Running
	n.Unlock()

	return nil
}

func (n *nodeController) sendAssignmentsUpdatesWithRetries() {
	backOff := time2.NewBackOffWithInitialInterval(n.ctx, n.initialRetryBackoff)

	_ = backoff.RetryNotify(func() error {
		return n.sendAssignmentsUpdates(backOff)
	}, backOff, func(err error, duration time.Duration) {
		if n.Status() == Draining {
			// Stop the health check and close
			_ = n.Close()
			n.nodeAvailabilityListener.NodeBecameUnavailable(n.server)
			return
		}

		n.log.Warn(
			"Failed to send assignments updates to storage node",
			slog.Duration("retry-after", duration),
			slog.Any("error", err),
		)
	})
}

func (n *nodeController) sendAssignmentsUpdateOnce(
	stream proto.OxiaCoordination_PushShardAssignmentsClient,
	assignments *proto.ShardAssignments,
) (*proto.ShardAssignments, error) {
	n.log.Debug(
		"Waiting for next assignments update",
		slog.Any("current-assignments", assignments),
	)
	assignments, err := n.shardAssignmentsProvider.WaitForNextUpdate(stream.Context(), assignments)
	if err != nil {
		return nil, err
	}

	if assignments == nil {
		n.log.Debug("Assignments are nil")
		return assignments, nil
	}

	n.log.Debug(
		"Sending assignments",
		slog.Any("assignments", assignments),
	)

	if err := stream.Send(assignments); err != nil {
		n.log.Debug(
			"Failed to send assignments",
			slog.Any("error", err),
		)
		return nil, err
	}

	n.log.Debug("Send assignments completed successfully")
	return assignments, nil
}

func (n *nodeController) sendAssignmentsUpdates(backoff backoff.BackOff) error {
	n.log.Debug("Ready to send assignments")
	n.Lock()
	ctx := n.healthCheckCtx
	n.Unlock()

	stream, err := n.rpc.PushShardAssignments(ctx, n.server)
	if err != nil {
		n.log.Debug("Failed to create shard assignments stream", slog.Any("error", err))
		return err
	}

	var assignments *proto.ShardAssignments

	for {
		select {
		case <-n.ctx.Done():
			return nil

		default:
			assignments, err = n.sendAssignmentsUpdateOnce(stream, assignments)
			if err != nil {
				n.log.Debug("Failed to send assignments", slog.Any("error", err))
				return err
			}

			n.log.Info("Sent new shareds assignments.", slog.Any("assignments", assignments))
			backoff.Reset()
		}
	}
}

func (n *nodeController) Close() error {
	n.nodeIsRunningGauge.Unregister()
	n.cancel()

	n.log.Info("Closed node controller")
	return nil
}
