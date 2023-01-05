package oxia

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"oxia/common"
	"oxia/oxia/internal"
	"oxia/proto"
	"time"
)

type notifications struct {
	multiplexCh  chan *Notification
	closeCh      chan any
	shardManager internal.ShardManager
	clientPool   common.ClientPool

	initialized chan error
	ctx         context.Context
	cancel      context.CancelFunc
}

func newNotifications(options clientOptions, ctx context.Context, clientPool common.ClientPool, shardManager internal.ShardManager) (*notifications, error) {
	nm := &notifications{
		multiplexCh:  make(chan *Notification, 100),
		closeCh:      make(chan any),
		shardManager: shardManager,
		clientPool:   clientPool,
		initialized:  make(chan error),
	}

	nm.ctx, nm.cancel = context.WithCancel(ctx)

	// Create a notification manager for each shard
	shards := shardManager.GetAll()
	for _, shard := range shards {
		newShardNotificationsManager(shard, nm)
	}

	go common.DoWithLabels(map[string]string{
		"oxia": "notifications-manager-close",
	}, func() {
		// Wait until all the shards managers are done before
		// closing the user-facing channel
		for i := 0; i < len(shards); i++ {
			<-nm.closeCh
		}

		close(nm.multiplexCh)
	})

	// Wait for the notifications on all the shards to be initialized
	timeoutCtx, cancel := context.WithTimeout(nm.ctx, options.requestTimeout)
	defer cancel()

	for i := 0; i < len(shards); i++ {
		select {
		case err := <-nm.initialized:
			if err != nil {
				return nil, err
			}

		case <-timeoutCtx.Done():
			return nil, timeoutCtx.Err()
		}
	}

	return nm, nil
}

func (nm *notifications) Ch() <-chan *Notification {
	return nm.multiplexCh
}

func (nm *notifications) Close() error {
	nm.cancel()
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Manages the notifications for a specific shard
type shardNotificationsManager struct {
	shard              uint32
	ctx                context.Context
	nm                 *notifications
	backoff            backoff.BackOff
	lastOffsetReceived int64
	initialized        bool
	log                zerolog.Logger
}

func newShardNotificationsManager(shard uint32, nm *notifications) *shardNotificationsManager {
	snm := &shardNotificationsManager{
		shard:              shard,
		ctx:                nm.ctx,
		nm:                 nm,
		lastOffsetReceived: -1,
		backoff:            common.NewBackOffWithInitialInterval(nm.ctx, 1*time.Second),
		log: log.Logger.With().
			Str("component", "oxia-notifications-manager").
			Uint32("shard", shard).
			Logger(),
	}

	go common.DoWithLabels(map[string]string{
		"oxia":  "notifications-manager",
		"shard": fmt.Sprintf("%d", shard),
	}, snm.getNotificationsWithRetries)

	return snm
}

func (snm *shardNotificationsManager) getNotificationsWithRetries() {
	_ = backoff.RetryNotify(snm.getNotifications,
		snm.backoff, func(err error, duration time.Duration) {
			if err != context.Canceled {
				snm.log.Error().Err(err).
					Dur("retry-after", duration).
					Msg("Error while getting notifications")
			}

			if !snm.initialized {
				snm.initialized = true
				snm.nm.initialized <- err
				snm.nm.cancel()
			}
		})
}

func (snm *shardNotificationsManager) getNotifications() error {
	leader := snm.nm.shardManager.Leader(snm.shard)

	rpc, err := snm.nm.clientPool.GetClientRpc(leader)
	if err != nil {
		return err
	}

	var startOffsetExclusive *int64
	if snm.lastOffsetReceived >= 0 {
		startOffsetExclusive = &snm.lastOffsetReceived
	}

	notifications, err := rpc.GetNotifications(snm.ctx, &proto.NotificationsRequest{
		ShardId:              snm.shard,
		StartOffsetExclusive: startOffsetExclusive,
	})
	if err != nil {
		if snm.ctx.Err() != nil {
			snm.nm.closeCh <- nil
			return snm.ctx.Err()
		}
		return err
	}

	snm.backoff.Reset()

	for {
		nb, err := notifications.Recv()
		if err != nil {
			if snm.ctx.Err() != nil {
				snm.nm.closeCh <- nil
			}
			return err
		} else if nb == nil {
			if snm.ctx.Err() != nil {
				snm.nm.closeCh <- nil
				return snm.ctx.Err()
			}
			return io.EOF
		}

		snm.log.Debug().
			Int64("offset", nb.Offset).
			Int("count", len(nb.Notifications)).
			Msg("Received batch notification")

		if !snm.initialized {
			snm.log.Debug().Msg("Initialized the notification manager")

			// We need to discard the very first notification, because it's only
			// needed to ensure that the notification cursor is created on the
			// server side.
			snm.initialized = true
			snm.nm.initialized <- nil
			snm.lastOffsetReceived = nb.Offset
			continue
		}

		for key, n := range nb.Notifications {
			select {
			case snm.nm.multiplexCh <- convertNotification(key, n):

			// Unblock from channel write when we're closing down
			case <-snm.ctx.Done():
				snm.nm.closeCh <- nil
				return snm.ctx.Err()
			}
		}

		snm.lastOffsetReceived = nb.Offset
	}
}

func convertNotificationType(t proto.NotificationType) NotificationType {
	switch t {
	case proto.NotificationType_KeyCreated:
		return KeyCreated
	case proto.NotificationType_KeyModified:
		return KeyModified
	case proto.NotificationType_KeyDeleted:
		return KeyDeleted
	default:
		panic("Invalid notification type")
	}
}

func convertNotification(key string, n *proto.Notification) *Notification {
	version := int64(-1)
	if n.Version != nil {
		version = *n.Version
	}
	return &Notification{
		Type:    convertNotificationType(n.Type),
		Key:     key,
		Version: version,
	}
}
