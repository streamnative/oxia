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

type notificationsManager struct {
	multiplexCh  chan Notification
	closeCh      chan any
	shardManager internal.ShardManager
	clientPool   common.ClientPool
}

func newNotificationsManager(ctx context.Context, clientPool common.ClientPool, shardManager internal.ShardManager) *notificationsManager {
	nm := &notificationsManager{
		multiplexCh:  make(chan Notification, 100),
		closeCh:      make(chan any),
		shardManager: shardManager,
		clientPool:   clientPool,
	}

	// Create a notification manager for each shard
	shards := shardManager.GetAll()
	for _, shard := range shards {
		newShardNotificationsManager(shard, ctx, nm)
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

	return nm
}

func (nm *notificationsManager) Ch() <-chan Notification {
	return nm.multiplexCh
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Manages the notifications for a specific shard
type shardNotificationsManager struct {
	shard              uint32
	ctx                context.Context
	nm                 *notificationsManager
	backoff            backoff.BackOff
	lastOffsetReceived int64
	log                zerolog.Logger
}

func newShardNotificationsManager(shard uint32, ctx context.Context, nm *notificationsManager) *shardNotificationsManager {
	snm := &shardNotificationsManager{
		shard:              shard,
		ctx:                ctx,
		nm:                 nm,
		lastOffsetReceived: -1,
		backoff:            common.NewBackOffWithInitialInterval(ctx, 1*time.Second),
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

		for key, n := range nb.Notifications {
			select {
			case snm.nm.multiplexCh <- convertNotification(key, n):

			// Unblock from channel write when we're closing down
			case <-snm.ctx.Done():
				snm.nm.closeCh <- nil
				return snm.ctx.Err()
			}
		}
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

func convertNotification(key string, n *proto.Notification) Notification {
	version := int64(-1)
	if n.Version != nil {
		version = *n.Version
	}
	return Notification{
		Type:    convertNotificationType(n.Type),
		Key:     key,
		Version: version,
	}
}
