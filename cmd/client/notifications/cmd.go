package notifications

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"oxia/cmd/client/common"
)

var Cmd = &cobra.Command{
	Use:   "notifications",
	Short: "Get notifications stream",
	Long:  `Follow the change notifications stream`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	defer client.Close()

	notifications, err := client.GetNotifications()
	if err != nil {
		return err
	}

	defer notifications.Close()

	for notification := range notifications.Ch() {
		log.Info().
			Stringer("type", notification.Type).
			Str("key", notification.Key).
			Int64("version", notification.Version).
			Msg("")
	}

	return nil
}
