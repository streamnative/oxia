package sequence

import (
	"fmt"
	"testing"

	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/server"
	"github.com/stretchr/testify/assert"
)

func TestSequence_WithOtherKeyInBatch(t *testing.T) {
	config := server.NewTestConfig(t.TempDir())
	standaloneServer, err := server.NewStandalone(config)
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := oxia.NewAsyncClient(fmt.Sprintf("localhost:%d", standaloneServer.RpcPort()))
	assert.NoError(t, err)
	defer client.Close()

	for i := 1; i <= 3; i++ {
		if i == 2 {
			_ = client.Put("/abc", []byte("0"),
				oxia.PartitionKey("x"))
		}

		ch := client.Put("x", []byte("0"), oxia.PartitionKey("x"), oxia.SequenceKeysDeltas(1))
		pr := <-ch
		assert.NoError(t, pr.Err)
		assert.Equal(t, fmt.Sprintf("x-0000000000000000000%d", i), pr.Key)
	}
}
