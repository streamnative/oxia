package server

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"os/exec"
	"oxia/common"
	"oxia/oxia"
	"oxia/perf"
	"runtime/pprof"
	"testing"
	"time"
)

func BenchmarkServer(b *testing.B) {
	common.LogLevel = zerolog.InfoLevel
	common.ConfigureLogger()

	tmp := b.TempDir()

	standaloneConf := StandaloneConfig{
		NumShards: 1,
		Config: Config{
			InternalServiceAddr: "localhost:0",
			PublicServiceAddr:   "localhost:0",
			MetricsServiceAddr:  "localhost:0",
			DataDir:             fmt.Sprintf("%s/db", tmp),
			WalDir:              fmt.Sprintf("%s/wal", tmp),
		},
	}

	standalone, err := NewStandalone(standaloneConf)
	if err != nil {
		b.Fatal(err)
	}

	perfConf := perf.Config{
		ServiceAddr:         fmt.Sprintf("localhost:%d", standalone.RpcPort()),
		RequestRate:         200_000,
		ReadPercentage:      80,
		KeysCardinality:     10_000,
		ValueSize:           1_024,
		BatchLinger:         oxia.DefaultBatchLinger,
		MaxRequestsPerBatch: oxia.DefaultMaxRequestsPerBatch,
		RequestTimeout:      oxia.DefaultRequestTimeout,
	}

	profile := "profile-zstd"
	file, err := os.Create(profile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()

	err = pprof.StartCPUProfile(file)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Minute))
	defer cancel()

	perf.New(perfConf).Run(ctx)

	pprof.StopCPUProfile()

	err = standalone.Close()
	if err != nil {
		b.Fatal(err)
	}

	var out []byte
	out, err = exec.Command("go", "tool", "pprof", "-http=:", profile).CombinedOutput()
	if err != nil {
		b.Fatal(string(out), err)
	}
	fmt.Println(string(out))
}
