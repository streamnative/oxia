# Deploying in the bare metal

Suppose you don't have the Kubernetes environment or some container-based system. You can try to deploy Oxia with the bare metal environment.

> The oxia coordinator only supports the file, k8s config map, and memory metadata. Therefore, We only suggest using it for testing or POC.

## Download code from the GitHub

You can check https://github.com/streamnative/oxia to download the source of StreamNative oxia.

## Build from the source code

Change the directory to the root directory of StreamNative oxia project.

```shell
cd <directory>/oxia
```

Then run the command as follows to build the project by source code.

```shell
make
```

After building, you'll see a bin directory at the project's root.

```shell
oxia/
├── bin/
│   ├── oxia
└── ...
```

## Deploying oxia storage node

We should deploy the oxia storage node first. We can start three storage nodes for primary usage. the command is as follows.

Make sure to pass differet wal and data dir paths for each storage node if running a local test.

```shell
# storage-node-0
./bin/oxia server -i 0.0.0.0:6649 -p 0.0.0.0:6648 -m 0.0.0.0:8080 --wal-dir "<wal-dir-path>" --data-dir "<data-dir-path>"
# storage-node-1
./bin/oxia server -i 0.0.0.0:6661 -p 0.0.0.0:6660 -m 0.0.0.0:8081 --wal-dir "<wal-dir-path>" --data-dir "<data-dir-path>"
# storage-node-2
./bin/oxia server -i 0.0.0.0:6663 -p 0.0.0.0:6662 -m 0.0.0.0:8082 --wal-dir "<wal-dir-path>" --data-dir "<data-dir-path>"
```

The command explanation is as follows:

```shell
Usage:
  oxia server [flags]

Flags:
      --data-dir string               Directory where to store data (default "./data/db")
      --db-cache-size-mb int          Max size of the shared DB cache (default 100)
  -h, --help                          help for server
  -i, --internal-addr string          Internal service bind address (default "0.0.0.0:6649")
  -m, --metrics-addr string           Metrics service bind address (default "0.0.0.0:8080")
  -p, --public-addr string            Public service bind address (default "0.0.0.0:6648")
      --wal-dir string                Directory for write-ahead-logs (default "./data/wal")
      --wal-retention-time duration   Retention time for the entries in the write-ahead-log (default 1h0m0s)

Global Flags:
  -j, --log-json                      Print logs in JSON format
  -l, --log-level string              Set logging level [disabled|trace|debug|info|warn|error|fatal|panic] (default "info")
      --profile                       Enable pprof profiler
      --profile-bind-address string   Bind address for pprof (default "127.0.0.1:6060")
```
## Deploying oxia coordinator

Since the coordinator is brain-like in the oxia cluster, it should have some configurations to help it to make decisions.

We can create it in the specific path you need. the configuration template `oxia_conf.yaml` is as follows. 

```yaml
namespaces:
  - name: default
    initialShardCount: 3  # This configuration indicates the number of shards for keys across the namespace.
    replicationFactor: 3  # This configuration indicates the number of replicas for of key-shard.
servers:
    # storage-node-0
  - public: 127.0.0.1:6648  
    internal: 127.0.0.1:6649
    # storage-node-1
  - public: 127.0.0.1:6660  
    internal: 127.0.0.1:6661
    # storage-node-2
  - public: 127.0.0.1:6662
    internal: 127.0.0.1:6663
```

> If you need to know what the namespaces are. You can check the [architecture](https://github.com/streamnative/oxia/blob/main/docs/architecture.md) section to get more information.

After configuration file creation, we can start the coordinator. The command is as follows.

```shell
./bin/oxia coordinator --conf "<conf-file>" --file-clusters-status-path "<cluster-status-file-path>" --metadata file  -i 0.0.0.0:6664 -m 0.0.0.0:8083 
```

The command explanation is as follows:

```shell
Usage:
  oxia coordinator [flags]

Flags:
  -f, --conf string                        Cluster config file
      --conf-file-refresh-time duration    How frequently to check for updates for cluster configuration file (default 1m0s)
      --file-clusters-status-path string   The path where the cluster status is stored when using 'file' provider (default "data/cluster-status.json")
  -h, --help                               help for coordinator
  -i, --internal-addr string               Internal service bind address (default "0.0.0.0:6649")
      --k8s-configmap-name string          ConfigMap name for metadata configmap
      --k8s-namespace string               Kubernetes namespace for metadata configmap
      --metadata MetadataProviderImpl      Metadata provider implementation: file, configmap or memory (default file)
  -m, --metrics-addr string                Metrics service bind address (default "0.0.0.0:8080")

Global Flags:
  -j, --log-json                      Print logs in JSON format
  -l, --log-level string              Set logging level [disabled|trace|debug|info|warn|error|fatal|panic] (default "info")
      --profile                       Enable pprof profiler
      --profile-bind-address string   Bind address for pprof (default "127.0.0.1:6060")
```

## Go for testing

After all of the components are up and running without an error log. We can use oxia-perf to test. the command is as follows.

```shell
./bin/oxia perf --rate 10000
```


Well done, please enjoy your powerful oxia cluster.
