# Getting started with Oxia

## Obtaining Oxia

### Docker image

Oxia is available in form of a Docker image

```shell
$ docker pull streamnative/oxia:main
```

### Building from source

```shell
$ make
```

This will create the binary file on `bin/oxia`.

## Oxia standalone

Oxia standalone is a single-node single-process instance of Oxia that can started with little or no configuration.
It provides a full-fledged Oxia service with a `default` namespace. Being a single-node instance, the data is not
getting replicated.

```shell
$ oxia standalone
```

Output will be something like:

```
Mar 30 16:51:40.519927 INF Starting Oxia standalone config={"DataDir":"./data/db","InMemory":false,"InternalServiceAddr":"","MetricsServiceAddr":"0.0.0.0:8080","NotificationsRetentionTime":3600000000000,"NumShards":1,"PublicServiceAddr":"0.0.0.0:6648","WalDir":"./data/wal","WalRetentionTime":3600000000000}
Mar 30 16:51:40.553559 INF Created leader controller component=leader-controller namespace=default shard=0 term=-1
Mar 30 16:51:40.566230 INF Leader successfully initialized in new term component=leader-controller last-entry={"offset":"-1","term":"-1"} namespace=default shard=0 term=0
Mar 30 16:51:40.566372 INF Applying all pending entries to database commit-offset=-1 component=leader-controller head-offset=-1 namespace=default shard=0 term=0
Mar 30 16:51:40.566389 INF Started leading the shard component=leader-controller head-offset=-1 namespace=default shard=0 term=0
Mar 30 16:51:40.566701 INF Started Grpc server bindAddress=[::]:6648 grpc-server=public
Mar 30 16:51:40.566760 INF Serving Prometheus metrics at http://localhost:8080/metrics
```

The service is now ready at `localhost:6648` address.

Using docker this can be done with:

```shell
$ docker run -p 6648:6648 streamnative/oxia:main oxia standalone
```

## Interact with Oxia by CLI client

There is a convenient CLI tool that allows you to interact with the records stored in Oxia.

```shell
# Write or update a record
$ bin/oxia client put -k /my-key -v my-value
{"version":{"version_id":0,"created_timestamp":1680220430128,"modified_timestamp":1680220430128,"modifications_count":0}}


# Read the value of a key
$ oxia client get -k /my-key
{"binary":false,"value":"my-value","version":{"version_id":0,"created_timestamp":1680220430128,"modified_timestamp":1680220430128,"modifications_count":0}}
```

## Interact with Oxia by Go client

Instead, you can write a Go application with [Oxia Go API](go-api.md).

## Using perf client

If you want to do a quick assessment of the capacity of an Oxia cluster, you can use the provided `perf` tool, which
generates some amount of traffic, based on the desired rate and read/write ratio.


```shell
$ oxia perf --rate 10000
Mar 30 16:57:25.507167 INF Starting Oxia perf client config={"BatchLinger":5000000,"KeysCardinality":1000,"MaxRequestsPerBatch":1000,"ReadPercentage":80,"RequestRate":10000,"RequestTimeout":30000000000,"ServiceAddr":"localhost:6648","ValueSize":128}
Mar 30 16:57:35.510046 INF Stats - Total ops: 10994.5 ops/s - Failed ops:    0.0 ops/s
			Write ops 2198.4 w/s  Latency ms: 50%   6.4 - 95%  10.9 - 99%  18.9 - 99.9%  18.9 - max   18.9
			Read  ops 8796.1 r/s  Latency ms: 50%   3.2 - 95%   5.5 - 99%  12.1 - 99.9%  12.1 - max   12.1
```
