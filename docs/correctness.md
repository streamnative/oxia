
# Verifying Oxia correctness

Given that Oxia is a critical component of large scales systems, it is of great importance to verify that 
the protocols it uses and their implementation are correct and resilient to all sort of failures scenarios.

We apply 3 methods to validate Oxia:

 1. TLA+ model
 2. Maelstrom / Jepsen test
 3. ChaosMesh

## TLA+ model

[TLA+](https://lamport.azurewebsites.net/tla/tla.html) is a high-level language for modeling distributed and 
concurrent systems.

The Oxia TLA model [OxiaReplication.tla](../tlaplus/OxiaReplication.tla), contains a model of data replication for a
single shard.

The TLA+ tools are able to use this model and explore all the possible states, verifying that the system properties are
not violated (eg: missing entries in the log).

You can run the TLA+ tools by doing:

```shell
$ make tla
```

It will download all the required TLA+ tools and run the validation.


## Maelstrom

[Maelstrom](https://github.com/jepsen-io/maelstrom) is a tool that makes it easy to run a [Jepsen](https://jepsen.io/)
simulation to verify the correctness of a system.

Unlike for TLA+, Maelstrom works by running the real production code, injecting different kinds of failures and verifying
that the external properties are not violated, using the Jepsen library.

For Oxia this means that we can run a multi-node Oxia cluster as a set of multiple processes running in a single physical
machine. Instead of TCP networking through gRPC, we run Oxia nodes that use stdin/stdout to communicate, using the 
JSON based [Maelstrom protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md).

To build Oxia for Maelstrom: 

```shell
$ make maelstrom
```

This will produce the binary in `bin/oxia-maelstrom`

To run the Maelstrom simulation, you need to download Maelstrom and then run:

```shell
$ ./maelstrom test -w lin-kv  --bin /path/to/oxia-maelstrom \
      --time-limit 30  --concurrency 2n  --latency 10 --latency-dist uniform
```


## Chaos Mesh

[Chaos Mesh](https://chaos-mesh.org/) is a tools that helps to define a testing plan and generate different classes
of failure and validate that the tested system can handle them appropriately.

### Run chaos-mesh in Kind

It is easy to run a Chaos Mesh test for Oxia using [Kind](https://kind.sigs.k8s.io/), a K8S distribution that works
on a single machine.

The same approach will work in any K8S environment

#### Create Kind Kubernetes cluster

```shell
$ kind create cluster
```

#### Install chaos-mesh

```shell
$ kubectl create namespace chaos-mesh

$ helm repo add chaos-mesh https://charts.chaos-mesh.org
$ helm upgrade --install chaos-mesh \
               --namespace chaos-mesh \
               --version 2.5.1 \
               chaos-mesh/chaos-mesh
```

#### Load Oxia image

```shell
$ docker pull streamnative/oxia:main
$ kind load docker-image streamnative/oxia:main
```

#### Create Oxia cluster

```shell
$ kubectl create namespace oxia

$ helm upgrade --install oxia \
  --namespace oxia \
  --set image.repository=oxia \
  --set image.tag=latest \
  --set image.pullPolicy=Never \
  deploy/charts/oxia-cluster
```

#### Run perf

```shell
kubectl run perf \
  --namespace oxia \
  --image oxia:latest \
  --image-pull-policy Never \
  --command -- oxia perf --service-address=oxia:6648
```

#### Start the chaos experiment

```shell
$ kubectl apply --namespace oxia -f deploy/chaos-mesh/chaos-mesh.yaml 
```
