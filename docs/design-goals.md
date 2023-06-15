
# Oxia design goals

Oxia has been designed to be system suitable for storing metadata and coordinating a large cluster of nodes.

In particular, the main motivation for Oxia has been to address issues in the systems currently used in this space, 
such as Apache ZooKeeper, Etcd and Consul.

 1. Improve system scalability by orders of magnitude
 2. Modern architecture to provide better ops experience
 3. Provide developers with better coordination primitives for distributed coordination

## Scalability

### Horizontal Scalability

Oxia achieves horizontal scalability by sharding the dataset.  Currently, Oxia supports hash-based sharding, though it's
not excluded that more types of routing will be added later on, such as sharding based on lexicographically sorting of
the keys.

The sharding is managed by the client library and it's completely transparent to the applications.

Initially the number of shards for a given namespace is fixed, though automatic shards splitting is already on the
roadmap.

### Storage Scalability

Systems like ZooKeeper and Etcd have limitation around ~2GB (See
[Etcd docs](https://etcd.io/docs/v3.5/dev-guide/limit/#storage-size-limit)), mainly because they are taking periodic snapshots of the entire data set.

Oxia is designed to efficiently store amount of metadata larger than the available RAM, in the order of 100s of GB, 
across multiple shards. The primary reason is that there is no complete full snapshot that has to be taken periodically.

Instead, Oxia relies on an LSM KV store ([Pebble](https://github.com/cockroachdb/pebble)) which allows to read through
a consistent snapshot of the data, without requiring to dump the full data set every N updates.

## Operational tooling

### Oxia Cluster Helm chart

Oxia is designed to work natively in a Kubernetes environment. There is an [Oxia Helm Chart](ks8-deploy.md) provided
by default as the canonical way to deploy.

### Namespaces

A single Oxia cluster can serve multiple use cases and these can be isolated by using different namespaces. 

Each namespace has its own configuration and its own set of shards.

## Replication protocol

Unlike other systems, Oxia does not implement Paxos, Raft or any derived [consensus protocols](https://en.wikipedia.org/wiki/Consensus_(computer_science)).

Instead, it uses a different approach to data replication: 

 * Non-fault tolerant log replication
 * Apply recovery protocol when the shard's leader fails

This is a similar approach as what already used by [Apache BookKeeper](https://bookkeeper.apache.org/docs/development/protocol)
The advantages of this model are:

 1. It becomes much easier to implement highly performant log-replication (compared to implement full Paxos/Raft)
 2. Separates the data-path (which needs to be optimized for speed), from the recovery path (which needs to be
    optimized for readability and correctness)
 3. Offloads the cluster-status checkpointing (which is a tiny amount of data) to an external source of consensus 

