# Context

In a Pulsar cluster, the handling of metadata is today the biggest obstacle in order to achieve a 10x increase in the number of topics.

There are other factors as well, metrics, lookups, overhead of single topic, global memory limits, though this is probably the hardest one.

There are 2 main ways in which we are using a metadata service today:

1. Storing pointers to data 
1. Coordination

# Goals

- Design expressively making assumption of a Kubernetes environment
- Transparent horizontal scalability
- Incremental snapshotting
- Ease of operations
    - Seamlessly add/remove pods
- Exposing better primitives for cluster coordination
    - Leader election, locks
- No need for global linearizable history
- Scale up to 100 GB of total data set
- Read - Write rates scalable to ~1M ops/s
- Latency target: reads 99pct &lt; 5ms — writes 99pct &lt; 20ms

# High level design

Instead of implementing a consensus algorithm for data replication, use instead a non-fault tolerant log-replication mechanism, coupled with an operator component which will handle the failure recover process.

The operator controls the state of the cluster, and it moves the cluster from one state to the next.

Example of this state transitions are:

- A pod is unhealthy → Changing assignments
- Keep track of last committed entry for a given shard 

Storage nodes are only tasked of performing log replication, without any coordination or error recovery.

The combination of storage-pods + the operator will enable to have a strongly-consistent replicated log.

## Advantages of this approach

Separate the control path from the data path components. Each of them can be developed independently, possibly even in different languages.

This makes the data replication path much easier to implement and, in particular, much easier to optimize since we&#39;re only considering a steady state with no failure detection or recovery.

Only the data path needs to be highly optimized for performance. The control path in the operator can be written focusing on the simplicity and readability of the code.



## Physical View

![](https://static.slab.com/prod/uploads/cam7h8fn/posts/images/0ey11iCHtDtP_EtEU_l4GTdN.png)

## Components

- **Client** → Fetches the list of shards assignments so that it will be able to connect to the right storage pod, the one that is leading a particular shard
- **Storage pod** → Shards are assigned to storage pods, which can be either &quot;leader&quot; or &quot;follower&quot; on them. Storage pod _do not_ perform health/aliveness checks against each others.
- **K8S Operator** → Operator is in charge of 2 main tasks:
    - Perform error detection+recovery
    - Keep track on the shards status
- **CRD** → This is a regular K8S custom resource definition which is manipulated through K8S APIs and it is backed by the underlying K8S etcd instance. The CRD has 2 purposes: 
    - It is the way for an administrator to declare the desired configuration for cluster, and updating it. The K8S operator will read updates to the CRD and will execute them on the cluster.
    - The operator will use a &quot;_status_&quot; in the CRD to store cluster status information, like:
        -  Assignments of shards to storage pods
        - Shard epochs and last entries
- **Administrator** → The administrator will be able to interact with the Operator by updating the CRD. Examples:
    - Change the number of storage pods
    - Change the replication factor

## Cluster state

Operator will react to changes in the cluster, like:

- Pod failing
- New pods available

by updating the `ClusterState`, storing it as part of the Kubernetes CRD status. Essentially the new status is written in Etcd.

```java
message ClusterStatus {
  repeated ShardStatus shards_status = 1;

  uint32 ack_quorum = 2;
}

message ShardStatus {
  uint32 shard = 1;
  ServerAddress leader = 2;
  repeated ServerAddress followers = 3;

  repeated EpochStatus epochs = 4;
}

message EpochStatus {
  uint64 epoch = 1;
  uint64 first_entry = 2;
}

message ServerAddress {
  string internal_url = 1;
  string public_url = 2;
}
```

After the cluster state is updated, the operator will propagate it back to every server pod.

## Client interactions

Clients interact with storage nodes through a Grpc interface.

```java
service ClientAPI {
  /**
   * Return the current shards -> server mapping and all the subsequent updates
   */
  rpc GetShardsAssignments(Empty) returns (stream ShardsAssignments) {}

  rpc Put(PutOp) returns (Stat) {}
  rpc Get(GetOp) returns (Entry) {}
  /* .... */
}

```

The first step is to subscribe to a feed of &quot;shard-assignment&quot; changes, so that the client is aware of the up-to-date state of the cluster.

After that, clients will make Grpc request directly to the node where the shard&#39;s leader is running.

### Receiving change data notifications

Client can subscribe to receive either all or a filtered set of notifications for the put/delete operations happening on a given shard.

```java
rpc GetNotifications(GetNotificationOp) returns (stream Notification) {}
```

Where the operation is defined as:

```java
message GetNotificationOp {
  uint32 shard_id = 1;
  uint64 first_entry_id = 2;
  string key_prefix = 3;
  bool include_stat = 4;
}

enum OperationType {
  INVALID = 0;
  PUT = 1;
  DELETE = 2;
}

message Notification {
  uint64 entry_id = 1;
  string key = 2;
  OperationType operation_type = 3;
  Stat stat = 4;
}
```

# Data replication

The operator will assign a given shard to 1 node as the &quot;leader&quot; and 0+ nodes as &quot;followers&quot;.

The cluster configuration will have a way to specify the desired replication factor for the data, and to adjust it dynamically.

The leader will wait until a majority of the storage pods has persisted a given entry before applying it to the local KV store.

![](https://static.slab.com/prod/uploads/cam7h8fn/posts/images/lcxo7ZQHBZP80v1z7f_NA5Tt.png)

## Leader

A leader will accept put/get/delete request for the shard it has been assigned.

Any read operation will just read from the underlying key-value storage library (eg: RocksDB or similar).

Any write operation will be appended to the local WAL. Once enough followers, sending back `ConfirmedEntryRequest` are including a given entry in the WAL, then the leader will be able to:

1. Advance the LastAddConfirmed entry id
1. Apply the update to the local KV store
1. Acknowledge the put/delete operation to the client
1. Send notification event to subscribed clients

## Follower

Followers are meant to be very close the leader, tailing the leader WAL and applying all the changes, up to the LastAddConfirmed (same concept as in BookKeeper), which represents the latest entry that we know for sure that it was fully committed.

This allows the followers, when required, to be promoted to leader in a very short amount of time.

A follower connects to the leader and opens up a 2 ways stream:

```java
/**
 * Used by followers to attach to the leader. This is a bi-directional stream:
 *  - Leader will send log entries to the follower
 *  - Follower send a stream of updates with the last entry persisted
 */
rpc Follow(stream ConfirmedEntryRequest) returns (stream LogEntry) {}
```

On one hand, the leader sends a stream of `LogEntry`, which contains the new entries added to the leader WAL.

On the other, the follower keeps communicating to the leader what was the last entry id that was committed on the follower WAL.

When starting, the follower will also pass the first transaction it wants to receive. If this transaction was already truncated in the leader, the leader will ship a snapshot of the data set, to ensure the follower can be fully rebuilt.

## Error handling

### Follower failure

If the operator detects that a follower is down, it can simply swap it with a different follower, but leaving the leader untouched. The new follower will sync-up with the leader or will get bootstrapped and fetch a snapshot.

### Leader failure

If the leader of a shard fails, the operator is responsible to ensure the correctness of the system, by doing:

1. Fence the current &quot;epoch&quot; for the shard, on all the followers. After that, no more updates are allowed on the shard for the given epoch

```java
message FenceRequest {
  uint32 shard = 1;
  uint64 epoch = 2;
}
```

1. Each of the followers, will apply the fencing and respond with their latest appended entry for the current epoch.
1. The operator will decide which one is the last fully committed entry in the current epoch by following these steps:
    1. Select the node with the highest last committed entry
    1. Read entries from this node and write them to the other followers which have a lower last committed entry
    1. Use the highest last committed entry as the last entry for the epoch
1. The operator will then start a new epoch and select a new leader, based on the overall shards assignments and load in the cluster, update it in the CRD status and communicate to all the interested pods.

![](https://static.slab.com/prod/uploads/cam7h8fn/posts/images/ocCPZcGEqlMMU99UEeRWfxbh.png)



## Discarding half-committed entries

Both in leader and followers, there can be, at the end of an epoch, some entries that were persisted in the local WAL but for which the required majority quorum was not reached at the moment where the epoch was closed.

When the operator announces that _Epoch-X_ is closed and a new _Epoch-Y_ starts, it includes the `first_entry` of _Epoch-Y_. Leader and followers will discard any entries that is greater or equal than `first_entry`, for any `epoch < Y`.

## WAL trimming

Each storage pod, leader or follower, will be able to independently trim the WAL, after a configurable amount of time has passed, and up to the point where all the transactions are already applied in to underlying KV storage.

## Fetching a snapshot

If we are trying to bootstrap a new follower, a trying to recover a follower which has lagged behind for too much, when it tries to do the `Follow()` operation, it will get an error, because the 1st requested log entry is not found in the leader&#39;s WAL anymore.

At that point, the follower needs to fetch a snapshot of the shard&#39;s data and start to `Follow()` again after that. The snapshot will be streamed back to the new follower and it will replace any previous existing state.

```java
rpc GetSnaphot(Empty) returns (stream LogEntry) {}
```

On the leader side, serving the snapshot is implemented by having a &quot;consistent&quot; cursor over the KV storage. This cursor is associated with a specific log entry `le-x` in the WAL, and iterating over that cursor will give the view of the KV state as it was at the moment when `le-x` was applied.

## Changing the replication factor

It is possible to dynamically update the replication factor for the cluster. The operator will apply the change by closing the current epoch for a shard and informing the leader of the new replication factor. The followers will be either started or stopped, depending on whether it&#39;s increasing or decreasing the replication factor. The leader will then use the new size when deciding when an entry is fully committed.
