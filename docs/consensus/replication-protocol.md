
# Oxia replication protocol

The term “replication” perhaps underplays the scope of the replication protocol. Certainly it is concerned with how
writes are replicated within a set of storage nodes. However, it extends significantly beyond that. The protocol 
additionally concerns itself with the client read pathway, and how storage nodes should behave during a leader election.
In an effort to deal with these concerns separately, we’ll first consider the protocol’s persistence behaviours when 
storage nodes are in a steady state, and then those that related to the coordination of nodes, specifically when the
state of the cluster is changing. Note however that The **Replication Protocol** comprises both concerns.

We’ll first define some commonalities: Oxia data is distributed across a number of shards. Each shard comprises 1 leader
replica, and 0 or more follower replicas. The leader accepts all client requests. It then streams data changes to its 
followers so that they are in a position to take on the position of leader if that is required. In a real cluster, 
storage nodes will lead some shards and follow others.


> ℹ️ Note that the protocol is scoped to a single shard. The protocol is described in the context of a single shard, but
> can be applied to each shard independently of one another. The protocol does not include matters such as how data is
> distributed across shards.


The movement of data between storage nodes is scoped within the [storage](replication-storage.md) component of the
protocol. The assignment of leaders and followers is scope within the remaining, much larger, 
[coordinator](replication-coordinator.md) component.


## Participants

The protocol consists of four principal actors:

- A linearizable metadata store
- A coordinator responsible for the management of the data nodes.
- Storage nodes that store and replicate entries
- Clients that write entries to the leader node

### Metadata Store

The metadata store persists the shard state. Only the coordinator reads or writes to this store.

### Coordinator

Responsible for:

- Performing leader elections
- Performing reconfigurations of a shard’s set of storage nodes (aka ensemble changes):
    1. Swapping one node for another
    2. Expanding a shard’s set of storage nodes (to increase the replication factor)
    3. Contracting a shard’s set of storage nodes (to decrease the replication factor)

The coordinator updates the shard metadata in the metadata store at key moments so that if the coordinator fails, 
the next one can continue where the failed one left off. The protocol also supports multiple coordinators battling for
control without the loss of acknowledged writes (only the loss of availability — i.e. they cannot be read by the
client). While it should not be possible for more than one coordinator to run at a time, it is best to design the
protocol defensively especially given the importance of the data.

## Storage Nodes

Nodes do not rely on the metadata store at all. They are relatively simple nodes that do as the coordinator instructs
them. They have no innate knowledge of the other nodes, and any awareness or interactions they have with one another
will be based on instruction originating from the coordinator.

### Leader

A shard’s set of storage node will include one that has been assigned the responsibility of leader. It will accept all
client requests for the shard, and will interact with its followers (as assigned by the coordinator) to ensure that they
create replicas of the data.

### Followers

A shard’s set of storage nodes will include 0 or more follower nodes. Followers provide replicas of the data, such that
they can take on the responsibility of the leader should the current leader fail. They take no active role in the
serving of client requests, or the forwarding of entries.
