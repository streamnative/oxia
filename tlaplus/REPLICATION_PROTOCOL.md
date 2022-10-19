# Oxia log replication protocol

This document accompanies the TLA+ specification of the Oxia log replication protocol. Specifically, this protocol
pertains to the data replication of a single shard and does not include matters such as how data is distributed across
shards.

Each shard is simply a log with a state machine. This log replication protocol does not currently include the state
machine or any projected state. It consists of the log only.

## Design

The protocol consists of four principle actors:
- a linearizable metadata store
- a K8s operator responsible for the management of the data nodes.
- nodes that store and replicate entries
- clients that write entries to the leader node

### Metadata store

The metadata store persists the shard state. Only the operator reads or writes to this store.

### Operator

The operator is responsible for:
- performing leader elections
- performing reconfigurations (aka ensemble changes):
  1. swapping one node for another
  1. expanding the shard cluster (to increase the replication factor)
  1. contracting the shard cluster (to decrease the replication factor)

The operator updates the shard metadata in the metadata store at key moments so that if the operator fails, the next
one can continue where the failed one left off. The protocol also supports multiple operators battling for control
without the loss of acknowledged writes (only the loss of availability). While it should not be possible for more than
one operator to run at a time, it is best to design the protocol defensively especially given the importance of this
data.


### Nodes

Nodes do not rely on the metadata store at all. They are relatively simple nodes that do as the operator instructs
them. They have no innate knowledge of the other nodes, have no failure detection or anything like that.

## Terms Used

- Entry Id: The epoch and offset of an entry.
- Head index: the last entry id in the log of a node.
- Follow cursor: A cursor stored in the leader that indicates the position of a follower in terms of last pushed and
  last confirmed.

## Comparing entry ids

Compared with precedence:
1. Epoch
1. Offset

e1,o2 is lower than e2,o1 for example.

## Leader election

The exact nature of the failure detection mechanism is not proscribed by this protocol. A simple heartbeat will
suffice. Upon detecting a failure of the leader the operator initiates a leader election.

1. Operator updates the metadata with:
   - epoch: an incremented epoch
   - status: election
1. Operator sends a `Fence Request` to each node in the `Fencing Ensemble` (more on what the fencing ensemble is
   below).
1. When a node receives a `Fence Request` it changes it's state to `Fenced` and will no longer perform any data 
   replication activities. It responds with a `Fence Response` which includes its head index.
1. Upon receiving a `Fence Response` from a majority of the `Fencing Ensemble` the operator chooses a new leader by 
   selecting the node with the highest head index. Any further fence responses must be kept until the leader confirms
   its leadership.
1. Operator sends a `Become Leader Request` to the selected leader. This request includes a `follower map` which is a
   map of `{node->head index}` for the fence responses received at the time the leader was chosen.
1. The new leader receives the `Become Leader Request` and now needs to establish a follow cursor for the followers in
   the `follower map` of the request. For each follower in the map it either can start the follow cursor immediately
   at the head index indicated or must send the follower a request to truncate its log to a safe point. The leader
   does the following:
   1. Sends a `Become Leader Response` to the operator 
   1. Sends a `Truncate Request` to each follower that needs truncation before a follow cursor can be activated.
   1. Activates a follow cursor for each follower that does not need truncation.
1. Completing the election:
   1. Operator receives `Become Leader Response` and completes the election by updating the metadata with:
      ```
      status: steady state
      leader: node id of elected leader
      ```
   1. All remaining fence responses can now be dealt with. For each fence response from the late minority the operator
      sends the leader an `Add Follower Request` with the followers head index.
   1. For each `Add Follower Request` the leader receives it either starts a follow cursor immediately or requests
      that the follower truncate its log to a safe point.
1. Truncation (concurrent with 7)
   1. Node receives `Truncate Request`. It truncates its log to the indicated index. Sends a `Truncate Response` to
      the leader.
   1. Leader receives a `Truncate Response` and starts the follow cursor at the indicated head index.

### Fencing Ensemble and Final Ensemble

Elections may be required to resolve stuck/stalled reconfigurations. Reconfiguration operations are executed as
two-phase commits. If a reconfiguration stalls in the commit phase then the leader may or may not have applied the
ensemble change and any newly added node may or may not have received entries. In the example of a node swap
reconfiguration, it is possible that the new node did indeed get added and received data, but we can't know for sure.
If we only fence a majority of the current ensemble stored in the metadata is is possible for two disjoint majorities
to be operating and replicating data (split-brain).

To avoid this we must include the new node that was part of a node swap or expand reconfiguration in the ensemble of
nodes being fenced. This is the fencing ensemble. Once a majority of responses have been received from this ensemble
the candidates for leader and follower must belong to the "final ensemble".

The final ensemble is the ensemble that is the result of any ensemble change that a stalled reconfiguration in the
commit phase would have applied. From this ensemble we select the leader and followers.

### Truncation

There are two reasons why a follower must truncate its log:
- The epoch of its head index is lower than the epoch of the leader's head index. In this case it is possible for the
  follower to have dirty entries in its log that do not exist in the leader's log and they must be removed. The leader
  must find the highest entry id it has in its log at or below the follower's head index epoch and send a truncate 
  request to the follower with that entry id.
- The head index of the follower is higher than the head index of the leader. The leader simply uses its own head index
  in the truncate request.

Note that only late minority fence responses can have higher head indexes than the leader.

## Replication in the steady state

The leader simply sends a stream of entries to each follower, keeping track via the follow cursors by updating the
fields:
- `last pushed (entry id)`
- `last confirmed (entry id)`

The commit index can be advanced when an entry id that is higher than the current commit index complies with the
following conditions:
- the entry id has reached majority quorum (based on the follow cursor last confirmed)
- the entry id epoch is of the current epoch
- the log prefix has reached majority quorum

Note that entries of prior epochs that are higher than the commit index can only be committed indirectly, by committing
a higher entry id whose epoch matches the current epoch. While not intuitive, there are edge cases that are possible
where entries that reached a majority quorum end up overwritten after leader elections. See the Raft paper for more
details on this rule.

The protocol as specified so far does not include the `apply index` required for applying commands to the state
machine. The rule for this is simple. The apply index cannot pass the commit index and can be advanced anytime there
is a gap between the commit index and the apply index.

## Reconfigurations

Reconfiguration is the process of changing the shard ensemble safely through a controlled process. 

There are three types of reconfiguration:
- swapping one node for another (for example when failure detection has flagged a node)
- expanding the shard cluster (to increase the replication factor)
- contracting the shard cluster (to decrease the replication factor)

Reconfigurations can only be performed when:
- there is an elected leader
- there is no active leader election in-progress
- there is no active reconfiguration in-progress

Reconfigurations increment the epoch.

Reconfigurations that fail or stall can be resolved via a leader election. The leader election will take into account
the fact that the ensemble change may or may not have been applied and will complete the operation.

### Node swap reconfiguration

A node swap operation is the removing of one follower and replacing it with another. This is required when the operator
wishes to replace a failed/unavailable node.
  
A node swap reconfiguration is performed via a two-phase commit:
- `PREPARE` phase:
  1. Operator updates the metadata with the reconfiguration details of: `NODE_SWAP` op, old node, new node, `PREPARE`
     phase and incremented epoch.
  1. Operator sends a Prepare Reconfiguration Request to the leader with the old node id and new epoch.
  1. Leader updates its epoch and fences the old node by deactivating its follow cursor. Sends back a snapshot to the
     operator.
  1. Operator sends the snapshot to the new node.
- `COMMIT` phase:
  1. Operator updates the phase in the reconfig metadata to `COMMIT`.
  1. Send a commit request to the leader with the new node id. 
  1. Leader removes the follow cursor for the old node completely and activates a follow cursor for the new node. Sends
     a response to the operator.
  1. The operator updates the metadata to clear the reconfiguration op state.
                   
Key points:
- a node swap cannot swap out the leader

### Expand reconfiguration

The operator selects a new node to add to the shard ensemble in order to increase the replication factor.
  
An expand reconfiguration is performed via a two-phase commit:
- `PREPARE` phase:
  1. Operator updates the metadata with the reconfiguration details of: `EXPAND OP`, new node, `PREPARE` phase and
     incremented epoch.
  1. Operator sends a Prepare Reconfiguration Request to the leader with the new epoch.
  1. Leader updates its epoch and sends back a snapshot to the operator.
  1. Operator sends the snapshot to the new node.
- `COMMIT` phase:
  1. Operator updates the phase in the reconfig metadata to `COMMIT`.
  1. Send a commit request to the leader with the new node id. 
  1. Leader activates a follow cursor for the new node. Sends a response to the operator.
  1. The operator updates the metadata to clear the reconfiguration op state.

### Contract reconfiguration

This operation reduces the replication factor by removing a node from the ensemble safely.
  
A node cannot be removed safely if by removing it the leader's commit index would be recalculated as a lower index
(taking into account the reduction in rep factor into the calculation). This would lower the redundancy level below
majority for some committed entries.

A contract reconfiguration is performed via a two-phase commit although it strictly does not require two phases for
correctness. The reason to use a two-phase commit is that we can fence off the node we want to remove during the
prepare phase and then wait for the commit index in the leader to pass the head index of the node to be removed. At
that point the commit phase will succeed.

- `PREPARE` phase:
  1. Operator updates the metadata with the reconfiguration details of: `CONTRACT` op, old node, `PREPARE` phase.
  1. Leader fences the old node by deactivating its follow cursor. Sends response to the operator.
- `COMMIT` phase:
  1. Operator updates the phase in the reconfig metadata to `COMMIT`.
  1. Send a commit request to the leader. 
  1. Leader removes the follow cursor for the old node completely, but only if the commit index is not affected.
     Confirms to the operator if it was successful or not.
  1. The operator updates the metadata to clear the reconfiguration op state upon success.
                      
Key points:
 - The operator can attempt the commit phase repeatedly until the leader responds with success.


## Message reference

For reference, also see the TLA+ specification which lists the messages, their fields and when they are sent and by
who.

```
Fence Request:                    Operator -> Node
Fence Response:                   Node -> Operator
Become Leader Request:            Operator -> Node (selected leader)
Become Leader Response:           Node (selected leader) -> Operator
Add Follower Request:             Operator -> Node (selected leader)
Add Follower Response:            Node (selected leader) -> Operator
Truncate Request:                 Node (leader) -> Node (follower)
Truncate Response:                Node (follower) -> Node (leader)
Snapshot Request:                 Operator -> Node (new follower in node swap or expand)
Snapshot Response:                Node -> Operator
Add Entry Request:                Node (leader) -> Node (follower)
Add Entry Response:               Node (follower) -> Node (leader)
Prepare Reconfiguration Request:  Operator -> Node (leader)
Prepare Reconfiguration Response: Node (leader) -> Operator
Commit Reconfiguration Request:   Operator -> Node (leader)
Commit Reconfiguration Response:  Node (leader) -> Operator
```

Not included in this specification:

```
Abort Reconfiguration Request:   Operator -> Node (leader)
Abort Reconfiguration Response:  Node (leader) -> Operator
```