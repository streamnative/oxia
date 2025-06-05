# Oxia Documentation


## Table of contents

- Architecture
  - [Design Goal](./architecture/design-goals.md)
  - [Physical Architecture](./architecture/physical-architecture.md)
  - [Logical Architecture](./architecture/logical-architecture.md)
  - [Storage Architecture]()
  - [Replication Architecture]()
- Client
  - [Golang](./client/go-api.md)
  - [JVM]()
- Consensus
  - [Leader Election]()
  - [Data Replication]()
- Deployment
  - [Bare Metal](./deployment/bare-metal.md)
  - [Docker]()
  - [Kubernetes](./deployment/kubernetes.md)
- [Development]()
- Correctness
  - [TLA+](./correctness/tla+.md)
  - [Maelstrom](./correctness/maelstrom.md)
- Performance
  - [Perf]()
- Features
  - [Versioning]()
  - [Automatic Session Management]()
  - [Notification]()
  - [Secondary Index]()
  - [PartitionKey]()
  - [Sequence Generation]()
  - [Sequence Notification]()
- Operations
  - Policies
    - [Affinity*/Anti-Affinity]()
  - [Namespace Management]()
  - [Data Node Management]()
  - LoadBalancing
    - [Shards]()
    - [Leader*]()
  - Shards Management
    - [Auto-Splitting*]()
    - [Auto-Merging*]()

> * denotes that support for this feature is planned for the future.