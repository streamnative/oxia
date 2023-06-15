# StreamNative Oxia

<img align="right" width="350" height="350" src="docs/oxia-logo.png">

[![Build](https://github.com/streamnative/oxia/actions/workflows/pr_build_and_test.yaml/badge.svg)](https://github.com/streamnative/oxia/actions/workflows/pr_build_and_test.yaml)

Oxia is a scalable metadata store and coordination system that can be used as the core infrastructure to build
large scale distributed systems.

## Why Oxia?

* Design optimized for Kubernetes environment
* Transparent horizontal scalability
* Linearizable per-key operations
* Able to sustain millions of read/write per second
* Able to store 100s of GBs

## Features

* Key-Value interface
* Notification mechanism
* Ephemeral records
* Automated session management

## Documentation

* [Design goals](docs/design-goals.md)
* [Architecture](docs/architecture.md)
* [Getting started with Oxia](docs/getting-started.md)
* [Go client API](docs/go-api.md)
* [Deploy using Helm Chart](docs/ks8-deploy.md)
* Developer docs 
  * [Replication protocol](docs/replication-protocol.md)
    * [Coordinator](docs/replication-coordinator.md)
    * [Storage](docs/replication-storage.md)
  * [Verifying correctness](docs/correctness.md) 
  * [Oxia's K8S resources](docs/kubernetes-oxia-cluster.md)

## License

Copyright 2023 StreamNative, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
