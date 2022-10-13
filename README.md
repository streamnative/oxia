# Oxia

[![Build](https://github.com/streamnative/oxia/actions/workflows/pr_build_and_test.yaml/badge.svg)](https://github.com/streamnative/oxia/actions/workflows/pr_build_and_test.yaml)

## Documentation
* [SNIP-18][snip]
* [High Level Overview][overview]
* [Replcation Protocol][rep]

## Development

### Prerequisites
* `protoc` — [Protocol Buffer compiler][protoc]

### Build
```shell
make test
```

### Package
```shell
make docker
```

[snip]: https://streamnative.slab.com/public/posts/snip-18-scalable-metadata-service-03fw44kd
[overview]: docs/Scalable%20Metadata%20Service.md
[rep]: tlaplus/REPLICATION_PROTOCOL.md
[protoc]: https://github.com/protocolbuffers/protobuf#protocol-compiler-installation