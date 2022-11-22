# Oxia

[![Build](https://github.com/streamnative/oxia/actions/workflows/pr_build_and_test.yaml/badge.svg)](https://github.com/streamnative/oxia/actions/workflows/pr_build_and_test.yaml)

## Documentation
* [SNIP-18][snip]
* [High Level Overview][overview]
* [Replication Protocol][rep]

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

### IDE settings
Make sure to add `testing` tag to your compiler settings. This will add files with `//go:build testing` to the compile path.
In GoLand you can do that by adding `testing` in `Preferences`/`Go`/`Build Tags & Vendoring`/`Custom tags` *and* adding `--tags testing` to `Run`/`Edit Configurations...`/`Edit configuration templates`/`Go Test`/`Go tool arguments`.

[snip]: https://streamnative.slab.com/public/posts/snip-18-scalable-metadata-service-03fw44kd
[overview]: docs/Scalable%20Metadata%20Service.md
[rep]: tlaplus/REPLICATION_PROTOCOL.md
[protoc]: https://github.com/protocolbuffers/protobuf#protocol-compiler-installation
[grpc]: https://grpc.io/docs/languages/go/quickstart/
