# Maelstrom

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