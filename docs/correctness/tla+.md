# TLA +


[TLA+](https://lamport.azurewebsites.net/tla/tla.html) is a high-level language for modeling distributed and
concurrent systems.

The Oxia TLA model [OxiaReplication.tla](../../tlaplus/OxiaReplication.tla), contains a model of data replication for a
single shard.

The TLA+ tools are able to use this model and explore all the possible states, verifying that the system properties are
not violated (eg: missing entries in the log).

You can run the TLA+ tools by doing:

```shell
$ make tla
```

It will download all the required TLA+ tools and run the validation.