# ProtoDB

*A versatile strongly typed watchable key/value store based on [badger](https://github.com/dgraph-io/badger) that allow you to scale when your needs evolve.*

### Why ?

**Protocol-buffer** (protobuf) is an incredible technology.

More than a fast serialization protocol, it is a networked type system with support for interfaces and reflection.

It makes it a perfect choice for strongly typed key-value store storage format.

### Status

*it is currently being used in its current implementation in multiple production applications. It largely lack testing, unit tests, regression tests, integration tests, end-to-end tests are missing, the API is often broken, but it will soon be stabilized. The replication is mostly done on a best effort basis and is mostly not tested as well. Metrics are mostly broken as it had significant changes in the latest badger releases.*  


### Principles

A single interface that allows to switch with very little work between the different modes:

- In Process (with in-memory mode support)
- Server
- Replicated (alpha) *available in server, in-process and in-memory modes*

### Features

- changes notifications

- Transactions

- Ephemeral storage (TTL)

- Filtering (based on [protofilters](https://github.com/linka-cloud/protofilters)) and token based paging.

- Defaulting (using [protoc-gen-defaults](https://github.com/linka-cloud/protoc-gen-defaults) annotations)

- Validation (using [protoc-gen-validate](https://github.com/buf/protoc-gen-validate))

Planned:

- Leases
- Indexes support
- [Reflection based CEL validation](https://github.com/bufbuild/protovalidate)
- [CEL based filtering](https://github.com/google/cel-spec)
- *Pointers support ?*
- Raft replication

### Where are the binaries ?

There are no binaries at the moment.

The server implementation is provided but you will still need to implement the server with the features you want, (authentication, tls, metrics, tracing...) to use it in server mode.

That way, you may embed the **protobuf descriptors** in the key-value store without the need to register them. This is right now the only way to support validation.
