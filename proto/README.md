# MPPI wire definitions

[`mppi/v1/mppi.proto`](mppi/v1/mppi.proto) is the public wire source of truth.
The [protocol profile](../docs/protocol.md) defines validation, payment semantics,
message directions and resource bounds. Generated bindings do not perform these
semantic checks automatically.

The package is `mppi.v1`. It defines exactly two v1 methods:

- `/mppi.v1.Chat/Completions`: one Complete request, a response stream.
- `/mppi.v1.Control/Session`: bidirectional Control messages.

Binary protobuf carries typed payment records and original UTF-8 JSON inference
payload bytes. The schema has no imported definitions. Typed payment errors use
`google.rpc.Status` and `google.protobuf.Any` in standard gRPC trailers; those
are runtime error dependencies, not additional RPCs.

`mppi.v1.binpb` is the generated descriptor set. Python and Rust bindings are
generated from this source with the repository's pinned local build tooling.
Do not edit generated files. Preserve released field numbers and reserve a
retired field's number and name before removing it.
