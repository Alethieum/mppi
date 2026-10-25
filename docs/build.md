# Building and checking the foundation

Run commands from the public source root (the directory containing
`pyproject.toml` and `buf.yaml`). The Python workspace has two SDK packages and
their shared protocol dependency. Rust and Solidity have their own manifests.

## Toolchain

The local validation profile uses Python 3.13.7, uv 0.10.12, Rust 1.95.0,
Buf 1.73.0 and Foundry 1.8.3. Solidity is pinned to 0.8.37 in
`contracts/foundry.toml`. Python dependencies are locked in `uv.lock`;
Rust dependencies are locked in `sdks/provider/rust/Cargo.lock`.

Install those tools, then:

```sh
uv sync --locked --all-packages
uv run --locked python scripts/check.py
```

The tests use the workspace's `dev` dependency group, which `uv sync` installs
by default. Running `proto/python/tests` outside this workspace also needs
`jsonschema==4.26.0`, which `test_fixture_schema.py` imports, alongside
`pytest==9.1.1`.

Rust regeneration needs `protoc` on PATH. Use the same bundled compiler as
Python generation: `grpcio-tools==1.84.0` (libprotoc 35.1). A wrapper executable
named `protoc` should invoke the workspace Python interpreter with
`-m grpc_tools.protoc`, forwarding its arguments. `PROTOC` may name that wrapper
explicitly. Runtime protobuf is separately pinned to 7.36.2.

The check command verifies generated files, protobuf lint/format, Python
lint/format/types/tests, Rust format/lint/tests, Solidity format/tests and ABI drift,
independent Python and Rust package builds, installed wheel imports and the
locked Python runtime dependencies against published vulnerability advisories.
It exits on failure. `--report /path/to/results.json` records each executed
check and its exit status. Dependency downloads and the advisory query need
network access; no hosted CI service is required.

## Regeneration

```sh
uv run --locked python scripts/generate.py
uv run --locked python scripts/generate.py --check
cargo run --manifest-path sdks/provider/rust/Cargo.toml --locked \
  --features codegen --example generate -- proto sdks/provider/rust/src/generated
```

Commit generated Python, its type declarations, the descriptor set and Rust
bindings together with any schema change. SDK consumers use the checked-in
generated code; they do not need a schema compiler.

## What a passing foundation check establishes

The schema generates reproducibly, both SDK roles can be packaged together,
the compiled interfaces match their declarations, and the shared fixtures
agree across languages. The native gRPC tests exercise generated transports and
typed trailers using test servicers.

The suite also checks implemented Python payment services and contract state
transitions using local fixtures. Live wallet-service compatibility and deployed
chain results require separate acceptance runs; this command submits no live
payments and does not establish deployment acceptance.

## Tool documentation

Verified against [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/),
[Buf local generation](https://buf.build/docs/generate/usage/),
[gRPC Python generation](https://grpc.io/docs/languages/python/quickstart/),
[Cargo packaging](https://doc.rust-lang.org/cargo/commands/cargo-package.html)
and [Foundry tests](https://getfoundry.sh/forge/tests/overview/).
