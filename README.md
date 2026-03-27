# hermes-rs

Hermes is an asynchronous Rust event broker ecosystem built on gRPC.

This workspace contains the core crates needed to build, run, and consume Hermes-based messaging systems:

- `hermes-client`: typed async client for publish/subscribe
- `hermes-server`: broker server runtime
- `hermes-core`: shared event traits and subject types
- `hermes-proto`: protobuf/gRPC generated interfaces
- `hermes-store`: durable storage integration
- `hermes-macros`: procedural macros (e.g. `derive(Event)`)

## Workspace layout

- `crates/client`
- `crates/server`
- `crates/core`
- `crates/proto`
- `crates/store`
- `crates/macros`

## Quick start

Build all crates:

```sh
cargo build --workspace
```

Run tests:

```sh
cargo test --workspace
```

Package selected crates before publishing:

```sh
cargo package -p hermes-client
cargo package -p hermes-server
```

## Publishing notes

The crates are configured for dual licensing and intended for publication to crates.io.
Before publishing, make sure repository metadata points to your final public repository URL in each `Cargo.toml`.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.