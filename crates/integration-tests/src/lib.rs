// This crate holds integration tests, examples, and benchmarks that depend on
// both `hermes-broker-client` and `hermes-broker-server`. Keeping them here
// avoids circular dev-dependencies between the client and server crates, which
// would otherwise block `cargo publish`.
