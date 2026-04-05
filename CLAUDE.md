# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all tests (unit + integration)
cargo test -p hermes-broker-core # Test a single crate
cargo test test_name             # Run a single test by name
cargo clippy --workspace         # Lint
cargo fmt                        # Format (enforced by pre-push hook)
cargo bench -p hermes-broker-core # Run throughput benchmarks (criterion)
cargo run -p hermes-broker-server # Start server on [::1]:50051
```

Environment variables: `RUST_LOG` (tracing filter, default "info"), `HERMES_LOG_FORMAT=json` for JSON log output.

## Architecture

hermes-rs is a lightweight message broker with gRPC streaming, organized as a Cargo workspace with 4 crates:

- **proto** (`hermes-broker-proto`) — Protobuf/gRPC definitions, compiled via `tonic-prost-build` in `build.rs`
- **core** (`hermes-broker-core`) — Routing engine: trie-based subject matching, fanout and queue group delivery
- **client** (`hermes-broker-client`) — Publisher (client-streaming, fire-and-forget) and Subscriber (bidirectional streaming) APIs. Depends on proto.
- **server** (`hermes-broker-server`) — gRPC server binary. Depends on proto and core.

### Single-threaded Router (actor model)

The Router (`core/src/router.rs`) is the central component — a single async task processing commands via an mpsc channel. This lock-free design means all routing state (trie + slot map) is owned by one task with no synchronization needed.

Three command types: `Publish` (assign sequence, lookup trie, deliver), `Subscribe` (register in slot map + trie), `Disconnect` (clean up subscriber, remove empty slots).

### Trie-based Subject Matching

`core/src/trie.rs` implements a prefix trie over dot-separated subject tokens with two wildcards:
- `*` matches exactly one token
- `>` matches one or more trailing tokens (must be last segment)

### Slot Map (dual delivery modes)

`core/src/slot.rs` manages routing slots indexed by `(subject, queue_group)`:
- **Broadcast** slots use `tokio::sync::broadcast` — all subscribers receive every message
- **QueueGroup** slots use `mpsc` channels with round-robin — one member receives each message

Slots are created lazily on first subscription and removed when the last subscriber disconnects.

### gRPC Service

`server/src/grpc.rs` implements two RPCs: `Publish` (client-streaming, ack on close with total count) and `Subscribe` (bidirectional streaming). Each subscribe stream spawns a forwarding task.

## Proto Definition

Source: `crates/proto/proto/hermes/broker/v1/broker.proto`. Generated code goes to OUT_DIR with a file descriptor set for gRPC reflection.
