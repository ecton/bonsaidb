# BonsaiDb Local

BonsaiDb's offline database implementation.

This crate exposes BonsaiDb's local database implementation. The
[`Storage`](https://docs.rs/bonsaidb-client/*/bonsaidb_local/struct.Storage.html) type provides its most common functionality by
implementing the [`StorageConnection`](https://docs.rs/bonsaidb-core/*/bonsaidb_core/connection/trait.StorageConnection.html).

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.70`.

## Feature Flags

By default, the `full` feature is enabled.

- `full`: Enables all the flags below
- `async`: Enables async-compatible types
- `cli`: Enables the `clap` structures for embedding database management
  commands into your own command-line interface.
- `encryption`: Enables at-rest encryption.
- `instrument`: Enables instrumenting with `tracing`.
- `multiuser`: Enables multi-user support.
- `password-hashing`: Enables the ability to use password authentication using
  Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), is open-source.
This repository is available under the [MIT License](./LICENSE-MIT) or the
[Apache License 2.0](./LICENSE-APACHE).

To learn more about contributing, please see [CONTRIBUTING.md](./CONTRIBUTING.md).
