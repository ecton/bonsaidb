# BonsaiDb

![BonsaiDb is considered experimental and unsupported](https://img.shields.io/badge/status-experimental-blueviolet)
[![crate version](https://img.shields.io/crates/v/bonsaidb.svg)](https://crates.io/crates/bonsaidb)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/bonsaidb/Tests/main)](https://github.com/khonsulabs/bonsaidb/actions?query=workflow:Tests)
[![HTML Coverage Report for `main` branch](https://khonsulabs.github.io/bonsaidb/coverage/badge.svg)](https://khonsulabs.github.io/bonsaidb/coverage/)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/bonsaidb/main/bonsaidb/)

BonsaiDb aims to be a [Rust](https://rust-lang.org)-written, ACID-compliant, document-database inspired by [CouchDB](https://couchdb.apache.org/). While it is inspired by CouchDB, this project will not aim to be compatible with existing CouchDB servers, and it will be implementing its own replication, clustering, and sharding strategies.

## Project Goals

The high-level goals for this project are:

- ☑️ Be able to build a document-based database's schema using Rust types.
- ☑️ Run within your Rust binary, simplifying basic deployments.
- ☑️ Run as a local-only file-based database with no networking involved.
- ☑️ Run as a networked server using QUIC with TLS enabled by default
- Easily set up read-replicas between multiple servers.
- Easily run a highly-available quorum-based cluster across at least 3 servers
- ☑️ Expose a Publish/Subscribe eventing system
- Expose a Job queue and scheduling system -- a la [Sidekiq](https://sidekiq.org/) or [SQS](https://aws.amazon.com/sqs/)

## ⚠️ Status of this project

**You should not attempt to use this software in anything except for experiments.** This project is under active development (![GitHub commit activity](https://img.shields.io/github/commit-activity/m/khonsulabs/bonsaidb)), but at the point of writing this README, the project is too early to be used.

If you're interested in chatting about this project or potentially wanting to contribute, come chat with us on Discord: [![Discord](https://img.shields.io/discord/578968877866811403)](https://discord.khonsulabs.com/).

## Example

Check out [./bonsaidb/examples](./bonsaidb/examples) for examples. To get an idea of how it works, this is a simple schema:

```rust: source @ snippet-a
bonsaidb/examples/view-examples.rs
```

After you have your collection(s) defined, you can open up a database and insert documents:

```rust: source @ snippet-b
bonsaidb/examples/view-examples.rs
```

And query data using the Map-Reduce-powered view:

```rust: source @ snippet-c
bonsaidb/examples/view-examples.rs
```

## Why write another database?

- Deploying highly-available databases is hard (and often expensive). It doesn't need to be.
- We are passionate Rustaceans and are striving for an ideal of supporting a 100% Rust-based deployment ecosystem for newly written software.
- Specifically for the founding author [@ecton](https://github.com/ecton), the idea for this design dates back to thoughts of fun side-projects while running my last business which was built atop CouchDB. Working on this project is fulfilling a long-time desire of his.

```rust: source @ snippet-c
bonsaidb/examples/view-examples.rs
```

## Feature Flags

No feature flags are enabled by default in the `bonsaidb` crate. This is
because in most Rust executables, you will only need a subset of the
functionality. If you'd prefer to enable everything, you can use the `full`
feature:

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "full" }
```

- `full`: Enables `local-full`, `server-full`, and `client-full`.
- `cli`: Enables the `bonsaidb` executable.

### Local databases only

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "local-full" }
```

- `local-full`: Enables `local`, `local-cli`, `local-keyvalue`, and
  `local-pubsub`
- `local`: Enables the [`local`](https://dev.bonsaidb.io/main/bonsaidb/local/) module, which re-exports the crate
  `bonsaidb-local`.
- `local-cli`: Enables the `StructOpt` structures for embedding database
  management commands into your own command-line interface.
- `local-pubsub`: Enables `PubSub` for `bonsaidb-local`.
- `local-keyvalue`: Enables the key-value store for `bonsaidb-local`.

### `BonsaiDb` server

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "server-full" }
```

- `server-full`: Enables `server`, `server-websockets`, `server-keyvalue`,
  and `server-pubsub`
- `server`: Enables the [`server`](https://dev.bonsaidb.io/main/bonsaidb/server/) module, which re-exports the crate
  `bonsaidb-server`.
- `server-websockets`: Enables `WebSocket` support for `bonsaidb-server`.
- `server-pubsub`: Enables `PubSub` for `bonsaidb-server`.
- `server-keyvalue`: Enables the key-value store for `bonsaidb-server`.

### Client for accessing a `BonsaiDb` server

```toml
[dependencies]
bonsaidb = { version = "*", default-features = false, features = "client-full" }
```

- `client-full`: Enables `client`, `client-trusted-dns`,
  `client-websockets`, `client-keyvalue`, and `client-pubsub`
- `client`: Enables the [`client`](https://dev.bonsaidb.io/main/bonsaidb/client/) module, which re-exports the crate
  `bonsaidb-client`.
- `client-trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `client-websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `client-pubsub`: Enables `PubSub` for `bonsaidb-client`.
- `client-keyvalue`: Enables the key-value store for `bonsaidb-client`.

## Backups

### Exporting and restoring databases with direct access

If you have a local `BonsaiDb` database, you can use the `local-backup` command to save and load backups:

```sh
bonsaidb local-backup <database-path> save
```

```sh
bonsaidb local-backup <destination-database-path> load <backup-path>
```

The format of this export should be easy to work with if you're either transitioning from BonsaiDb to another solution or needing to do complicated disaster recovery work. It is [described here](https://khonsulabs.github.io/bonsaidb/main/bonsaidb/local/backup/enum.Command.html#variant.Save).

## Developing BonsaiDb

### Pre-commit hook

Our CI processes require that some commands succeed without warnings or errors. To ensure that code you submit passes the basic checks, install the included [pre-commit](./git-pre-commit-hook.sh) hook:

```bash
./git-pre-commit-hook.sh install
```

Once done, tools including `cargo fmt`, `cargo doc`, and `cargo test` will all be checked before `git commit` will execute.

## Open-source Licenses

This project, like all projects from [Khonsu Labs](https://khonsulabs.com/), are open-source. This repository is available under the [MIT License](./LICENSE-MIT) or the [Apache License 2.0](./LICENSE-APACHE).