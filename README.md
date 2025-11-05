# Scylla Vector Store

This is an indexing service for ScyllaDB for vector searching functionality.

## Configuration

All configuration of the Vector Store is done using environment variables. The
service supports also `.env` files.

| Variable                      | Description                                                                                                              | Default                   |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------- |
| `VECTOR_STORE_URI`            | The bind address and a listening port of HTTP API                                                                        | `127.0.0.1:6080`          |
| `VECTOR_STORE_SCYLLADB_URI`   | The connection endpoint to ScyllaDB server.                                                                              | `127.0.0.1:9042`          |
| `VECTOR_STORE_OPENSEARCH_URI` | A connection endpoint to an OpenSearch instance HTTP API. If not set, the service uses the USearch library for indexing. |                           |
| `VECTOR_STORE_THREADS`        | How many threads should be used for Vector Store indexing.                                                               | (number of logical cores) |

## Development builds

You need to install [Rust
environment](https://www.rust-lang.org/tools/install). To install all
components run `rustup install` in the main directory of the repository.

Development workflow is similar to the typical `Cargo` development in Rust.

```
$ cargo b [-r]
$ cargo r [-r]
```

To install all cargo tools used in the CI:

```
$ scripts/install-cargo-tools
```

## Subdirectories

- [scripts](./scripts/README.md) - helper scripts for development, release,
  deployment, and testing

```
$ docker build -t vector-store .
```

