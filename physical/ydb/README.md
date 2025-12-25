# TODO: move to [https://github.com/hashicorp/web-unified-docs](https://github.com/hashicorp/web-unified-docs))

# YDB Physical Backend

This document describes the YDB physical backend shipped under `vault/physical/ydb`. It covers general information, configuration options, transactional behavior, testing helpers, and troubleshooting notes useful for development and CI.

## Overview

The YDB backend stores Vault physical data in Yandex YDB using the official Go SDK. It supports Vault's transactional interface and exposes the common physical backend contract. The implementation contains:

- A production backend in `vault/physical/ydb/ydb.go`.
- A testing helper in `vault/helper/testhelpers/ydb/ydbhelper.go` which can either use an existing YDB DSN or start a local Docker YDB instance for integration tests.

## Features

- Standard physical backend operations (Get/Put/Delete/List).
- Native transactional execution path using the YDB SDK (`table.DoTx` / `TransactionActor.Execute`), integrated with Vault's `GenericTransactionHandler`.
- Reasonable defaults for transactional limits to match Vault expectations.

## Configuration

The backend is configured via the map passed to `NewYDBBackend(conf map[string]string, logger)`. Common keys and environment fallbacks:

- `dsn` — YDB DSN (e.g. `grpc://host:port` or `grpcs://...`). If not provided in config, the code will look for `VAULT_YDB_DSN`.
- `table` — YDB table name to use. If not provided, the code will look for `VAULT_YDB_TABLE` and otherwise use the default table name constant in the backend (`VAULT_TABLE`).
- `token` — optional access token to authenticate to YDB. If not provided in the config map, a token may be read from the environment variable `VAULT_YDB_TOKEN`. Note: Yandex.Cloud-specific helper options such as `internal_ca` and `service_account_key_file` have been removed; if you require special TLS or service-account handling, provide credentials or TLS configuration externally.

Example (Go-style config map shown for illustration):
```/dev/null/example.go#L1-12
```
// Example conf map passed to NewYDBBackend
conf := map[string]string{
    "dsn":   "grpc://127.0.0.1:2136",
    "table": "vault_kv",
    // Optionally provide an access token instead of YC helpers:
    // "token": "eyJhbGciOi...",
}
b, err := ydb.NewYDBBackend(conf, logger)
```

## Transactional Behavior

- The backend implements `physical.Transactional` and `physical.TransactionalLimits`.
- Native transactional execution is implemented via YDB SDK transactional actors (using `table.DoTx`).
- For cases that require pseudo-transactional behavior, Vault's `GenericTransactionHandler` is used to wrap the YDB transactional implementation so Vault's transaction tests operate correctly.

Transactional limits returned by the backend (common defaults used by Vault):
- max entries per transaction: 63
- max size per entry: 128 KiB (131072 bytes)

These limits are exposed via the `TransactionalLimits()` implementation.

## Testing

Integration tests for the YDB backend were updated to use the test helper in:
- `vault/helper/testhelpers/ydb/ydbhelper.go`

Behavior of the helper:
- If `VAULT_YDB_DSN` is set in the environment, the helper will use that DSN and (optionally) `VAULT_YDB_TABLE`.
- If `VAULT_YDB_DSN` is not set, the helper looks for `YDB_DOCKER_REPO` (and optional `YDB_DOCKER_TAG`) and will start a Docker container exposing gRPC ports for a local YDB instance.
- If neither DSN nor Docker repo is provided, YDB integration tests are skipped.
- By default the helper creates a unique table name per run (e.g. `vault_kv_<timestamp>`) to avoid conflicts with system keys and parallel test runs.
- The helper ensures the YDB table exists, retrying schema creation until available.

Typical local workflow:
- Provide `VAULT_YDB_DSN` or set `YDB_DOCKER_REPO`/`YDB_DOCKER_TAG` in your environment.
- Run tests:
```/dev/null/example.sh#L1-6
# Example (local):
export YDB_DOCKER_REPO="ghcr.io/ydb-platform/ydb"
export YDB_DOCKER_TAG="latest"
go test ./vault/physical/ydb -v
```

Or point to an existing YDB service:
```/dev/null/example.sh#L1-4
export VAULT_YDB_DSN="grpc://your-ydb-host:2136"
export VAULT_YDB_TABLE="vault_kv"
go test ./vault/physical/ydb -v
```

For CI:
- Either provide an accessible YDB Docker image (`YDB_DOCKER_REPO`/`YDB_DOCKER_TAG`) to pull in the runner environment, or set a reachable `VAULT_YDB_DSN` that the CI runners can use.
- Consider making `VAULT_YDB_TABLE` configurable to reuse an existing table across runs if desired.

## Troubleshooting / Notes

- Scan behavior: The YDB Go SDK expects optional (nullable) byte columns to be scanned into pointer types (for example `*[]byte`). Scanning nullable byte columns into `[]byte` can produce "type *[]uint8 is not optional! use double pointer or sql.Scanner..." errors during transactional operations. The backend implementation scans into `*[]byte` and converts to `[]byte` before returning results to Vault.
- Secure DSNs: If using `grpcs://` in your DSN and you need to provide credentials, supply an access token via the `token` config key or the `VAULT_YDB_TOKEN` environment variable, or configure TLS/CA externally. Yandex.Cloud-specific helper options have been removed from this implementation.
- Table/schema readiness: The test helper creates the table and retries until the schema service is ready. In production environments ensure the table exists with the expected schema before starting Vault or plan for initial schema setup (the helper demonstrates a safe retry pattern).
- Logs and debugging: Transaction failures surface via the YDB SDK errors; adding additional logging around transaction actor execution and query parameters may help debug intermittent issues, especially in CI.

## Where to look in the code

- Backend implementation: `vault/physical/ydb/ydb.go`
- Transactional helper registration and DoTx path: implemented in the backend file referenced above.
- Test helper and Docker runner: `vault/helper/testhelpers/ydb/ydbhelper.go`
