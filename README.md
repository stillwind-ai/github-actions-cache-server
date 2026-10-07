# 🚀 GitHub Actions Cache Server

This is a drop-in replacement for the official GitHub hosted cache server. It is compatible with the official `actions/cache` action, so there is no need to change your workflow files and it even works with packages that internally use `actions/cache`.

## Features

- 🔥 **Compatible with official `actions/cache` action**
- 🦀 Written in Rust: a single static binary using a few tens of MB of memory
- 💽 Filesystem storage with [io_uring](https://kernel.dk/io_uring.pdf) file I/O (falls back to regular I/O where io_uring is unavailable)
- 🐘 PostgreSQL for cache metadata, safe to run with multiple replicas sharing a volume
- 🔒 Secure and self-hosted, giving you full control over your cache data.
- 😎 Easy setup

```yaml
services:
  cache-server:
    image: ghcr.io/falcondev-oss/github-actions-cache-server
    ports:
      - '3000:3000'
    environment:
      API_BASE_URL: http://localhost:3000
      STORAGE_FILESYSTEM_PATH: /data/cache
      DB_POSTGRES_URL: postgres://postgres:postgres@postgres:5432/postgres
    volumes:
      - cache-data:/data
    depends_on:
      - postgres

  postgres:
    image: postgres:17
    environment:
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres-data:/var/lib/postgresql/data

volumes:
  cache-data:
  postgres-data:
```

## Configuration

| Variable                                                                                                  | Default                                                  | Description                                                                                                                                                        |
| --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `API_BASE_URL`                                                                                            | _required_                                               | Base URL of the server, reachable by your runners.                                                                                                                 |
| `PORT` / `HOST`                                                                                           | `3000` / all interfaces                                  | Listen address.                                                                                                                                                    |
| `DB_POSTGRES_URL`                                                                                         |                                                          | PostgreSQL connection URL.                                                                                                                                         |
| `DB_POSTGRES_HOST`, `DB_POSTGRES_PORT`, `DB_POSTGRES_DATABASE`, `DB_POSTGRES_USER`, `DB_POSTGRES_PASSWORD` |                                                          | Alternative to `DB_POSTGRES_URL`.                                                                                                                                  |
| `DB_POSTGRES_MAX_CONNECTIONS`                                                                             | `10`                                                     | Connection pool size.                                                                                                                                              |
| `STORAGE_FILESYSTEM_PATH`                                                                                 | `.data/storage/filesystem`                               | Directory owned by the server for cache data.                                                                                                                      |
| `STORAGE_FILESYSTEM_IO_URING`                                                                             | `true`                                                   | Use io_uring for file I/O when the kernel allows it. Docker's default seccomp profile blocks io_uring; the server then logs a warning and uses regular file I/O. |
| `STORAGE_FILESYSTEM_IO_URING_THREADS`                                                                     | `2`                                                      | io_uring worker threads, each with its own ring.                                                                                                                   |
| `CACHE_CLEANUP_OLDER_THAN_DAYS`                                                                           | `90`                                                     | Delete entries neither saved nor restored for this long. `0` disables.                                                                                             |
| `CACHE_MAX_SIZE_BYTES`                                                                                    |                                                          | Storage Budget in bytes, instead of a share of the volume.                                                                                                 |
| `CACHE_FILESYSTEM_MAX_USAGE_PERCENT`                                                                      | `90`                                                     | Volume usage that triggers Capacity-based Eviction.                                                                                                                |
| `ORPHANED_STORAGE_GRACE_PERIOD_HOURS`                                                                     | `24`                                                     | Age before unreferenced storage is deleted.                                                                                                                        |
| `EAGER_MERGE`                                                                                             | `false`                                                  | Merge uploaded parts at upload completion instead of on first download.                                                                                            |
| `DISABLE_CLEANUP_JOBS`                                                                                    | `false`                                                  | Disable the periodic cleanup jobs.                                                                                                                                 |
| `MANAGEMENT_API_KEY`                                                                                      |                                                          | Enables the management API under `/management-api`, authenticated with the `X-Api-Key` header.                                                                     |
| `ACTIONS_TOKEN_ISSUER`                                                                                    | `https://token.actions.githubusercontent.com`            | OIDC issuer of runner tokens (set for GitHub Enterprise Server).                                                                                                   |
| `ACTIONS_TOKEN_JWKS_URL`                                                                                  | discovered                                               | Override the JWKS URL instead of using OIDC discovery.                                                                                                             |
| `DEFAULT_ACTIONS_RESULTS_URL`                                                                             | `https://results-receiver.actions.githubusercontent.com` | Where requests the server doesn't handle (e.g. artifacts) are forwarded.                                                                                           |
| `SKIP_TOKEN_VALIDATION`                                                                                   | `false`                                                  | Development only: accept unsigned tokens.                                                                                                                          |
| `DEBUG`                                                                                                   |                                                          | Debug logging. `RUST_LOG` overrides log filtering.                                                                                                                 |

A database used by the previous TypeScript server is migrated by dropping its tables: cached entries are discarded and their files reclaimed as orphaned storage.

## Management API

With `MANAGEMENT_API_KEY` set, these routes are available (send the key as `X-Api-Key`):

- `GET /management-api/cache-entries?key=&version=&scope=&repoId=&itemsPerPage=&page=`
- `GET /management-api/cache-entries/match?primaryKey=&restoreKeys=&scopes=&repoId=&version=`
- `GET|DELETE /management-api/cache-entries/{id}`
- `DELETE /management-api/cache-entries?key=&version=&scope=&repoId=`
- `GET|DELETE /management-api/storage-locations/{id}`

## Development

```sh
docker compose up -d postgres
cargo run                # reads .env
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres cargo test
```

Tests create a throwaway database per test on the server `TEST_DATABASE_URL` points at.

## Documentation

👉 <https://gha-cache-server.falcondev.io/getting-started> 👈
