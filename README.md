# 🚀 GitHub Actions Cache Server

This is a drop-in replacement for the official GitHub hosted cache server. It is compatible with the official `actions/cache` action, so there is no need to change your workflow files and it even works with packages that internally use `actions/cache`.

## Features

- 🔥 **Compatible with official `actions/cache` action**
- 🦀 Written in Rust: a single static binary using a few tens of MB of memory
- 💽 Filesystem storage with [io_uring](https://kernel.dk/io_uring.pdf) file I/O (falls back to regular I/O where io_uring is unavailable)
- 🪣 S3 storage (AWS S3 or any S3-compatible store such as MinIO, RustFS, Ceph or Cloudflare R2), with optional direct downloads from the bucket
- 🐘 PostgreSQL for cache metadata, safe to run with multiple replicas sharing a volume or a bucket
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
| `STORAGE_DRIVER`                                                                                          | `filesystem`                                             | `filesystem` or `s3`.                                                                                                                                              |
| `STORAGE_FILESYSTEM_PATH`                                                                                 | `.data/storage/filesystem`                               | Directory owned by the server for cache data.                                                                                                                      |
| `STORAGE_FILESYSTEM_IO_URING`                                                                             | `true`                                                   | Use io_uring for file I/O when the kernel allows it. Docker's default seccomp profile blocks io_uring; the server then logs a warning and uses regular file I/O. |
| `STORAGE_FILESYSTEM_IO_URING_THREADS`                                                                     | `2`                                                      | io_uring worker threads, each with its own ring.                                                                                                                   |
| `STORAGE_S3_BUCKET`                                                                                       | _required for `s3`_                                      | Existing bucket for cache data. The server owns everything under the `gh-actions-cache/` prefix.                                                                  |
| `AWS_REGION`                                                                                              | `us-east-1`                                              | Bucket region.                                                                                                                                                     |
| `AWS_ENDPOINT_URL`                                                                                        | AWS                                                      | Endpoint of an S3-compatible store, e.g. `http://minio:9000`.                                                                                                     |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`                                         | default credential chain                                 | Static credentials. Without them the AWS default chain applies (profile, web identity/IRSA, ECS, instance metadata).                                              |
| `STORAGE_S3_FORCE_PATH_STYLE`                                                                             | `true`                                                   | Path-style (`endpoint/bucket/key`) instead of virtual-hosted-style bucket addressing.                                                                             |
| `STORAGE_S3_SOCKET_TIMEOUT_MS`                                                                            | `10000`                                                  | Time allowed for an S3 response and between chunks of a download (data uploads and copies get at least 5 minutes).                                               |
| `ENABLE_DIRECT_DOWNLOADS`                                                                                 | `false`                                                  | S3 only: runners download merged entries straight from the bucket through presigned URLs. Runners must reach the S3 endpoint.                                     |
| `CACHE_CLEANUP_OLDER_THAN_DAYS`                                                                           | `90`                                                     | Delete entries neither saved nor restored for this long. `0` disables.                                                                                             |
| `CACHE_MAX_SIZE_BYTES`                                                                                    |                                                          | Storage Budget in bytes, instead of a share of the volume. With S3, Capacity-based Eviction only runs when this is set.                                    |
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

### S3 storage

```yaml
environment:
  STORAGE_DRIVER: s3
  STORAGE_S3_BUCKET: gh-actions-cache
  AWS_REGION: eu-central-1
  # For S3-compatible stores:
  # AWS_ENDPOINT_URL: http://minio:9000
  # AWS_ACCESS_KEY_ID: ...
  # AWS_SECRET_ACCESS_KEY: ...
```

The bucket must exist. The server needs `s3:ListBucket` on it and `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:AbortMultipartUpload` on `gh-actions-cache/*`. Writes are multipart uploads that only become visible when complete; a server killed mid-upload leaves an incomplete multipart upload behind, so configure an `AbortIncompleteMultipartUpload` lifecycle rule on the bucket. With `EAGER_MERGE`, entries whose parts are all at least 5 MiB are merged inside S3 (`UploadPartCopy`) without passing through the server.

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

# The same suite on S3 storage, against the RustFS service:
docker compose up -d postgres rustfs
TEST_STORAGE_DRIVER=s3 TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres cargo test
```

Tests create a throwaway database per test on the server `TEST_DATABASE_URL` points at, and with `TEST_STORAGE_DRIVER=s3` a bucket per test on `TEST_S3_ENDPOINT` (default `http://127.0.0.1:9000`, credentials `TEST_S3_ACCESS_KEY_ID`/`TEST_S3_SECRET_ACCESS_KEY`, default `access_key`/`secret_key`).

## Documentation

👉 <https://gha-cache-server.falcondev.io/getting-started> 👈
