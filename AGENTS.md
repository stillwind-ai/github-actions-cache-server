## Project

A self-hosted drop-in replacement for the GitHub Actions cache service, written in Rust (edition 2024, tokio, axum, SeaORM 2). It supports exactly one database, Postgres, and one storage backend, the filesystem. The TypeScript server, SQLite, MySQL, S3 and GCS are gone (ADR-0010), so don't reintroduce them or follow ADRs that only describe them.

### Layout

- `src/lib.rs`: `App`, which connects and migrates the database, sets up storage and serves the router. `src/main.rs` is a thin binary around it.
- `src/config.rs`: every environment variable, parsed in `Config::from_vars`.
- `src/routes/`: the Twirp `CacheService` and downloads (`cache.rs`), Azure-blob-style uploads (`upload.rs`), `/management-api` (`management.rs`), Results Passthrough (`proxy.rs`).
- `src/storage/`: the storage engine (Uploads, Cache Entries, Storage Locations, Merge, leases). `fs.rs` is filesystem storage; `io.rs` runs file I/O on io_uring worker threads, or on `tokio::fs` as a fallback.
- `src/entity/`, `src/migration/`: SeaORM entities and migrations. Migrations run at startup, inside a transaction and under an advisory lock.
- `src/cleanup.rs`: scheduled cleanup jobs and Capacity-based Eviction.
- `tests/`: integration tests against a real server (`tests/common/mod.rs`).
- `benches/`: load benchmarks that emulate real cache clients (see `benches/README.md`).
- `nix/`, `flake.nix`: package, NixOS module and VM test. `install/kubernetes/`: Helm chart. `scripts/`: release metadata tooling (Python).

### Commands

Tests need Postgres. Each test creates its own throwaway database on the server that `TEST_DATABASE_URL` points at (default `postgres://postgres@127.0.0.1:5432/postgres`).

```sh
docker compose up -d postgres
export TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres

cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo test --locked
STORAGE_FILESYSTEM_IO_URING=false cargo test --locked   # the tokio::fs fallback
cargo bench --bench workloads --locked -- --quick       # smoke test of the benchmarks
cargo run                                               # reads .env
```

These are the commands CI runs (`.github/workflows/ci-cd.yml`), so run the ones your change touches before pushing. CI also runs `nix flake check`, builds the Docker image, lints the Helm chart (`helm lint` / `helm template`) and runs `python3 -m unittest discover -s scripts -p 'test_*.py'`.

### Conventions

- Clippy `pedantic` is on in `Cargo.toml` and CI rejects warnings. Fix the lint rather than adding `#[allow]`, unless the allow is local and explained.
- The `io-uring` feature (on by default, Linux only) must stay optional. Code has to build and pass tests with `--no-default-features`, and at runtime with `STORAGE_FILESYSTEM_IO_URING=false`.
- A new or changed environment variable goes in `src/config.rs`, the README's configuration table, and the Helm chart (`values.yaml` and `templates/_helpers.tpl`) when the chart should expose it. The NixOS module passes variables through `settings`.
- A schema change is a new migration in `src/migration/`. Never edit a migration that has already been released.
- Postgres is the source of truth for which storage may be kept (ADR-0001). Uploads become visible atomically (ADR-0004), and active downloads are protected by leases (ADR-0002). Read those ADRs before changing storage or cleanup.
- Commits follow Conventional Commits (`feat:`, `fix(storage):`, `perf:`, …); the changelog is generated from them.

## Agent skills

### Issue tracker

Issues and PRDs are tracked in GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the default five triage labels. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout. See `docs/agents/domain.md`.
