# Benchmarks

`cargo bench` replays CI cache traffic modelled on what the real clients send, against the server binary, and reports what a CI fleet would feel and what the server spends:

```sh
export TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
cargo bench --bench workloads                       # every scenario, ~3 GB of disk
cargo bench --bench workloads -- buildkit sccache   # scenarios whose name contains a filter
cargo bench --bench workloads -- --quick            # small sizes: a smoke test
```

Each scenario starts a fresh server (the release binary of this checkout) on a fresh database and storage directory, runs an unmeasured setup (for example saving the caches a warm build restores), then the measured run. It prints wall time, transfer rate, the server's user and system CPU time and peak RSS (from `/proc`, so Linux only), the longest stall between body chunks of any download (`@actions/cache` aborts a restore after 5 s without a byte), failures, and latency percentiles per request type.

| Variable | Effect |
|---|---|
| `BENCH_JSON=out.json` | save the results |
| `BENCH_COMPARE=out.json` | print each metric's change against a saved run |
| `BENCH_SERVER_BIN=path` | benchmark another build, e.g. the base branch's |
| `BENCH_DB_RTT_MS=1` | put Postgres behind a proxy adding this round-trip time, as a managed database in another zone would be |
| `BENCH_FULL=1` | include the captured archives over 512 MiB (react's 908 MiB yarn cache, typst's 1 GiB rust-cache) |
| `BENCH_COLD=1` | drop the page cache between setup and run, so reads hit the disk (needs root) |
| `BENCH_STORAGE_DIR` | where storage directories go (default: the system temp dir) |
| `STORAGE_FILESYSTEM_IO_URING=false`, … | server variables pass through |

Comparing two builds: `BENCH_SERVER_BIN=../base/target/release/github-actions-cache-server BENCH_JSON=base.json cargo bench`, then `BENCH_COMPARE=base.json cargo bench`. Interleave and repeat runs on a noisy machine; wall times on shared VMs vary by tens of percent, CPU time and peak RSS much less.

## Scenarios

| Scenario | What it models |
|---|---|
| `actions-cache/save` | cold CI: every captured `@actions/cache` archive (17 MiB to 512 MiB) is missed, then saved, all at once |
| `actions-cache/restore` | warm CI: four matrix jobs per archive restore it at once; the first download of each starts its Merge |
| `actions-cache/lookup-misses` | 64 pull request runs probing 50 nonexistent keys each, with two restore keys, across two scopes |
| `buildkit/cold` | first Docker builds: the captured images' layer chains exported layer by layer in 1 MiB blocks |
| `buildkit/warm` | two concurrent rebuilds per image: import (lookup burst, needed layers) and export (existence checks, new index) |
| `buildkit/change` | rebuilds after a source change: the captured layers the rebuild downloads, the new layers it exports |
| `sccache/cold`, `sccache/warm` | four Rust builds sharing an sccache: a write per compilation unit, then a read per unit |
| `fleet` | 16 runners × 6 jobs, half pull requests: 70% dependency-cache jobs (85% exact hits, else a restore-key hit and a save), 30% Docker rebuilds |

## Where the workloads come from

The traffic was captured by running the clients' real code against this server behind `capture/record-proxy.mjs`, which logs every request as a JSON line (connection, timing, headers that shape traffic, byte counts, Twirp bodies). `capture/run-server.sh` starts a server behind the proxy and prints the runner variables a client needs. `capture/distill.py` reduces the traces to `workloads/profiles.json` — key shapes, sizes, outcomes; no project data — which the generators in `workloads/scenarios.rs` read.

| Client | Captured with | Profile |
|---|---|---|
| `actions/cache`, `actions/setup-node` (`@actions/cache` 6.x on the v2 service) | the actions' own `dist/` code with a runner environment: commander.js (npm), vite (pnpm, Playwright browsers), react (yarn); miss, exact hit, restore-key hit, 4-job matrices | archive sizes and key shapes |
| `Swatinem/rust-cache` 2.9 | its `dist/` code around real builds of fd, axum and typst | archive sizes and key shapes |
| BuildKit `type=gha` (buildx 0.37, BuildKit 0.33) | `docker buildx build --cache-from/--cache-to type=gha,mode=max` of traefik/whoami (also `mode=min`), example-voting-app's Node `result` service and zero-to-production (Rust, cargo-chef); cold, warm, after a source change, two racing builders | layer chains, index sizes, the layers each build downloads |
| sccache 0.18 (GHA backend) | cold and warm `cargo` builds of axum and typst | object sizes, one per compilation unit |

The emulators in `workloads/clients.rs` reproduce each client's wire behaviour, as read from its source and confirmed in the traces:

- **`@actions/cache`**: JSON Twirp with keep-alive off, so every call opens a connection. An archive up to 128 MiB is one Put Blob; a larger one is 64 MiB Put Blocks, 8 in flight, then Put Block List. A restore is one lookup and one whole-object GET, no `Range`. One lookup and at most one save per cache step; an exact hit saves nothing.
- **BuildKit**: JSON Twirp, one constant version, the key as its own restore key. Export is strictly sequential: per layer an existence lookup, and for a missing layer Create, Put Blob below 1 MiB or sequential 1 MiB Put Blocks plus a block list, then Finalize; then a new `index-…#N` entry found by prefix match (retried on Twirp `already_exists`, failing the export on anything else). Import is the newest index, a burst of one lookup per layer in the chain (up to 11 in flight, fresh connections), then whole-object GETs of only the layers the build needs, 4 at a time.
- **sccache**: protobuf Twirp on kept-alive connections, exact keys only, one Put Blob per object whatever its size, at most 2-3 requests in flight per build (cargo's scheduling).

Payloads are random bytes: the real archives and layers are zstd or gzip compressed, so incompressible data is what the server sees. Sizes, keys and request order are the captured ones; compile and extraction time between cache requests is not modelled, so scenarios press harder than one real build — they stand for a fleet of builds overlapping.

## Compatibility gaps the captures exposed

Found while capturing; they are server behaviour, not benchmark artefacts:

- sccache refuses an upload URL without a query string (OpenDAL expects a SAS), so it never writes to this server and runs read-only. The captures appended `?sig=capture` in the proxy.
- `CreateCacheEntry` for a key whose upload is in progress answers `200 {"ok":false}`. BuildKit only tolerates a Twirp `already_exists` (409), so concurrent builders of one scope fail their export (`buildkit/warm` counts these as failures).
- A key that is already saved can be created and uploaded again; GitHub refuses it. Matrix jobs therefore upload the same archive several times.
- `GET /download/{id}` ignores `Range` and `x-ms-range`. None of the captured clients sent one, but BuildKit does when it resumes a read.
- A retried Put Block or Put Blob unbalances the started/finished part counters, and Finalize then rejects the upload, so a transient error under load loses the save that the Azure SDK's retry would have rescued.
