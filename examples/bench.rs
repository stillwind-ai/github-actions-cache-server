//! End-to-end load benchmark. Runs a server binary as a child process against
//! a fresh Postgres database, saves and restores cache entries the way
//! `@actions/cache` does, and reports throughput plus the server's own CPU time
//! and peak RSS (read from `/proc`, so Linux only).
//!
//! ```sh
//! cargo build --release
//! TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo run --release --example bench -- target/release/github-actions-cache-server
//! ```
//!
//! Tunables (environment): `BENCH_ENTRIES` (4), `BENCH_SIZE_MIB` (256),
//! `BENCH_BLOCK_MIB` (32), `BENCH_BLOCK_CONCURRENCY` (4), `BENCH_ROUNDS` (3),
//! `BENCH_LOOKUPS` (500), `BENCH_COLD=1` to drop the page cache before each
//! restore (needs root), and any server variable such as
//! `STORAGE_FILESYSTEM_IO_URING` or `EAGER_MERGE`, which is passed through.

use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use sea_orm::{ConnectionTrait, Database};
use serde_json::{Value, json};

const CACHE_SERVICE: &str = "twirp/github.actions.results.api.v1.CacheService";
const MIB: usize = 1024 * 1024;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// User and system CPU time of a process.
fn cpu_time(pid: u32) -> (Duration, Duration) {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    // Fields after the parenthesized command name; utime and stime are 14 and 15.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    // `/proc` reports in USER_HZ, which is 100 on every mainstream Linux.
    let ticks = |index: usize| Duration::from_millis(fields[index].parse::<u64>().unwrap() * 10);
    (ticks(11), ticks(12))
}

fn status_kib(pid: u32, field: &str) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kib| kib.parse().ok())
        .unwrap_or(0)
}

struct Bench {
    url: String,
    client: reqwest::Client,
    token: String,
    pid: u32,
}

impl Bench {
    async fn twirp(&self, method: &str, body: Value) -> Value {
        let response = self
            .client
            .post(format!("{}/{CACHE_SERVICE}/{method}", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{method}: {}",
            response.status()
        );
        response.json().await.unwrap()
    }

    async fn save(&self, key: &str, payload: &Bytes, block: usize, concurrency: usize) {
        let created = self
            .twirp("CreateCacheEntry", json!({ "key": key, "version": "v" }))
            .await;
        let upload_url = created["signed_upload_url"].as_str().unwrap().to_owned();
        let blocks = payload.len().div_ceil(block);
        futures::stream::iter(0..blocks)
            .map(|index| {
                let body = payload.slice(index * block..((index + 1) * block).min(payload.len()));
                let block_id = STANDARD.encode(format!("{}{index:012}", uuid::Uuid::new_v4()));
                let request = self
                    .client
                    .put(&upload_url)
                    .query(&[("comp", "block"), ("blockid", &block_id)])
                    .body(body)
                    .send();
                async move {
                    assert_eq!(request.await.unwrap().status(), 201);
                }
            })
            .buffer_unordered(concurrency)
            .collect::<()>()
            .await;
        let finalized = self
            .twirp(
                "FinalizeCacheEntryUpload",
                json!({ "key": key, "version": "v", "size_bytes": payload.len().to_string() }),
            )
            .await;
        assert_eq!(finalized["ok"], true);
    }

    async fn restore(&self, key: &str) -> usize {
        let found = self
            .twirp(
                "GetCacheEntryDownloadURL",
                json!({ "key": key, "restore_keys": [], "version": "v" }),
            )
            .await;
        let url = found["signed_download_url"].as_str().unwrap();
        let response = self.client.get(url).send().await.unwrap();
        assert_eq!(response.status(), 200);
        response
            .bytes_stream()
            .try_fold(0, |total, chunk| async move { Ok(total + chunk.len()) })
            .await
            .unwrap()
    }

    /// Runs `run`, returning wall time and server user and system CPU time.
    async fn measure<F: Future<Output = ()>>(&self, run: F) -> [Duration; 3] {
        let (user, system) = cpu_time(self.pid);
        let started = Instant::now();
        run.await;
        let elapsed = started.elapsed();
        let (user_after, system_after) = cpu_time(self.pid);
        [elapsed, user_after - user, system_after - system]
    }
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort();
    values[values.len() / 2]
}

/// Drops the page cache so restores read from disk. Needs root.
fn drop_caches() {
    std::process::Command::new("sync").status().unwrap();
    std::fs::write("/proc/sys/vm/drop_caches", "3").expect("drop caches (needs root)");
}

#[tokio::main]
async fn main() {
    let binary = std::env::args()
        .nth(1)
        .expect("usage: bench <path to server binary>");
    let entries = env_usize("BENCH_ENTRIES", 4);
    let size = env_usize("BENCH_SIZE_MIB", 256) * MIB;
    let block = env_usize("BENCH_BLOCK_MIB", 32) * MIB;
    let concurrency = env_usize("BENCH_BLOCK_CONCURRENCY", 4);
    let rounds = env_usize("BENCH_ROUNDS", 3).max(1);
    let lookups = env_usize("BENCH_LOOKUPS", 500).max(1);
    let cold = std::env::var("BENCH_COLD").is_ok_and(|value| value == "1");

    let admin_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres@127.0.0.1:5432/postgres".into());
    let admin = Database::connect(&admin_url).await.unwrap();
    let database = format!("cache_server_bench_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!(r#"CREATE DATABASE "{database}""#))
        .await
        .unwrap();
    let mut database_url = url::Url::parse(&admin_url).unwrap();
    database_url.set_path(&database);

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let storage = tempfile::tempdir().unwrap();
    let mut server = tokio::process::Command::new(binary)
        .env("HOST", "127.0.0.1")
        .env("PORT", port.to_string())
        .env("API_BASE_URL", &url)
        .env("DB_POSTGRES_URL", database_url.as_str())
        .env("STORAGE_FILESYSTEM_PATH", storage.path())
        .env("SKIP_TOKEN_VALIDATION", "true")
        .env("DISABLE_CLEANUP_JOBS", "true")
        .env("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "100")
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or("warn".into()),
        )
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = server.id().unwrap();

    let client = reqwest::Client::new();
    while client.get(format!("{url}/health")).send().await.is_err() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let claims = json!({
        "ac": json!([{ "Scope": "refs/heads/main", "Permission": 3 }]).to_string(),
        "repository_id": "1",
    });
    let bench = Bench {
        url,
        client,
        token: format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        ),
        pid,
    };

    let mut payload = vec![0u8; size];
    rand::fill(&mut payload[..]);
    let payload = Bytes::from(payload);
    let total = entries * size;
    println!(
        "{entries} entries x {} MiB, {} MiB blocks, {concurrency} concurrent blocks per entry, \
         median of {rounds} rounds{}",
        size / MIB,
        block / MIB,
        if cold { ", cold page cache" } else { "" }
    );

    let mut results: Vec<(&str, [Vec<Duration>; 3])> =
        ["save", "restore (parts)", "restore (merged)"]
            .into_iter()
            .map(|name| (name, Default::default()))
            .collect();
    for round in 0..rounds {
        // Keep disk use to one round: the previous round's keys are never
        // looked up again, so their data can go.
        for folder in std::fs::read_dir(storage.path()).unwrap() {
            std::fs::remove_dir_all(folder.unwrap().path()).unwrap();
        }
        let keys: Vec<String> = (0..entries)
            .map(|index| format!("bench-{round}-{index}"))
            .collect();
        let save = bench
            .measure(async {
                futures::future::join_all(
                    keys.iter()
                        .map(|key| bench.save(key, &payload, block, concurrency)),
                )
                .await;
            })
            .await;
        let restore = || async {
            if cold {
                drop_caches();
            }
            bench
                .measure(async {
                    let sizes =
                        futures::future::join_all(keys.iter().map(|key| bench.restore(key))).await;
                    assert!(sizes.iter().all(|&read| read == size));
                })
                .await
        };
        let parts = restore().await;
        // Let the Merges started by the first restores finish.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let merged = restore().await;
        for ((_, samples), measured) in results.iter_mut().zip([save, parts, merged]) {
            for (sample, value) in samples.iter_mut().zip(measured) {
                sample.push(value);
            }
        }
    }
    let gib = total as f64 / (1024.0 * MIB as f64);
    for (name, [walls, users, systems]) in results {
        let [wall, user, system] = [walls, users, systems].map(median);
        println!(
            "{name:<17} {:>5.0} MiB/s  wall {:>5.2}s  server cpu-s/GiB: user {:.2} sys {:.2}",
            gib * 1024.0 / wall.as_secs_f64(),
            wall.as_secs_f64(),
            user.as_secs_f64() / gib,
            system.as_secs_f64() / gib,
        );
    }

    // Small requests: a cache-hit lookup round trip on a keep-alive connection.
    let mut latencies = Vec::new();
    for _ in 0..lookups {
        let started = Instant::now();
        bench
            .twirp(
                "GetCacheEntryDownloadURL",
                json!({ "key": format!("bench-{}-0", rounds - 1), "restore_keys": [], "version": "v" }),
            )
            .await;
        latencies.push(started.elapsed());
    }
    latencies.sort();
    println!(
        "lookup latency    p50 {:.2}ms  p99 {:.2}ms",
        latencies[latencies.len() / 2].as_secs_f64() * 1000.0,
        latencies[latencies.len() * 99 / 100].as_secs_f64() * 1000.0,
    );

    // Small downloads: the response head and a short body on a keep-alive connection.
    let small = Bytes::from(vec![1u8; 16 * 1024]);
    bench.save("small", &small, small.len(), 1).await;
    let found = bench
        .twirp(
            "GetCacheEntryDownloadURL",
            json!({ "key": "small", "restore_keys": [], "version": "v" }),
        )
        .await;
    let url = found["signed_download_url"].as_str().unwrap().to_owned();
    let mut latencies = Vec::new();
    for _ in 0..lookups {
        let started = Instant::now();
        let body = bench
            .client
            .get(&url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body.len(), small.len());
        latencies.push(started.elapsed());
    }
    latencies.sort();
    println!(
        "16 KiB download   p50 {:.2}ms  p99 {:.2}ms",
        latencies[latencies.len() / 2].as_secs_f64() * 1000.0,
        latencies[latencies.len() * 99 / 100].as_secs_f64() * 1000.0,
    );

    println!(
        "server peak RSS {} MiB, threads {}",
        status_kib(pid, "VmHWM:") / 1024,
        status_kib(pid, "Threads:")
    );

    // SIGTERM rather than SIGKILL, so a wrapper (`perf record`, say) can flush.
    std::process::Command::new("kill")
        .arg(pid.to_string())
        .status()
        .unwrap();
    server.wait().await.unwrap();
    admin
        .execute_unprepared(&format!(r#"DROP DATABASE "{database}" WITH (FORCE)"#))
        .await
        .unwrap();
}
