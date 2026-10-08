//! Realistic load benchmarks: CI workloads modelled on traffic captured from
//! the real cache clients (see `benches/README.md`), replayed against the
//! server binary. Needs Postgres at `TEST_DATABASE_URL` and Linux (`/proc`).
//!
//! ```sh
//! cargo bench --bench workloads                 # every scenario
//! cargo bench --bench workloads -- sccache      # scenarios matching a filter
//! cargo bench --bench workloads -- --quick      # small sizes, as a smoke test
//! ```
//!
//! `BENCH_JSON=out.json` saves the results and `BENCH_COMPARE=out.json`
//! prints the change against a saved run; `BENCH_SERVER_BIN` benchmarks
//! another build, `BENCH_DB_RTT_MS=1` puts the database a 1 ms round trip
//! away, and server variables such as `STORAGE_FILESYSTEM_IO_URING` pass
//! through.

mod clients;
mod latency;
mod scenarios;
mod server;
mod stats;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{Value, json};

use crate::clients::{Ctx, MIB};
use crate::scenarios::Scenario;
use crate::server::Server;

struct Measurement {
    wall: Duration,
    user: Duration,
    system: Duration,
    peak_rss: u64,
    summary: stats::Summary,
}

async fn run(scenario: &Scenario, payload: &Bytes) -> Measurement {
    let server = Server::start(&scenario.env).await;
    let setup = Ctx::new(&server.url, payload.clone());
    (scenario.setup)(&setup).await;
    let failures = setup.stats.summary().failures;
    assert!(failures.is_empty(), "setup failed: {failures:?}");
    if std::env::var("BENCH_COLD").is_ok_and(|value| value == "1") {
        // Restores read from disk rather than the page cache. Needs root.
        std::process::Command::new("sync").status().unwrap();
        std::fs::write("/proc/sys/vm/drop_caches", "3").expect("drop caches (needs root)");
    }

    let ctx = Ctx::new(&server.url, payload.clone());
    let (user, system) = server.cpu();
    let started = Instant::now();
    (scenario.run)(&ctx).await;
    let wall = started.elapsed();
    let (user_after, system_after) = server.cpu();
    let measurement = Measurement {
        wall,
        user: user_after - user,
        system: system_after - system,
        peak_rss: server.peak_rss(),
        summary: ctx.stats.summary(),
    };
    server.stop().await;
    measurement
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn to_json(measurement: &Measurement) -> Value {
    let summary = &measurement.summary;
    json!({
        "wall_s": measurement.wall.as_secs_f64(),
        "cpu_user_s": measurement.user.as_secs_f64(),
        "cpu_system_s": measurement.system.as_secs_f64(),
        "peak_rss_mib": measurement.peak_rss as f64 / MIB as f64,
        "uploaded_mib": summary.uploaded as f64 / MIB as f64,
        "downloaded_mib": summary.downloaded as f64 / MIB as f64,
        "max_body_gap_ms": ms(summary.max_body_gap),
        "failures": summary.failures.iter().map(|(op, (count, _))| (op.to_string(), json!(count))).collect::<BTreeMap<_, _>>(),
        "ops": summary.ops.iter().map(|op| (op.op.to_string(), json!({
            "count": op.count,
            "p50_ms": ms(op.p50),
            "p90_ms": ms(op.p90),
            "p99_ms": ms(op.p99),
            "max_ms": ms(op.max),
        }))).collect::<BTreeMap<_, _>>(),
    })
}

/// "+12%" against the same metric of a saved run, if there is one.
fn change(baseline: Option<&Value>, path: &[&str], now: f64) -> String {
    let Some(before) = baseline
        .and_then(|baseline| path.iter().try_fold(baseline, |value, key| value.get(key)))
        .and_then(Value::as_f64)
    else {
        return String::new();
    };
    if before == 0.0 {
        return String::new();
    }
    format!(" ({:+.0}%)", (now / before - 1.0) * 100.0)
}

fn report(scenario: &Scenario, measurement: &Measurement, baseline: Option<&Value>) {
    let summary = &measurement.summary;
    let transferred = (summary.uploaded + summary.downloaded) as f64 / MIB as f64;
    let wall = measurement.wall.as_secs_f64();
    let cpu = (measurement.user + measurement.system).as_secs_f64();
    println!("\n## {} — {}", scenario.name, scenario.description);
    println!(
        "wall {wall:.2}s{}  |  {:.0} MiB up, {:.0} MiB down, {:.0} MiB/s",
        change(baseline, &["wall_s"], wall),
        summary.uploaded as f64 / MIB as f64,
        summary.downloaded as f64 / MIB as f64,
        transferred / wall,
    );
    println!(
        "server cpu {cpu:.2}s (user {:.2}s{}, sys {:.2}s{})  |  peak RSS {:.0} MiB{}  |  longest body stall {:.0} ms",
        measurement.user.as_secs_f64(),
        change(baseline, &["cpu_user_s"], measurement.user.as_secs_f64()),
        measurement.system.as_secs_f64(),
        change(
            baseline,
            &["cpu_system_s"],
            measurement.system.as_secs_f64()
        ),
        measurement.peak_rss as f64 / MIB as f64,
        change(
            baseline,
            &["peak_rss_mib"],
            measurement.peak_rss as f64 / MIB as f64
        ),
        ms(summary.max_body_gap),
    );
    println!(
        "{:<16} {:>7} {:>10} {:>10} {:>10} {:>10}",
        "op", "count", "p50 ms", "p90 ms", "p99 ms", "max ms"
    );
    for op in &summary.ops {
        println!(
            "{:<16} {:>7} {:>10.2} {:>10.2} {:>10.2} {:>10.2}{}",
            op.op,
            op.count,
            ms(op.p50),
            ms(op.p90),
            ms(op.p99),
            ms(op.max),
            change(baseline, &["ops", op.op, "p50_ms"], ms(op.p50)).replace('(', "(p50 ")
        );
    }
    for (op, (count, example)) in &summary.failures {
        println!("FAILED {op} x{count}: {example}");
    }
}

#[tokio::main]
async fn main() {
    let mut quick = false;
    let mut filters = Vec::new();
    // `cargo bench` passes `--bench`.
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--bench" => {}
            "--quick" => quick = true,
            filter => filters.push(filter.to_owned()),
        }
    }
    let scenarios: Vec<Scenario> = scenarios::all(quick)
        .into_iter()
        .filter(|scenario| {
            filters.is_empty()
                || filters
                    .iter()
                    .any(|filter| scenario.name.contains(filter.as_str()))
        })
        .collect();
    let baseline: Option<Value> = std::env::var("BENCH_COMPARE").ok().map(|path| {
        serde_json::from_slice(&std::fs::read(&path).expect("read BENCH_COMPARE"))
            .expect("parse BENCH_COMPARE")
    });

    println!(
        "server {}{}",
        server::binary(),
        if quick { " (quick)" } else { "" }
    );
    let mut payload = vec![0u8; 128 * MIB as usize];
    rand::fill(&mut payload[..]);
    let payload = Bytes::from(payload);

    let mut results = serde_json::Map::new();
    for scenario in &scenarios {
        let measurement = run(scenario, &payload).await;
        report(
            scenario,
            &measurement,
            baseline
                .as_ref()
                .and_then(|baseline| baseline.get(scenario.name)),
        );
        results.insert(scenario.name.to_owned(), to_json(&measurement));
    }
    if let Ok(path) = std::env::var("BENCH_JSON") {
        std::fs::write(&path, serde_json::to_vec_pretty(&results).unwrap())
            .expect("write BENCH_JSON");
        println!("\nsaved {path}");
    }
}
