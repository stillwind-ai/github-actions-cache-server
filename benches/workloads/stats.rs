//! Per-operation latencies, transfer totals and failures of one scenario.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Stats {
    latencies: Mutex<BTreeMap<&'static str, Vec<Duration>>>,
    failures: Mutex<BTreeMap<&'static str, (u64, String)>>,
    pub uploaded: AtomicU64,
    pub downloaded: AtomicU64,
    /// Longest stall between body chunks of any download: `@actions/cache`
    /// aborts a restore after 5 s without a byte.
    max_body_gap: Mutex<Duration>,
}

impl Stats {
    pub fn record(&self, op: &'static str, elapsed: Duration) {
        self.latencies
            .lock()
            .unwrap()
            .entry(op)
            .or_default()
            .push(elapsed);
    }

    pub fn fail(&self, op: &'static str, error: impl std::fmt::Display) {
        let mut failures = self.failures.lock().unwrap();
        let entry = failures.entry(op).or_insert((0, error.to_string()));
        entry.0 += 1;
    }

    pub fn body_gap(&self, gap: Duration) {
        let mut max = self.max_body_gap.lock().unwrap();
        *max = (*max).max(gap);
    }

    pub fn add_uploaded(&self, bytes: u64) {
        self.uploaded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_downloaded(&self, bytes: u64) {
        self.downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn summary(&self) -> Summary {
        let ops = self
            .latencies
            .lock()
            .unwrap()
            .iter()
            .map(|(op, samples)| {
                let mut samples = samples.clone();
                samples.sort();
                let at = |percent: usize| samples[(samples.len() - 1) * percent / 100];
                OpSummary {
                    op,
                    count: samples.len(),
                    p50: at(50),
                    p90: at(90),
                    p99: at(99),
                    max: *samples.last().unwrap(),
                }
            })
            .collect();
        Summary {
            ops,
            failures: self.failures.lock().unwrap().clone(),
            uploaded: self.uploaded.load(Ordering::Relaxed),
            downloaded: self.downloaded.load(Ordering::Relaxed),
            max_body_gap: *self.max_body_gap.lock().unwrap(),
        }
    }
}

pub struct OpSummary {
    pub op: &'static str,
    pub count: usize,
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub max: Duration,
}

pub struct Summary {
    pub ops: Vec<OpSummary>,
    pub failures: BTreeMap<&'static str, (u64, String)>,
    pub uploaded: u64,
    pub downloaded: u64,
    pub max_body_gap: Duration,
}
