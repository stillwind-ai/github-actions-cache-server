//! Prometheus metrics. Per-process, as in ADR-0007: scale with replicas and
//! aggregate in PromQL.

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

pub struct Metrics {
    registry: Registry,
    pub cache_requests_total: IntCounterVec,
    pub cache_uploads_total: IntCounter,
    cache_storage_bytes: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(
                prometheus::process_collector::ProcessCollector::for_self(),
            ))
            .expect("register process collector");

        let cache_requests_total = IntCounterVec::new(
            Opts::new(
                "cache_requests_total",
                "Cache download-URL lookups by result. A restore-key prefix match counts as a hit.",
            ),
            &["result"],
        )
        .expect("valid metric");
        // Materialize both series at 0 so dashboards see them before the first event.
        cache_requests_total.with_label_values(&["hit"]);
        cache_requests_total.with_label_values(&["miss"]);
        let cache_uploads_total = IntCounter::new(
            "cache_uploads_total",
            "Cache uploads finalized into cache entries.",
        )
        .expect("valid metric");
        let cache_storage_bytes = IntGauge::new(
            "cache_storage_bytes",
            "Total bytes of finalized cache payloads tracked across storage locations.",
        )
        .expect("valid metric");

        for collector in [
            Box::new(cache_requests_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(cache_uploads_total.clone()),
            Box::new(cache_storage_bytes.clone()),
        ] {
            registry.register(collector).expect("register metric");
        }

        Self {
            registry,
            cache_requests_total,
            cache_uploads_total,
            cache_storage_bytes,
        }
    }

    pub fn record_lookup(&self, hit: bool) {
        self.cache_requests_total
            .with_label_values(&[if hit { "hit" } else { "miss" }])
            .inc();
    }

    /// Renders the text exposition format; storage bytes are computed at
    /// scrape time from the sizes recorded at upload completion (ADR-0008).
    pub fn render(&self, cache_storage_bytes: u64) -> String {
        self.cache_storage_bytes.set(cache_storage_bytes as i64);
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buffer)
            .expect("text encoding");
        String::from_utf8(buffer).expect("text exposition is UTF-8")
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
