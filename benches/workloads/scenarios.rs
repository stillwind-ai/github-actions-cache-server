//! Workload generators. Sizes, counts and shapes are calibrated against the
//! captured traffic described in `benches/README.md`; every generator is
//! seeded, so a scenario sends the same requests on every run.

use futures::FutureExt;
use futures::future::BoxFuture;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::clients::{ActionsCache, Buildkit, Ctx, KIB, Layer, MIB, Sccache, bounded};

pub struct Scenario {
    pub name: &'static str,
    pub description: &'static str,
    pub env: Vec<(&'static str, &'static str)>,
    /// Unmeasured: puts the server in the scenario's starting state.
    pub setup: for<'a> fn(&'a Ctx) -> BoxFuture<'a, ()>,
    pub run: for<'a> fn(&'a Ctx) -> BoxFuture<'a, ()>,
}

/// An empirical size distribution: (quantile, bytes) points, sampled with
/// log-linear interpolation between them.
pub struct Sizes(pub &'static [(f64, u64)]);

impl Sizes {
    pub fn sample(&self, rng: &mut StdRng) -> u64 {
        let q: f64 = rng.random();
        let points = self.0;
        let upper = points
            .iter()
            .position(|&(at, _)| at >= q)
            .unwrap_or(points.len() - 1);
        if upper == 0 {
            return points[0].1;
        }
        let (q0, s0) = points[upper - 1];
        let (q1, s1) = points[upper];
        let t = if q1 > q0 { (q - q0) / (q1 - q0) } else { 1.0 };
        ((s0 as f64).ln() + t * ((s1 as f64).ln() - (s0 as f64).ln())).exp() as u64
    }
}

fn hex(rng: &mut StdRng, len: usize) -> String {
    (0..len)
        .map(|_| char::from_digit(rng.random_range(0..16), 16).unwrap())
        .collect()
}

pub fn all(quick: bool) -> Vec<Scenario> {
    let _ = quick;
    vec![Scenario {
        name: "placeholder",
        description: "to be calibrated",
        env: Vec::new(),
        setup: |_| async {}.boxed(),
        run: |ctx| {
            async move {
                let mut rng = StdRng::seed_from_u64(1);
                let sizes = Sizes(&[(0.0, 10 * KIB), (1.0, 10 * MIB)]);
                let version = hex(&mut rng, 64);
                let cache = ActionsCache(ctx);
                let size = sizes.sample(&mut rng);
                cache.save("k", &version, size).await;
                cache.restore("k", &[], &version).await;
                let buildkit = Buildkit::new(ctx);
                let layers = vec![Layer {
                    digest: hex(&mut rng, 64),
                    size: 3 * MIB,
                }];
                buildkit
                    .export("index-buildkit-1-abcdef01", &layers, 2 * KIB)
                    .await;
                buildkit
                    .import("index-buildkit-1-abcdef01", &layers, 3)
                    .await;
                let sccache = Sccache { ctx };
                let units: Vec<_> = (0..4)
                    .map(|i| {
                        let sccache = &sccache;
                        async move {
                            sccache
                                .compile(&format!("sccache/a/b/c/{i}"), 100 * KIB)
                                .await
                        }
                    })
                    .collect();
                bounded(units, 4).await;
            }
            .boxed()
        },
    }]
}
