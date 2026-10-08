//! Workload generators, driven by `profiles.json`: profiles distilled from
//! traffic the real clients sent to this server (see `benches/README.md`).
//! Archive sizes, key shapes, restore keys, `BuildKit` layer chains and the
//! layers each build downloads are the captured ones; keys are fresh, and
//! every generator is seeded, so a scenario sends the same requests each run.

use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::FutureExt;
use futures::future::BoxFuture;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;

use crate::clients::{ActionsCache, Buildkit, Ctx, Layer, MIB, Sccache, bounded};

pub struct Scenario {
    pub name: &'static str,
    pub description: &'static str,
    pub env: Vec<(&'static str, &'static str)>,
    /// Unmeasured: puts the server in the scenario's starting state.
    pub setup: for<'a> fn(&'a Ctx) -> BoxFuture<'a, ()>,
    pub run: for<'a> fn(&'a Ctx) -> BoxFuture<'a, ()>,
}

#[derive(Deserialize)]
struct Profiles {
    buildkit: BTreeMap<String, Image>,
    actions_cache: Vec<ActionsTrace>,
    sccache: Vec<SccacheBuild>,
}

/// The objects one cold sccache build wrote, one per compilation unit.
#[derive(Deserialize)]
struct SccacheBuild {
    objects: Vec<u64>,
}

#[derive(Deserialize)]
struct Image {
    chain: Vec<u64>,
    index_size: u64,
    warm_downloads: Vec<usize>,
    #[serde(default)]
    change_new_layers: Vec<u64>,
    #[serde(default)]
    change_downloads: Vec<usize>,
}

#[derive(Deserialize)]
struct ActionsTrace {
    source: String,
    saves: Vec<Save>,
    lookups: Vec<Lookup>,
}

#[derive(Deserialize)]
struct Save {
    key: String,
    size: u64,
}

#[derive(Deserialize)]
struct Lookup {
    restore_keys: usize,
}

static PROFILES: LazyLock<Profiles> =
    LazyLock::new(|| serde_json::from_str(include_str!("profiles.json")).expect("profiles.json"));

static QUICK: AtomicBool = AtomicBool::new(false);

fn quick() -> bool {
    QUICK.load(Ordering::Relaxed)
}

/// Archives over this size are left out unless `BENCH_FULL=1`, to bound the
/// disk a run needs (each is stored, and merged, once per scenario).
fn max_archive() -> u64 {
    if quick() {
        64 * MIB
    } else if std::env::var("BENCH_FULL").is_ok_and(|value| value == "1") {
        u64::MAX
    } else {
        512 * MIB
    }
}

fn hex(rng: &mut StdRng, len: usize) -> String {
    (0..len)
        .map(|_| char::from_digit(rng.random_range(0..16), 16).unwrap())
        .collect()
}

/// A concrete key from a captured shape: every `<n>` becomes n random hex
/// digits.
fn key_from_shape(shape: &str, rng: &mut StdRng) -> String {
    let mut key = String::new();
    let mut rest = shape;
    while let Some(start) = rest.find('<') {
        let end = start + rest[start..].find('>').unwrap();
        key.push_str(&rest[..start]);
        key.push_str(&hex(rng, rest[start + 1..end].parse().unwrap()));
        rest = &rest[end + 1..];
    }
    key + rest
}

/// One captured `@actions/cache` archive, instantiated with fresh keys.
#[derive(Clone)]
struct Archive {
    source: &'static str,
    key: String,
    restore_keys: Vec<String>,
    version: String,
    size: u64,
}

/// The first archive each captured trace saved, smallest first.
fn archives(seed: u64) -> Vec<Archive> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut archives: Vec<Archive> = PROFILES
        .actions_cache
        .iter()
        .filter_map(|trace| {
            let save = trace.saves.first()?;
            let key = key_from_shape(&save.key, &mut rng);
            // A restore key is the key up to its last `-`: rust-cache's
            // environment prefix, or a workflow's `os-name-` prefix.
            let restore_keys = match trace
                .lookups
                .first()
                .map_or(0, |lookup| lookup.restore_keys)
            {
                0 => Vec::new(),
                _ => vec![key[..=key.rfind('-').unwrap_or(0)].to_owned()],
            };
            Some(Archive {
                source: trace.source.as_str(),
                key,
                restore_keys,
                version: hex(&mut rng, 64),
                size: save.size,
            })
        })
        .filter(|archive| archive.size <= max_archive())
        .collect();
    archives.sort_by_key(|archive| archive.size);
    archives
}

/// A captured `BuildKit` image with fresh layer digests.
struct Build {
    index: String,
    chain: Vec<Layer>,
    index_size: u64,
    warm: Vec<Layer>,
    new_layers: Vec<Layer>,
    change: Vec<Layer>,
}

fn images() -> Vec<&'static str> {
    if quick() {
        vec!["whoami-min", "node"]
    } else {
        vec!["whoami", "node", "rust"]
    }
}

fn build(name: &str, scope: &str) -> Build {
    let image = &PROFILES.buildkit[name];
    // Digests depend only on the image, so builds of it share layers.
    let mut rng = StdRng::seed_from_u64(name.bytes().map(u64::from).sum());
    let layer = |size: u64, rng: &mut StdRng| Layer {
        digest: hex(rng, 64),
        size,
    };
    let chain: Vec<Layer> = image
        .chain
        .iter()
        .map(|&size| layer(size, &mut rng))
        .collect();
    let pick = |positions: &[usize]| positions.iter().map(|&at| chain[at].clone()).collect();
    let mut scope_rng = StdRng::seed_from_u64(scope.bytes().map(u64::from).sum());
    Build {
        index: format!("index-{scope}-1-{}", hex(&mut scope_rng, 8)),
        warm: pick(&image.warm_downloads),
        change: pick(&image.change_downloads),
        new_layers: image
            .change_new_layers
            .iter()
            .map(|&size| layer(size, &mut rng))
            .collect(),
        chain,
        index_size: image.index_size,
    }
}

async fn save_all(ctx: &Ctx, archives: &[Archive], parallel: usize) {
    let cache = ActionsCache(ctx);
    let saves: Vec<_> = archives
        .iter()
        .map(|archive| cache.save(&archive.key, &archive.version, archive.size))
        .collect();
    assert!(
        bounded(saves, parallel)
            .await
            .into_iter()
            .all(|saved| saved)
    );
}

async fn export_all(ctx: &Ctx) {
    for name in images() {
        let build = build(name, name);
        assert!(
            Buildkit::new(ctx)
                .export(&build.index, &build.chain, build.index_size)
                .await
        );
    }
}

/// Concurrent sccache builds: each captured build twice (as two projects).
/// Cargo's scheduling keeps at most `IN_FLIGHT` cache requests of one build
/// outstanding (2-3 at -j4 in the captures). A unit's key depends on the build and
/// the unit, so a warm build looks up exactly what its cold build wrote.
async fn sccache_builds(ctx: &Ctx) {
    const IN_FLIGHT: usize = 3;
    let builds: Vec<(usize, &SccacheBuild)> = if quick() {
        vec![(0, &PROFILES.sccache[0])]
    } else {
        PROFILES
            .sccache
            .iter()
            .chain(&PROFILES.sccache)
            .enumerate()
            .collect()
    };
    let runs: Vec<_> = builds
        .into_iter()
        .map(|(build, profile)| async move {
            let sccache = Sccache { ctx };
            let mut rng = StdRng::seed_from_u64(build as u64);
            let units: Vec<_> = profile
                .objects
                .iter()
                .map(|&size| {
                    (
                        format!("sccache/{0}/{0}/{0}/{1}", build, hex(&mut rng, 64)),
                        size,
                    )
                })
                .collect();
            let compiles: Vec<_> = units
                .iter()
                .map(|(key, size)| sccache.compile(key, *size))
                .collect();
            bounded(compiles, IN_FLIGHT).await;
        })
        .collect();
    let parallel = runs.len();
    bounded(runs, parallel).await;
}

/// Pull request runs of new branches probing caches that do not exist yet:
/// misses with two restore keys across two scopes.
async fn lookup_storm(ctx: &Ctx) {
    let (clients, lookups) = if quick() { (16, 20) } else { (64, 50) };
    let jobs: Vec<_> = (0..clients)
        .map(|client| async move {
            let pull_request = ctx.pull_request(client);
            let cache = ActionsCache(&pull_request);
            let mut rng = StdRng::seed_from_u64(u64::from(client));
            let version = hex(&mut rng, 64);
            for _ in 0..lookups {
                let key = format!(
                    "v0-rust-test-Linux-x64-{}-{}",
                    hex(&mut rng, 8),
                    hex(&mut rng, 8)
                );
                let restore_keys = vec![
                    key[..key.len() - 8].to_owned(),
                    "v0-rust-test-Linux-x64-".to_owned(),
                ];
                cache.restore(&key, &restore_keys, &version).await;
            }
        })
        .collect();
    bounded(jobs, clients as usize).await;
}

/// A mixed CI fleet: runners each run a sequence of jobs, about half of them
/// pull requests. Dependency-cache jobs restore an `@actions/cache` archive
/// (and after a lockfile change save a new one); Docker jobs import and export
/// `BuildKit` caches.
async fn fleet(ctx: &Ctx) {
    let (runners, jobs_per_runner) = if quick() { (4, 3) } else { (16, 6) };
    let archives = archives(1);
    let images = images();
    let runs: Vec<_> = (0..runners)
        .map(|runner| {
            let archives = &archives;
            let images = &images;
            async move {
                let mut rng = StdRng::seed_from_u64(1000 + u64::from(runner));
                for job in 0..jobs_per_runner {
                    let pull_request = ctx.pull_request(runner * 100 + job);
                    let ctx = if rng.random_bool(0.5) {
                        &pull_request
                    } else {
                        ctx
                    };
                    if rng.random_range(0..10) < 7 {
                        // Mostly exact hits; a lockfile change misses,
                        // restores a prefix match and saves.
                        let archive = &archives[rng.random_range(0..archives.len())];
                        let cache = ActionsCache(ctx);
                        if rng.random_bool(0.85) {
                            cache
                                .restore(&archive.key, &archive.restore_keys, &archive.version)
                                .await;
                        } else {
                            let key = format!(
                                "{}{}",
                                &archive.key[..archive.key.len() - 8],
                                hex(&mut rng, 8)
                            );
                            cache
                                .restore(&key, &archive.restore_keys, &archive.version)
                                .await;
                            cache.save(&key, &archive.version, archive.size).await;
                        }
                    } else {
                        // A warm Docker rebuild, sometimes after a source change.
                        let build = build(images[rng.random_range(0..images.len())], "fleet");
                        let buildkit = Buildkit::new(ctx);
                        if rng.random_bool(0.7) {
                            buildkit
                                .import(&build.index, &build.chain, &build.warm)
                                .await;
                            buildkit
                                .export(&build.index, &build.chain, build.index_size)
                                .await;
                        } else {
                            buildkit
                                .import(&build.index, &build.chain, &build.change)
                                .await;
                            let mut chain = build.chain.clone();
                            chain.extend(build.new_layers.iter().map(|layer| Layer {
                                digest: hex(&mut rng, 64),
                                size: layer.size,
                            }));
                            buildkit
                                .export(&build.index, &chain, build.index_size)
                                .await;
                        }
                    }
                }
            }
        })
        .collect();
    bounded(runs, runners as usize).await;
}

#[allow(clippy::too_many_lines)] // One entry per scenario.
pub fn all(quick: bool) -> Vec<Scenario> {
    QUICK.store(quick, Ordering::Relaxed);
    vec![
        Scenario {
            name: "actions-cache/save",
            description: "cold CI: each job misses its restore keys, then saves its archive (captured sizes)",
            env: Vec::new(),
            setup: |_| async {}.boxed(),
            run: |ctx| {
                async move {
                    let archives = archives(1);
                    let jobs: Vec<_> = archives
                        .iter()
                        .map(|archive| async move {
                            let cache = ActionsCache(ctx);
                            cache
                                .restore(&archive.key, &archive.restore_keys, &archive.version)
                                .await;
                            assert!(
                                cache
                                    .save(&archive.key, &archive.version, archive.size)
                                    .await,
                                "{}",
                                archive.source
                            );
                        })
                        .collect();
                    let parallel = jobs.len();
                    bounded(jobs, parallel).await;
                }
                .boxed()
            },
        },
        Scenario {
            name: "actions-cache/restore",
            description: "warm CI: four matrix jobs per cache restore it at once (exact hits; the first download merges)",
            env: Vec::new(),
            setup: |ctx| async move { save_all(ctx, &archives(1), 4).await }.boxed(),
            run: |ctx| {
                async move {
                    let archives = archives(1);
                    let jobs: Vec<_> = archives
                        .iter()
                        .flat_map(|archive| std::iter::repeat_n(archive, 4))
                        .map(|archive| async move {
                            let restored = ActionsCache(ctx)
                                .restore(&archive.key, &archive.restore_keys, &archive.version)
                                .await;
                            assert_eq!(restored, Some(archive.size), "{}", archive.source);
                        })
                        .collect();
                    let parallel = jobs.len();
                    bounded(jobs, parallel).await;
                }
                .boxed()
            },
        },
        Scenario {
            name: "actions-cache/lookup-misses",
            description: "pull request runs probing caches that do not exist: misses with two restore keys in two scopes",
            env: Vec::new(),
            setup: |_| async {}.boxed(),
            run: |ctx| lookup_storm(ctx).boxed(),
        },
        Scenario {
            name: "buildkit/cold",
            description: "first builds: export every layer of the captured images (sequential 1 MiB blocks)",
            env: Vec::new(),
            setup: |_| async {}.boxed(),
            run: |ctx| {
                async move {
                    let exports: Vec<_> = images()
                        .into_iter()
                        .map(|name| async move {
                            let build = build(name, name);
                            assert!(
                                Buildkit::new(ctx)
                                    .export(&build.index, &build.chain, build.index_size)
                                    .await
                            );
                        })
                        .collect();
                    bounded(exports, 8).await;
                }
                .boxed()
            },
        },
        Scenario {
            name: "buildkit/warm",
            description: "rebuilds, two builders per image: import (lookup burst, needed layers), export (existence checks, new index)",
            env: Vec::new(),
            setup: |ctx| export_all(ctx).boxed(),
            run: |ctx| {
                async move {
                    let builds: Vec<_> = images()
                        .into_iter()
                        .flat_map(|name| [name, name])
                        .map(|name| async move {
                            let build = build(name, name);
                            let buildkit = Buildkit::new(ctx);
                            // Failures are recorded, not fatal: concurrent
                            // exports of one scope race on the index.
                            buildkit
                                .import(&build.index, &build.chain, &build.warm)
                                .await;
                            buildkit
                                .export(&build.index, &build.chain, build.index_size)
                                .await;
                        })
                        .collect();
                    bounded(builds, 8).await;
                }
                .boxed()
            },
        },
        Scenario {
            name: "buildkit/change",
            description: "builds after a source change: import the layers the rebuild needs, export the new ones",
            env: Vec::new(),
            setup: |ctx| export_all(ctx).boxed(),
            run: |ctx| {
                async move {
                    let builds: Vec<_> = images()
                        .into_iter()
                        .map(|name| async move {
                            let build = build(name, name);
                            let buildkit = Buildkit::new(ctx);
                            assert!(
                                buildkit
                                    .import(&build.index, &build.chain, &build.change)
                                    .await
                            );
                            let chain: Vec<Layer> = build
                                .chain
                                .iter()
                                .chain(&build.new_layers)
                                .cloned()
                                .collect();
                            assert!(
                                buildkit
                                    .export(&build.index, &chain, build.index_size)
                                    .await
                            );
                        })
                        .collect();
                    bounded(builds, 8).await;
                }
                .boxed()
            },
        },
        Scenario {
            name: "sccache/cold",
            description: "four Rust builds sharing an empty sccache: a lookup miss and a write per compilation unit",
            env: Vec::new(),
            setup: |_| async {}.boxed(),
            run: |ctx| sccache_builds(ctx).boxed(),
        },
        Scenario {
            name: "sccache/warm",
            description: "the same four builds again: a lookup hit and a download per compilation unit",
            env: Vec::new(),
            setup: |ctx| sccache_builds(ctx).boxed(),
            run: |ctx| sccache_builds(ctx).boxed(),
        },
        Scenario {
            name: "fleet",
            description: "a mixed CI fleet: runners running dependency-cache and Docker jobs, half of them pull requests",
            env: Vec::new(),
            setup: |ctx| {
                async move {
                    save_all(ctx, &archives(1), 4).await;
                    for name in images() {
                        let build = build(name, "fleet");
                        assert!(
                            Buildkit::new(ctx)
                                .export(&build.index, &build.chain, build.index_size)
                                .await
                        );
                    }
                }
                .boxed()
            },
            run: |ctx| fleet(ctx).boxed(),
        },
    ]
}
