//! A self-hosted drop-in replacement for the GitHub Actions cache service,
//! with filesystem or S3 storage and a Postgres database.

pub mod auth;
pub mod cleanup;
pub mod config;
pub mod db;
pub mod entity;
pub mod error;
pub mod metrics;
pub mod migration;
pub mod routes;
pub mod storage;
pub mod twirp;
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod uring;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::serve::ListenerExt;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
pub use crate::routes::AppState;

/// A fully initialized server: database migrated, storage ready.
pub struct App {
    pub state: AppState,
    /// Stops the cleanup scheduler.
    pub shutdown: CancellationToken,
    /// The rings serving connections and file I/O, when `io_uring` is in use.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    rings: Option<Arc<uring::RingPool>>,
}

/// Starts the `io_uring` threads when configured and the kernel allows it.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn start_rings(config: &Config) -> Option<Arc<uring::RingPool>> {
    if !config.io_uring {
        tracing::info!("io_uring is disabled: connections are served by hyper, file I/O blocks");
        return None;
    }
    match uring::RingPool::start(config.io_uring_threads) {
        Ok(pool) => {
            tracing::info!(
                threads = config.io_uring_threads,
                "Connections and file I/O use io_uring"
            );
            Some(Arc::new(pool))
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "io_uring is unavailable: connections are served by hyper, file I/O blocks"
            );
            None
        }
    }
}

impl App {
    /// # Errors
    ///
    /// If the database can't be connected to or migrated, or the storage root
    /// can't be set up.
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let config = Arc::new(config);

        let db = db::connect(&config)
            .await
            .context("Failed to connect to the database")?;
        tracing::info!("Migrating database...");
        db::migrate(&db)
            .await
            .context("Database migration failed")?;
        tracing::info!("Database migrated");

        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        let rings = start_rings(&config);
        let backend = match &config.storage_driver {
            config::StorageDriver::Filesystem => {
                #[cfg(all(target_os = "linux", feature = "io-uring"))]
                let io = storage::io::FileIo::new(rings.clone());
                #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
                let io = storage::io::FileIo::Tokio;
                let fs = storage::fs::FsStorage::new(&config.storage_filesystem_path, io)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to initialize storage at {}",
                            config.storage_filesystem_path.display()
                        )
                    })?;
                storage::backend::Backend::Filesystem(fs)
            }
            config::StorageDriver::S3(settings) => {
                let s3 = storage::s3::S3Storage::new(settings)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to initialize S3 storage in bucket {}",
                            settings.bucket
                        )
                    })?;
                if config.cache_max_size_bytes.is_none() {
                    tracing::info!(
                        "S3 storage has no capacity limit: set CACHE_MAX_SIZE_BYTES to enable Capacity-based Eviction"
                    );
                }
                storage::backend::Backend::S3(s3)
            }
        };
        tracing::info!(driver = backend.name(), "Storage initialized");
        let storage = Arc::new(storage::Storage::new(db, backend, config.clone()));
        let cleanup = Arc::new(cleanup::Cleanup::new(storage.clone(), config.clone()));

        let state = AppState {
            auth: Arc::new(auth::Auth::new(&config)),
            metrics: Arc::new(metrics::Metrics::new()),
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .build()
                .context("Failed to build HTTP client")?,
            config,
            storage,
            cleanup,
        };
        Ok(Self {
            state,
            shutdown: CancellationToken::new(),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            rings,
        })
    }

    pub fn router(&self) -> axum::Router {
        routes::router(self.state.clone())
    }

    /// Serves until `signal` resolves, then drains in-flight requests and
    /// waits for background merges.
    ///
    /// # Errors
    ///
    /// If the HTTP server fails.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        if self.state.config.disable_cleanup_jobs {
            tracing::info!("Cleanup jobs are disabled");
        } else {
            cleanup::spawn_scheduler(&self.state.cleanup, &self.shutdown);
        }

        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        if let Some(rings) = &self.rings {
            let shutdown = self.shutdown.clone();
            let watcher = tokio::spawn(async move {
                signal.await;
                shutdown.cancel();
            });
            let served = uring::server::serve(
                rings,
                listener.into_std()?,
                self.router(),
                self.shutdown.clone(),
            )
            .await;
            watcher.abort();
            served?;
            tracing::info!("Waiting for ongoing merges...");
            self.state.storage.wait_for_ongoing_merges().await;
            return Ok(());
        }

        // Small responses (Twirp, response heads ahead of a download body)
        // go out at once instead of waiting on Nagle and delayed ACKs.
        let listener = listener.tap_io(|tcp| {
            if let Err(err) = tcp.set_nodelay(true) {
                tracing::debug!(error = %err, "Failed to set TCP_NODELAY");
            }
        });
        let shutdown = self.shutdown.clone();
        axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                signal.await;
                shutdown.cancel();
            })
            .await?;

        tracing::info!("Waiting for ongoing merges...");
        self.state.storage.wait_for_ongoing_merges().await;
        Ok(())
    }
}
