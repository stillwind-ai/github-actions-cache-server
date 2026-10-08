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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;
pub use crate::routes::AppState;

/// A fully initialized server: database migrated, storage ready.
pub struct App {
    pub state: AppState,
    /// Stops the cleanup scheduler.
    pub shutdown: CancellationToken,
}

impl App {
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

        let backend = match &config.storage_driver {
            config::StorageDriver::Filesystem => {
                let io = storage::io::FileIo::new(
                    config.storage_filesystem_io_uring,
                    config.storage_filesystem_io_uring_threads,
                );
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
        })
    }

    pub fn router(&self) -> axum::Router {
        routes::router(self.state.clone())
    }

    /// Serves until `signal` resolves, then drains in-flight requests and
    /// waits for background merges.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        if self.state.config.disable_cleanup_jobs {
            tracing::info!("Cleanup jobs are disabled");
        } else {
            cleanup::spawn_scheduler(self.state.cleanup.clone(), self.shutdown.clone());
        }

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
