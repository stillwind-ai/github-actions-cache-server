mod cache;
mod management;
mod misc;
mod proxy;
mod upload;

use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{any, get, post, put};

use crate::auth::Auth;
use crate::cleanup::Cleanup;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::storage::Storage;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<Storage>,
    pub auth: Arc<Auth>,
    pub metrics: Arc<Metrics>,
    pub cleanup: Arc<Cleanup>,
    pub http: reqwest::Client,
}

const CACHE_SERVICE: &str = "/twirp/github.actions.results.api.v1.CacheService";

pub fn router(state: AppState) -> Router {
    let debug = state.config.debug;
    let mut router = Router::new()
        .route("/", get(misc::index))
        .route("/health", get(misc::health))
        .route("/metrics", get(misc::metrics))
        .route(
            &format!("{CACHE_SERVICE}/CreateCacheEntry"),
            post(cache::create_cache_entry),
        )
        .route(
            &format!("{CACHE_SERVICE}/FinalizeCacheEntryUpload"),
            any(cache::finalize_cache_entry_upload),
        )
        .route(
            &format!("{CACHE_SERVICE}/GetCacheEntryDownloadURL"),
            post(cache::get_cache_entry_download_url),
        )
        .route("/upload/{upload_id}", put(upload::upload))
        .route("/devstoreaccount1/upload/{upload_id}", put(upload::upload))
        .route("/download/{cache_entry_id}", get(cache::download))
        .nest("/management-api", management::router(state.clone()))
        .fallback(proxy::results_passthrough)
        // Uploads and passthrough bodies are streamed, never buffered.
        .layer(DefaultBodyLimit::disable())
        .with_state(state);

    if debug {
        router = router.layer(tower_http::trace::TraceLayer::new_for_http());
    }
    router
}
