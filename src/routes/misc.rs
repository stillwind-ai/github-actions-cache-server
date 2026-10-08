use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use super::AppState;
use crate::error::ApiError;

pub async fn index() -> &'static str {
    "OK"
}

pub async fn health() -> &'static str {
    "healthy"
}

pub async fn metrics(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let bytes = state.storage.total_stored_bytes().await?;
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(bytes),
    ))
}
