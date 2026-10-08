use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::AppState;
use crate::error::ApiError;
use crate::storage::MatchQuery;
use crate::twirp::{
    CreateCacheEntryRequest, CreateCacheEntryResponse, FinalizeCacheEntryUploadRequest,
    FinalizeCacheEntryUploadResponse, GetCacheEntryDownloadUrlRequest,
    GetCacheEntryDownloadUrlResponse, TwirpError, TwirpRequest, TwirpResponse,
};

fn require_key_and_version(key: &str, version: &str) -> Result<(), TwirpError> {
    if key.is_empty() || version.is_empty() {
        return Err(TwirpError(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Invalid body: `key` and `version` are required",
        )));
    }
    Ok(())
}

fn no_write_scope() -> TwirpError {
    TwirpError(ApiError::new(
        StatusCode::FORBIDDEN,
        "No scope with write permission found",
    ))
}

pub async fn create_cache_entry(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: TwirpRequest<CreateCacheEntryRequest>,
) -> Result<TwirpResponse<CreateCacheEntryResponse>, TwirpError> {
    let scopes = state.auth.token_scopes(&headers).await?;
    let CreateCacheEntryRequest { key, version } = request.message;
    require_key_and_version(&key, &version)?;
    let scope = scopes.write_scope().ok_or_else(no_write_scope)?;

    let upload = state
        .storage
        .create_upload(&key, &version, &scope.scope, &scopes.repo_id)
        .await?;
    Ok(TwirpResponse(
        request.format,
        match upload {
            Some(id) => CreateCacheEntryResponse {
                ok: true,
                signed_upload_url: format!(
                    "{}/devstoreaccount1/upload/{id}",
                    state.config.api_base_url
                ),
                message: String::new(),
            },
            None => CreateCacheEntryResponse::default(),
        },
    ))
}

pub async fn finalize_cache_entry_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: TwirpRequest<FinalizeCacheEntryUploadRequest>,
) -> Result<TwirpResponse<FinalizeCacheEntryUploadResponse>, TwirpError> {
    let scopes = state.auth.token_scopes(&headers).await?;
    let FinalizeCacheEntryUploadRequest { key, version } = request.message;
    require_key_and_version(&key, &version)?;
    let scope = scopes.write_scope().ok_or_else(no_write_scope)?;

    let upload_id = state
        .storage
        .complete_upload(&key, &version, &scope.scope, &scopes.repo_id)
        .await?
        .ok_or_else(|| TwirpError(ApiError::new(StatusCode::NOT_FOUND, "Upload not found")))?;
    state.metrics.cache_uploads_total.inc();

    Ok(TwirpResponse(
        request.format,
        FinalizeCacheEntryUploadResponse {
            ok: true,
            entry_id: upload_id,
            message: String::new(),
        },
    ))
}

pub async fn get_cache_entry_download_url(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: TwirpRequest<GetCacheEntryDownloadUrlRequest>,
) -> Result<TwirpResponse<GetCacheEntryDownloadUrlResponse>, TwirpError> {
    let scopes = state.auth.token_scopes(&headers).await?;
    let GetCacheEntryDownloadUrlRequest {
        key,
        restore_keys,
        version,
    } = request.message;
    require_key_and_version(&key, &version)?;

    let read_scopes = scopes.read_scopes();
    let matched = state
        .storage
        .cache_entry_download_url(&MatchQuery {
            primary_key: &key,
            restore_keys: &restore_keys,
            version: &version,
            scopes: &read_scopes,
            repo_id: &scopes.repo_id,
        })
        .await?;
    state.metrics.record_lookup(matched.is_some());

    Ok(TwirpResponse(
        request.format,
        match matched {
            Some((url, entry)) => GetCacheEntryDownloadUrlResponse {
                ok: true,
                signed_download_url: url,
                matched_key: entry.key,
            },
            None => GetCacheEntryDownloadUrlResponse::default(),
        },
    ))
}

pub async fn download(
    State(state): State<AppState>,
    Path(cache_entry_id): Path<String>,
) -> Result<Response, ApiError> {
    let not_found = || ApiError::new(StatusCode::NOT_FOUND, "Cache file not found");
    let id = cache_entry_id.parse().map_err(|_| not_found())?;
    let download = state.storage.download(id).await?.ok_or_else(not_found)?;

    // Once headers are out, a failing stream aborts the connection, which the
    // client sees as a short read against Content-Length.
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
            (header::CONTENT_LENGTH, download.size.to_string()),
        ],
        Body::from_stream(download.stream),
    )
        .into_response())
}
