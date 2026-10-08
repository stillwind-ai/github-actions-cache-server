//! The Azure Blob Storage "Put Block" subset the cache clients upload with.

use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use futures::TryStreamExt;

use super::AppState;
use crate::error::ApiError;

fn response() -> Response {
    let mut response = StatusCode::CREATED.into_response();
    // Prevents a random EOF error in tonistiigi/go-actions-cache.
    response.headers_mut().insert(
        "x-ms-request-id",
        HeaderValue::from_str(&uuid::Uuid::new_v4().to_string()).expect("valid header"),
    );
    response
}

pub async fn upload(
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Result<Response, ApiError> {
    let upload_id: i64 = upload_id.parse().map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("Invalid upload id: {upload_id}"),
        )
    })?;

    let query: Vec<(String, String)> =
        url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let param = |name: &str| {
        query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };

    // Put Block List: the parts are already stored.
    if param("comp") == Some("blocklist") {
        return Ok(response());
    }

    // Without a block id, the whole payload is a single Put Blob.
    let index = match param("blockid") {
        Some(block_id) => chunk_index(block_id).ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("Invalid block id: {block_id}"),
            )
        })?,
        None => 0,
    };

    let stream = body.into_data_stream().map_err(std::io::Error::other);
    if !state.storage.upload_part(upload_id, index, stream).await? {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "Upload not found"));
    }
    Ok(response())
}

/// Decodes the part index from an Azure block id: 64 bytes from docker buildx
/// (big-endian u32 at offset 16), 48 bytes from everything else (a UUID
/// followed by the zero-padded decimal index).
fn chunk_index(block_id: &str) -> Option<u32> {
    let config =
        GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent);
    let decoded = GeneralPurpose::new(&base64::alphabet::STANDARD, config)
        .decode(block_id)
        .or_else(|_| GeneralPurpose::new(&base64::alphabet::URL_SAFE, config).decode(block_id))
        .ok()?;
    match decoded.len() {
        64 => Some(u32::from_be_bytes(decoded[16..20].try_into().ok()?)),
        48 => {
            let index = std::str::from_utf8(decoded.get(36..)?).ok()?.trim_start();
            let digits = index
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(index.len());
            index[..digits].parse().ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_actions_cache_block_ids() {
        let id = format!("{}{:012}", uuid::Uuid::nil(), 7);
        let encoded = base64::engine::general_purpose::STANDARD.encode(id);
        assert_eq!(chunk_index(&encoded), Some(7));
    }

    #[test]
    fn decodes_buildx_block_ids() {
        let mut id = [0u8; 64];
        id[16..20].copy_from_slice(&42u32.to_be_bytes());
        let encoded = base64::engine::general_purpose::STANDARD.encode(id);
        assert_eq!(chunk_index(&encoded), Some(42));
    }

    #[test]
    fn rejects_other_block_ids() {
        assert_eq!(chunk_index("not base64!"), None);
        assert_eq!(
            chunk_index(&base64::engine::general_purpose::STANDARD.encode("short")),
            None
        );
        let id = format!("{}{}", uuid::Uuid::nil(), "x".repeat(12));
        assert_eq!(
            chunk_index(&base64::engine::general_purpose::STANDARD.encode(id)),
            None
        );
    }
}
