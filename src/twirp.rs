//! The Twirp `CacheService` wire format. Twirp negotiates JSON or protobuf via
//! Content-Type, and a protobuf request must get a protobuf response.
//! Anything not explicitly protobuf is JSON, which is what the runner's
//! `@actions/cache` client sends.

use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;

const PROTOBUF_CONTENT_TYPE: &str = "application/protobuf";
const MAX_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    Protobuf,
}

// Only the fields the server reads or writes are declared; both decoders skip
// unknown fields such as `metadata`.

#[derive(Clone, PartialEq, prost::Message, Deserialize)]
pub struct CreateCacheEntryRequest {
    #[prost(string, tag = "2")]
    #[serde(default)]
    pub key: String,
    #[prost(string, tag = "3")]
    #[serde(default)]
    pub version: String,
}

#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct CreateCacheEntryResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub signed_upload_url: String,
    #[prost(string, tag = "3")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[derive(Clone, PartialEq, prost::Message, Deserialize)]
pub struct FinalizeCacheEntryUploadRequest {
    #[prost(string, tag = "2")]
    #[serde(default)]
    pub key: String,
    #[prost(string, tag = "4")]
    #[serde(default)]
    pub version: String,
}

#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct FinalizeCacheEntryUploadResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    /// Protobuf JSON encodes int64 as a string.
    #[prost(int64, tag = "2")]
    #[serde(serialize_with = "int64_as_string")]
    pub entry_id: i64,
    #[prost(string, tag = "3")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[derive(Clone, PartialEq, prost::Message, Deserialize)]
pub struct GetCacheEntryDownloadUrlRequest {
    #[prost(string, tag = "2")]
    #[serde(default)]
    pub key: String,
    #[prost(string, repeated, tag = "3")]
    #[serde(default, alias = "restoreKeys", deserialize_with = "null_as_empty")]
    pub restore_keys: Vec<String>,
    #[prost(string, tag = "4")]
    #[serde(default)]
    pub version: String,
}

#[derive(Clone, PartialEq, prost::Message, Serialize)]
pub struct GetCacheEntryDownloadUrlResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub signed_download_url: String,
    #[prost(string, tag = "3")]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub matched_key: String,
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's `serialize_with` passes a reference"
)]
fn int64_as_string<S: serde::Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

fn null_as_empty<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(deserializer)?.unwrap_or_default())
}

/// Extracts a Twirp request body in either wire format.
pub struct TwirpRequest<T> {
    pub format: Format,
    pub message: T,
}

impl<S, T> FromRequest<S> for TwirpRequest<T>
where
    S: Send + Sync,
    T: prost::Message + Default + for<'de> Deserialize<'de>,
{
    type Rejection = TwirpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let format = if req
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains(PROTOBUF_CONTENT_TYPE))
        {
            Format::Protobuf
        } else {
            Format::Json
        };
        let invalid = |message: String| {
            TwirpError(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("Invalid body: {message}"),
            ))
        };

        let req = req.map(|body| {
            axum::body::Body::new(http_body_util::Limited::new(body, MAX_REQUEST_BYTES))
        });
        let body = Bytes::from_request(req, state)
            .await
            .map_err(|err| invalid(err.body_text()))?;
        let message = match format {
            Format::Protobuf => T::decode(body).map_err(|err| invalid(err.to_string()))?,
            Format::Json if body.is_empty() => T::default(),
            Format::Json => {
                serde_json::from_slice(&body).map_err(|err| invalid(err.to_string()))?
            }
        };
        Ok(Self { format, message })
    }
}

/// A Twirp response in the request's wire format.
pub struct TwirpResponse<T>(pub Format, pub T);

impl<T: prost::Message + Serialize> IntoResponse for TwirpResponse<T> {
    fn into_response(self) -> Response {
        match self.0 {
            Format::Json => axum::Json(self.1).into_response(),
            Format::Protobuf => (
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(PROTOBUF_CONTENT_TYPE),
                )],
                self.1.encode_to_vec(),
            )
                .into_response(),
        }
    }
}

/// Errors in Twirp's JSON error shape, `{"code": ..., "msg": ...}`.
#[derive(Debug)]
pub struct TwirpError(pub ApiError);

impl<E: Into<ApiError>> From<E> for TwirpError {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

impl IntoResponse for TwirpError {
    fn into_response(self) -> Response {
        let code = match self.0.status {
            StatusCode::BAD_REQUEST => "invalid_argument",
            StatusCode::UNAUTHORIZED => "unauthenticated",
            StatusCode::FORBIDDEN => "permission_denied",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::CONFLICT => "already_exists",
            _ => "internal",
        };
        let body = serde_json::json!({ "code": code, "msg": self.0.message });
        (self.0.status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_requests_accept_null_restore_keys_and_unknown_fields() {
        let request: GetCacheEntryDownloadUrlRequest = serde_json::from_str(
            r#"{"metadata":{"repository_id":"1"},"key":"k","restore_keys":null,"version":"v"}"#,
        )
        .unwrap();
        assert_eq!((request.key.as_str(), request.restore_keys.len()), ("k", 0));
    }

    #[test]
    fn json_responses_omit_empty_fields_and_stringify_int64() {
        let miss = serde_json::to_string(&GetCacheEntryDownloadUrlResponse {
            ok: false,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(miss, r#"{"ok":false}"#);
        let finalized = serde_json::to_string(&FinalizeCacheEntryUploadResponse {
            ok: true,
            entry_id: 42,
            message: String::new(),
        })
        .unwrap();
        assert_eq!(finalized, r#"{"ok":true,"entry_id":"42"}"#);
    }
}
