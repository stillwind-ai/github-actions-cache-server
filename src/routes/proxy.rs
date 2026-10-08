//! Results Passthrough: requests the cache server doesn't handle (artifacts,
//! other Twirp services) are forwarded to the Default Results Origin and the
//! response returned unchanged.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode, header};
use axum::response::Response;
use futures::TryStreamExt;

use super::AppState;
use crate::error::ApiError;

/// Hop-by-hop headers, which describe one connection and are not forwarded.
const HOP_BY_HOP: [HeaderName; 8] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

fn forwardable(headers: &HeaderMap) -> HeaderMap {
    let mut headers = headers.clone();
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
    headers
}

pub async fn results_passthrough(
    State(state): State<AppState>,
    request: Request,
) -> Result<Response, ApiError> {
    let path = request
        .uri()
        .path_and_query()
        .map_or("/", axum::http::uri::PathAndQuery::as_str);
    let url = format!("{}{path}", state.config.default_actions_results_url);
    tracing::debug!(method = %request.method(), url, "Proxying unknown path");

    let mut headers = forwardable(request.headers());
    headers.remove(header::HOST);
    let upstream = state
        .http
        .request(request.method().clone(), &url)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(
            request.into_body().into_data_stream(),
        ))
        .send()
        .await
        .map_err(|err| {
            tracing::warn!(url, error = %err, "Results Passthrough failed");
            ApiError::new(StatusCode::BAD_GATEWAY, "Results origin unreachable")
        })?;

    let mut response = Response::builder().status(upstream.status());
    *response.headers_mut().expect("fresh builder") = forwardable(upstream.headers());
    Ok(response
        .body(Body::from_stream(
            upstream.bytes_stream().map_err(std::io::Error::other),
        ))
        .expect("valid response"))
}
