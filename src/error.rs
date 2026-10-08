use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// An HTTP error with a client-facing message. Server-side causes are logged
/// where they are converted, never sent to the client.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn internal(err: impl std::fmt::Display) -> Self {
        tracing::error!(error = %err, "Request failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "statusCode": self.status.as_u16(),
            "message": self.message,
        });
        (self.status, Json(body)).into_response()
    }
}

impl From<crate::storage::Error> for ApiError {
    fn from(err: crate::storage::Error) -> Self {
        match err {
            crate::storage::Error::UploadRejected(reason) => {
                Self::new(StatusCode::BAD_REQUEST, reason)
            }
            err => Self::internal(err),
        }
    }
}

impl From<sea_orm::DbErr> for ApiError {
    fn from(err: sea_orm::DbErr) -> Self {
        Self::internal(err)
    }
}
