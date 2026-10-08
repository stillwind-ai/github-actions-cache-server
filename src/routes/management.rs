//! The management API: the same REST routes and JSON shapes the original
//! oRPC/OpenAPI handler served under `/management-api`.

// Handlers short-circuit with ready-made responses.
#![allow(clippy::result_large_err)]

use axum::Json;
use axum::Router;
use axum::extract::{Path, RawQuery, Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use sea_orm::{
    ColumnTrait, Condition, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
};
use serde::Serialize;
use uuid::Uuid;

use super::AppState;
use crate::cleanup::Task;
use crate::entity::{cache_entry, storage_location};
use crate::storage::{MatchQuery, MatchType};

pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route(
            "/cache-entries",
            get(find_cache_entries).delete(delete_cache_entries),
        )
        .route("/cache-entries/match", get(match_cache_entry))
        .route(
            "/cache-entries/{id}",
            get(get_cache_entry).delete(delete_cache_entry),
        )
        .route(
            "/storage-locations/{id}",
            get(get_storage_location).delete(delete_storage_location),
        )
        .fallback(|| async { error(StatusCode::NOT_FOUND, "NOT_FOUND", "Not found") })
        .layer(middleware::from_fn_with_state(state, require_api_key))
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
                .allow_methods([Method::GET, Method::DELETE]),
        )
}

/// Errors in oRPC's shape.
fn error(status: StatusCode, code: &str, message: &str) -> Response {
    let body = serde_json::json!({
        "defined": false,
        "code": code,
        "status": status.as_u16(),
        "message": message,
    });
    (status, Json(body)).into_response()
}

fn internal(err: impl std::fmt::Display) -> Response {
    tracing::error!(error = %err, "Management API request failed");
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_SERVER_ERROR",
        "Internal server error",
    )
}

fn bad_request(message: &str) -> Response {
    error(StatusCode::BAD_REQUEST, "BAD_REQUEST", message)
}

async fn require_api_key(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let Some(expected) = state.config.management_api_key.as_deref() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "SERVICE_UNAVAILABLE",
            "Management API is disabled",
        );
    };
    let provided = request
        .headers()
        .get("x-api-key")
        .map(|value| value.as_bytes());
    // Constant-time comparison: the key is the API's only credential.
    let authorized = provided.is_some_and(|provided| {
        provided.len() == expected.len()
            && provided
                .iter()
                .zip(expected.as_bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
    });
    if !authorized {
        return error(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "Unauthorized");
    }
    next.run(request).await
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CacheEntryJson {
    id: Uuid,
    key: String,
    version: String,
    scope: String,
    repo_id: String,
    /// Milliseconds since the epoch.
    updated_at: i64,
    location_id: Uuid,
}

impl From<cache_entry::Model> for CacheEntryJson {
    fn from(entry: cache_entry::Model) -> Self {
        Self {
            id: entry.id,
            key: entry.key,
            version: entry.version,
            scope: entry.scope,
            repo_id: entry.repo_id,
            updated_at: entry.updated_at.timestamp_millis(),
            location_id: entry.location_id,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StorageLocationJson {
    id: Uuid,
    folder_name: String,
    part_count: i32,
    merge_started_at: Option<i64>,
    merged_at: Option<i64>,
    parts_deleted_at: Option<i64>,
    last_downloaded_at: Option<i64>,
    size_bytes: i64,
}

impl From<storage_location::Model> for StorageLocationJson {
    fn from(location: storage_location::Model) -> Self {
        let millis = |at: Option<chrono::DateTime<chrono::Utc>>| at.map(|at| at.timestamp_millis());
        Self {
            id: location.id,
            folder_name: location.folder_name,
            part_count: location.part_count,
            merge_started_at: millis(location.merge_started_at),
            merged_at: millis(location.merged_at),
            parts_deleted_at: millis(location.parts_deleted_at),
            last_downloaded_at: millis(location.last_downloaded_at),
            size_bytes: location.size_bytes,
        }
    }
}

/// Query parameters; repeated keys are kept, for array inputs.
struct Params(Vec<(String, String)>);

impl Params {
    fn parse(query: Option<String>) -> Self {
        Self(
            url::form_urlencoded::parse(query.unwrap_or_default().as_bytes())
                .into_owned()
                .collect(),
        )
    }

    fn one(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn all(&self, name: &str) -> Vec<String> {
        self.0
            .iter()
            .filter(|(key, _)| key == name || key.strip_suffix("[]") == Some(name))
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn integer(
        &self,
        name: &str,
        default: u64,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<u64, Response> {
        match self.one(name) {
            None => Ok(default),
            Some(value) => value
                .parse()
                .ok()
                .filter(|value| range.contains(value))
                .ok_or_else(|| bad_request(&format!("Invalid {name}"))),
        }
    }

    /// Exact-match filters shared by listing and bulk deletion.
    fn filters(&self) -> Condition {
        let mut condition = Condition::all();
        for (name, column) in [
            ("key", cache_entry::Column::Key),
            ("version", cache_entry::Column::Version),
            ("scope", cache_entry::Column::Scope),
            ("repoId", cache_entry::Column::RepoId),
        ] {
            if let Some(value) = self.one(name).filter(|value| !value.is_empty()) {
                condition = condition.add(column.eq(value));
            }
        }
        condition
    }
}

fn parse_id(id: &str, not_found: &str) -> Result<Uuid, Response> {
    id.parse()
        .map_err(|_| error(StatusCode::NOT_FOUND, "NOT_FOUND", not_found))
}

async fn get_cache_entry(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    const NOT_FOUND: &str = "Cache entry not found";
    let id = match parse_id(&id, NOT_FOUND) {
        Ok(id) => id,
        Err(response) => return response,
    };
    match cache_entry::Entity::find_by_id(id)
        .one(state.storage.db())
        .await
    {
        Ok(Some(entry)) => Json(CacheEntryJson::from(entry)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "NOT_FOUND", NOT_FOUND),
        Err(err) => internal(err),
    }
}

async fn match_cache_entry(State(state): State<AppState>, RawQuery(query): RawQuery) -> Response {
    let params = Params::parse(query);
    let (Some(primary_key), Some(repo_id), Some(version)) = (
        params.one("primaryKey"),
        params.one("repoId"),
        params.one("version"),
    ) else {
        return bad_request("primaryKey, repoId and version are required");
    };
    let scopes = params.all("scopes");
    if scopes.is_empty() {
        return bad_request("scopes is required");
    }
    let restore_keys = params.all("restoreKeys");

    #[derive(Serialize)]
    struct Matched {
        #[serde(rename = "match")]
        entry: CacheEntryJson,
        #[serde(rename = "type")]
        match_type: MatchType,
    }

    let matched = state
        .storage
        .match_cache_entry(&MatchQuery {
            primary_key,
            restore_keys: &restore_keys,
            version,
            scopes: &scopes,
            repo_id,
        })
        .await;
    match matched {
        Ok(matched) => Json(matched.map(|matched| Matched {
            entry: matched.entry.into(),
            match_type: matched.match_type,
        }))
        .into_response(),
        Err(err) => internal(err),
    }
}

async fn find_cache_entries(State(state): State<AppState>, RawQuery(query): RawQuery) -> Response {
    let params = Params::parse(query);
    let (items_per_page, page) = match (
        params.integer("itemsPerPage", 20, 1..=100),
        params.integer("page", 1, 1..=u32::MAX as u64),
    ) {
        (Ok(items_per_page), Ok(page)) => (items_per_page, page),
        (Err(response), _) | (_, Err(response)) => return response,
    };

    let query = cache_entry::Entity::find().filter(params.filters());
    let db = state.storage.db();
    let total = match query.clone().count(db).await {
        Ok(total) => total,
        Err(err) => return internal(err),
    };
    let items = query
        .order_by_desc(cache_entry::Column::UpdatedAt)
        .order_by_asc(cache_entry::Column::Id)
        .limit(items_per_page)
        .offset((page - 1) * items_per_page)
        .all(db)
        .await;
    match items {
        Ok(items) => Json(serde_json::json!({
            "total": total,
            "items": items.into_iter().map(CacheEntryJson::from).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(err) => internal(err),
    }
}

async fn delete_cache_entry(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let Ok(id) = id.parse::<Uuid>() else {
        return StatusCode::OK.into_response();
    };
    if let Err(err) = cache_entry::Entity::delete_by_id(id)
        .exec(state.storage.db())
        .await
    {
        return internal(err);
    }
    state.cleanup.trigger(Task::StorageLocations);
    StatusCode::OK.into_response()
}

async fn delete_cache_entries(
    State(state): State<AppState>,
    RawQuery(query): RawQuery,
) -> Response {
    let params = Params::parse(query);
    let deleted = cache_entry::Entity::delete_many()
        .filter(params.filters())
        .exec(state.storage.db())
        .await;
    if let Err(err) = deleted {
        return internal(err);
    }
    state.cleanup.trigger(Task::StorageLocations);
    StatusCode::OK.into_response()
}

async fn get_storage_location(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    const NOT_FOUND: &str = "Storage location not found";
    let id = match parse_id(&id, NOT_FOUND) {
        Ok(id) => id,
        Err(response) => return response,
    };
    match storage_location::Entity::find_by_id(id)
        .one(state.storage.db())
        .await
    {
        Ok(Some(location)) => Json(StorageLocationJson::from(location)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "NOT_FOUND", NOT_FOUND),
        Err(err) => internal(err),
    }
}

/// Deletes a Storage Location (and its Cache Entries) and then its folder.
async fn delete_storage_location(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<Uuid>() else {
        return StatusCode::OK.into_response();
    };
    let db = state.storage.db();
    let folder = storage_location::Entity::find_by_id(id)
        .select_only()
        .column(storage_location::Column::FolderName)
        .into_tuple::<String>()
        .one(db)
        .await;
    let folder = match folder {
        Ok(Some(folder)) => folder,
        Ok(None) => return StatusCode::OK.into_response(),
        Err(err) => return internal(err),
    };
    if let Err(err) = storage_location::Entity::delete_by_id(id).exec(db).await {
        return internal(err);
    }
    if let Err(err) = state.storage.backend().delete_folder(&folder).await {
        return internal(err);
    }
    StatusCode::OK.into_response()
}
