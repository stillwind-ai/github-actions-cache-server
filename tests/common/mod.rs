//! Test harness: a real server on a random port, backed by a fresh Postgres
//! database (from `TEST_DATABASE_URL`) and a temporary storage directory.

#![allow(dead_code)]

use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use cache_server::{App, AppState, Config};
use sea_orm::{ConnectionTrait, Database};
use serde_json::{Value, json};

pub const DEFAULT_DATABASE_URL: &str = "postgres://postgres@127.0.0.1:5432/postgres";

pub struct TestServer {
    pub url: String,
    pub state: AppState,
    pub client: reqwest::Client,
    pub token: String,
    pub storage_dir: tempfile::TempDir,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// An unsigned runtime token; the test server skips signature validation.
pub fn token(scopes: &Value, repository_id: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD
        .encode(json!({ "ac": scopes.to_string(), "repository_id": repository_id }).to_string());
    format!("{header}.{payload}.signature")
}

pub fn main_token() -> String {
    token(
        &json!([{ "Scope": "refs/heads/main", "Permission": 3 }]),
        "123",
    )
}

async fn create_database() -> String {
    let admin_url =
        std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.into());
    let admin = Database::connect(&admin_url).await.expect(
        "connect to TEST_DATABASE_URL (a Postgres server the tests may create databases on)",
    );
    let name = format!("cache_server_test_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!(r#"CREATE DATABASE "{name}""#))
        .await
        .unwrap();
    admin.close().await.unwrap();
    let mut url = url::Url::parse(&admin_url).unwrap();
    url.set_path(&name);
    url.into()
}

pub async fn start() -> TestServer {
    start_with(&[]).await
}

pub async fn start_with(overrides: &[(&str, &str)]) -> TestServer {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,sea_orm_migration=error".into()),
        )
        .with_test_writer()
        .try_init();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let storage_dir = tempfile::tempdir().unwrap();

    let mut vars: HashMap<String, String> = [
        ("API_BASE_URL", url.as_str()),
        ("SKIP_TOKEN_VALIDATION", "true"),
        ("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "100"),
        ("MANAGEMENT_API_KEY", "secret"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect();
    vars.insert("DB_POSTGRES_URL".into(), create_database().await);
    vars.insert(
        "STORAGE_FILESYSTEM_PATH".into(),
        storage_dir.path().join("storage").display().to_string(),
    );
    // Lets CI run the suite against both the io_uring and tokio::fs paths.
    if let Ok(io_uring) = std::env::var("STORAGE_FILESYSTEM_IO_URING") {
        vars.insert("STORAGE_FILESYSTEM_IO_URING".into(), io_uring);
    }
    for (key, value) in overrides {
        vars.insert((*key).into(), (*value).into());
    }

    let app = App::new(Config::from_vars(vars).unwrap()).await.unwrap();
    let state = app.state.clone();
    // The cleanup scheduler is not started; tests run tasks explicitly.
    let router = app.router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    TestServer {
        url,
        state,
        client: reqwest::Client::new(),
        token: main_token(),
        storage_dir,
        server,
    }
}

const CACHE_SERVICE: &str = "twirp/github.actions.results.api.v1.CacheService";

/// Block ids as `@actions/cache` (Azure SDK) generates them: a UUID followed
/// by the zero-padded block index, base64 encoded.
pub fn block_id(index: usize) -> String {
    STANDARD.encode(format!("{}{index:012}", uuid::Uuid::new_v4()))
}

impl TestServer {
    pub fn storage_path(&self) -> std::path::PathBuf {
        self.state.storage.fs().root().to_path_buf()
    }

    pub async fn twirp(&self, method: &str, body: Value) -> reqwest::Response {
        self.twirp_as(&self.token, method, body).await
    }

    pub async fn twirp_as(&self, token: &str, method: &str, body: Value) -> reqwest::Response {
        self.client
            .post(format!("{}/{CACHE_SERVICE}/{method}", self.url))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    /// `CreateCacheEntry`; the signed upload URL, or `None` for `ok: false`.
    pub async fn create_entry(&self, key: &str, version: &str) -> Option<String> {
        let response = self
            .twirp(
                "CreateCacheEntry",
                json!({ "key": key, "version": version }),
            )
            .await;
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        body["ok"]
            .as_bool()
            .unwrap()
            .then(|| body["signed_upload_url"].as_str().unwrap().to_owned())
    }

    pub async fn upload_blocks(&self, upload_url: &str, data: &[u8], block_size: usize) {
        for (index, block) in data.chunks(block_size.max(1)).enumerate() {
            let response = self
                .client
                .put(upload_url)
                .query(&[("comp", "block"), ("blockid", &block_id(index))])
                .body(block.to_vec())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 201);
            assert!(response.headers().contains_key("x-ms-request-id"));
        }
        let response = self
            .client
            .put(upload_url)
            .query(&[("comp", "blocklist")])
            .body("<BlockList/>")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
    }

    pub async fn finalize(&self, key: &str, version: &str, size: usize) -> reqwest::Response {
        self.twirp(
            "FinalizeCacheEntryUpload",
            json!({ "key": key, "version": version, "size_bytes": size.to_string() }),
        )
        .await
    }

    /// Saves a cache entry the way `@actions/cache` does.
    pub async fn save(&self, key: &str, version: &str, data: &[u8], block_size: usize) {
        let upload_url = self
            .create_entry(key, version)
            .await
            .expect("upload created");
        self.upload_blocks(&upload_url, data, block_size).await;
        let response = self.finalize(key, version, data.len()).await;
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    }

    /// `GetCacheEntryDownloadURL`: `(url, matched_key)` on a hit.
    pub async fn lookup(
        &self,
        key: &str,
        restore_keys: &[&str],
        version: &str,
    ) -> Option<(String, String)> {
        let response = self
            .twirp(
                "GetCacheEntryDownloadURL",
                json!({ "key": key, "restore_keys": restore_keys, "version": version }),
            )
            .await;
        assert_eq!(response.status(), 200);
        let body: Value = response.json().await.unwrap();
        body["ok"].as_bool().unwrap().then(|| {
            (
                body["signed_download_url"].as_str().unwrap().to_owned(),
                body["matched_key"].as_str().unwrap().to_owned(),
            )
        })
    }

    pub async fn download(&self, url: &str) -> (u16, Vec<u8>) {
        let response = self.client.get(url).send().await.unwrap();
        let status = response.status().as_u16();
        (status, response.bytes().await.unwrap().to_vec())
    }

    /// Looks up and downloads `key`, asserting a hit.
    pub async fn restore(&self, key: &str, version: &str) -> Vec<u8> {
        let (url, matched) = self.lookup(key, &[], version).await.expect("cache hit");
        assert_eq!(matched, key);
        let (status, body) = self.download(&url).await;
        assert_eq!(status, 200);
        body
    }

    /// Waits for background merges started so far.
    pub async fn wait_for_merges(&self) {
        self.state.storage.wait_for_ongoing_merges().await;
    }
}

pub fn random_bytes(len: usize) -> Vec<u8> {
    let mut data = vec![0u8; len];
    rand::fill(&mut data[..]);
    data
}
