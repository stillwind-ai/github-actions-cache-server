//! Test harness: a real server on a random port, backed by a fresh Postgres
//! database (from `TEST_DATABASE_URL`) and fresh storage: a temporary
//! directory, or with `TEST_STORAGE_DRIVER=s3` a new bucket on the S3 server
//! at `TEST_S3_ENDPOINT`.

#![allow(dead_code)]

use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use cache_server::config::S3Settings;
use cache_server::storage::backend::{Backend, StorageFolder};
use cache_server::{App, AppState, Config};
use sea_orm::{ConnectionTrait, Database};
use serde_json::{Value, json};

pub const DEFAULT_DATABASE_URL: &str = "postgres://postgres@127.0.0.1:5432/postgres";
pub const DEFAULT_S3_ENDPOINT: &str = "http://127.0.0.1:9000";

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
pub fn token(scopes: Value, repository_id: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD
        .encode(json!({ "ac": scopes.to_string(), "repository_id": repository_id }).to_string());
    format!("{header}.{payload}.signature")
}

pub fn main_token() -> String {
    token(
        json!([{ "Scope": "refs/heads/main", "Permission": 3 }]),
        "123",
    )
}

pub fn testing_s3() -> bool {
    std::env::var("TEST_STORAGE_DRIVER").is_ok_and(|driver| driver == "s3")
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}

/// Creates a bucket for one test, waiting for the S3 server to come up.
async fn create_bucket() -> S3Settings {
    let settings = S3Settings {
        bucket: format!("test-{}", uuid::Uuid::new_v4().simple()),
        region: "us-east-1".into(),
        endpoint_url: Some(env_or("TEST_S3_ENDPOINT", DEFAULT_S3_ENDPOINT)),
        access_key_id: Some(env_or("TEST_S3_ACCESS_KEY_ID", "access_key")),
        secret_access_key: Some(env_or("TEST_S3_SECRET_ACCESS_KEY", "secret_key")),
        session_token: None,
        force_path_style: true,
        socket_timeout: std::time::Duration::from_secs(30),
    };
    let client = cache_server::storage::s3::client(&settings).await;
    let mut attempts = 0;
    loop {
        match client.create_bucket().bucket(&settings.bucket).send().await {
            Ok(_) => return settings,
            Err(err) if attempts < 60 => {
                attempts += 1;
                tracing::debug!(error = %err, "S3 server not ready");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(err) => panic!(
                "create a bucket on TEST_S3_ENDPOINT: {}",
                aws_sdk_s3::error::DisplayErrorContext(err)
            ),
        }
    }
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
    if testing_s3() {
        let s3 = create_bucket().await;
        for (key, value) in [
            ("STORAGE_DRIVER", "s3".to_owned()),
            ("STORAGE_S3_BUCKET", s3.bucket),
            ("AWS_REGION", s3.region),
            ("AWS_ENDPOINT_URL", s3.endpoint_url.unwrap()),
            ("AWS_ACCESS_KEY_ID", s3.access_key_id.unwrap()),
            ("AWS_SECRET_ACCESS_KEY", s3.secret_access_key.unwrap()),
        ] {
            vars.insert(key.into(), value);
        }
    }
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
    pub fn backend(&self) -> &Backend {
        self.state.storage.backend()
    }

    pub fn is_s3(&self) -> bool {
        matches!(self.backend(), Backend::S3(_))
    }

    /// The storage directory; only for filesystem storage.
    pub fn storage_path(&self) -> std::path::PathBuf {
        match self.backend() {
            Backend::Filesystem(fs) => fs.root().to_path_buf(),
            Backend::S3(_) => panic!("S3 storage has no storage path"),
        }
    }

    pub async fn object_exists(&self, name: &str) -> bool {
        self.backend().exists(name).await.unwrap()
    }

    /// An object's contents, `None` if it doesn't exist.
    pub async fn read_object(&self, name: &str) -> Option<Vec<u8>> {
        use futures::TryStreamExt;
        match self.backend().read(name).await {
            Ok(stream) => {
                let chunks: Vec<_> = stream.try_collect().await.unwrap();
                Some(chunks.concat())
            }
            Err(cache_server::storage::backend::StorageError::NotFound(_)) => None,
            Err(err) => panic!("read {name}: {err}"),
        }
    }

    pub async fn put_object(&self, name: &str, data: &[u8]) {
        let data = Bytes::copy_from_slice(data);
        self.backend()
            .write(name, futures::stream::iter([Ok(data)]), None)
            .await
            .unwrap();
    }

    /// External mutation: removes a whole folder behind the server's back.
    pub async fn remove_folder(&self, folder: &str) {
        self.backend().delete_folder(folder).await.unwrap();
    }

    /// External mutation: removes one object behind the server's back.
    pub async fn remove_object(&self, name: &str) {
        match self.backend() {
            Backend::Filesystem(fs) => std::fs::remove_file(fs.root().join(name)).unwrap(),
            Backend::S3(s3) => {
                s3.client()
                    .delete_object()
                    .bucket(s3.bucket())
                    .key(s3.key(name).unwrap())
                    .send()
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn storage_folders(&self) -> Vec<StorageFolder> {
        self.backend().list_storage_folders().await.unwrap()
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
