//! Emulators of the real cache clients' wire behaviour: request order, body
//! sizes, block sizes, connection reuse and concurrency, as read from their
//! source and checked against captured traffic (see `benches/README.md`).
//! Payloads are incompressible bytes, like the zstd and gzip archives and
//! layers the real clients send.

use std::sync::Arc;
use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use cache_server::twirp::{
    CreateCacheEntryRequest, CreateCacheEntryResponse, FinalizeCacheEntryUploadRequest,
    GetCacheEntryDownloadUrlRequest, GetCacheEntryDownloadUrlResponse,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use prost::Message;
use serde_json::{Value, json};

use crate::stats::Stats;

pub const KIB: u64 = 1024;
pub const MIB: u64 = 1024 * KIB;
const CACHE_SERVICE: &str = "twirp/github.actions.results.api.v1.CacheService";
/// Largest body any emulated request sends: `@actions/cache`'s single-shot
/// Put Blob limit.
const MAX_BODY: u64 = 128 * MIB;

type Result<T> = std::result::Result<T, String>;

fn error(err: impl std::fmt::Display) -> String {
    err.to_string()
}

/// What every emulator shares: the server, its token, the payload bytes and
/// two HTTP clients, since real clients differ in connection reuse.
pub struct Ctx {
    base: String,
    token: String,
    pub stats: Arc<Stats>,
    payload: Bytes,
    keep_alive: reqwest::Client,
    /// A new connection per request, like `@actions/http-client`'s default.
    no_keep_alive: reqwest::Client,
}

impl Ctx {
    pub fn new(base: &str, payload: Bytes) -> Self {
        assert!(payload.len() as u64 >= MAX_BODY);
        let claims = json!({
            "ac": json!([{ "Scope": "refs/heads/main", "Permission": 3 }]).to_string(),
            "repository_id": "1",
        });
        Self {
            base: base.to_owned(),
            token: format!(
                "{}.{}.signature",
                URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#),
                URL_SAFE_NO_PAD.encode(claims.to_string())
            ),
            stats: Arc::new(Stats::default()),
            payload,
            keep_alive: reqwest::Client::builder()
                .pool_max_idle_per_host(usize::MAX)
                .build()
                .unwrap(),
            no_keep_alive: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .unwrap(),
        }
    }

    fn body(&self, len: u64) -> Bytes {
        self.payload.slice(..len as usize)
    }

    /// Runs `f`, recording its latency under `op`, or its failure.
    async fn timed<T>(&self, op: &'static str, f: impl Future<Output = Result<T>>) -> Option<T> {
        let started = Instant::now();
        match f.await {
            Ok(value) => {
                self.stats.record(op, started.elapsed());
                Some(value)
            }
            Err(err) => {
                self.stats.fail(op, err);
                None
            }
        }
    }

    async fn twirp_json(
        &self,
        client: &reqwest::Client,
        method: &str,
        body: Value,
    ) -> Result<Value> {
        let response = client
            .post(format!("{}/{CACHE_SERVICE}/{method}", self.base))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!(
                "{method}: {status} {}",
                response.text().await.unwrap_or_default()
            ));
        }
        response.json().await.map_err(error)
    }

    async fn twirp_protobuf<R: Message + Default>(
        &self,
        client: &reqwest::Client,
        method: &str,
        request: impl Message,
    ) -> Result<R> {
        let response = client
            .post(format!("{}/{CACHE_SERVICE}/{method}", self.base))
            .bearer_auth(&self.token)
            .header("content-type", "application/protobuf")
            .body(request.encode_to_vec())
            .send()
            .await
            .map_err(error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("{method}: {status}"));
        }
        R::decode(response.bytes().await.map_err(error)?).map_err(error)
    }

    /// A full GET, checked against Content-Length, tracking the longest
    /// stall between body chunks.
    async fn download(&self, client: &reqwest::Client, url: &str) -> Result<u64> {
        let response = client.get(url).send().await.map_err(error)?;
        if response.status() != 200 {
            return Err(format!("download: {}", response.status()));
        }
        let expected = response.content_length();
        let mut last = Instant::now();
        let mut total = 0;
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(error)?;
            self.stats.body_gap(last.elapsed());
            last = Instant::now();
            total += chunk.len() as u64;
        }
        if expected.is_some_and(|expected| expected != total) {
            return Err(format!("short download: {total} of {expected:?} bytes"));
        }
        self.stats.add_downloaded(total);
        Ok(total)
    }

    async fn put(
        &self,
        client: &reqwest::Client,
        url: &str,
        query: &[(&str, &str)],
        body: Bytes,
    ) -> Result<()> {
        let len = body.len() as u64;
        let response = client
            .put(url)
            .query(query)
            .header("x-ms-version", "2026-10-06")
            .header("x-ms-blob-type", "BlockBlob")
            .body(body)
            .send()
            .await
            .map_err(error)?;
        if response.status() != 201 {
            return Err(format!("put: {}", response.status()));
        }
        self.stats.add_uploaded(len);
        Ok(())
    }

    async fn put_block_list(
        &self,
        client: &reqwest::Client,
        url: &str,
        ids: &[String],
    ) -> Result<()> {
        let blocks: String = ids
            .iter()
            .map(|id| format!("<Latest>{id}</Latest>"))
            .collect();
        let body =
            format!(r#"<?xml version="1.0" encoding="utf-8"?><BlockList>{blocks}</BlockList>"#);
        self.put(client, url, &[("comp", "blocklist")], Bytes::from(body))
            .await
    }
}

/// `@actions/cache` 4.x-6.x on the v2 service, as actions/cache, setup-node,
/// setup-go, setup-python, rust-cache and others use it.
pub struct ActionsCache<'a>(pub &'a Ctx);

impl ActionsCache<'_> {
    /// Archives up to this size are one Put Blob.
    const MAX_SINGLE_SHOT: u64 = 128 * MIB;
    const BLOCK_SIZE: u64 = 64 * MIB;
    const UPLOAD_CONCURRENCY: usize = 8;

    /// The lookup and the download both open a fresh connection. `None` on a
    /// miss or a failed download.
    pub async fn restore(&self, key: &str, restore_keys: &[String], version: &str) -> Option<u64> {
        let ctx = self.0;
        let started = Instant::now();
        let found = ctx
            .timed(
                "lookup",
                ctx.twirp_json(
                    &ctx.no_keep_alive,
                    "GetCacheEntryDownloadURL",
                    json!({ "key": key, "restore_keys": restore_keys, "version": version }),
                ),
            )
            .await?;
        if found["ok"] != true {
            return None;
        }
        let url = found["signed_download_url"].as_str()?;
        let size = ctx
            .timed("download", ctx.download(&ctx.no_keep_alive, url))
            .await?;
        ctx.stats.record("restore (hit)", started.elapsed());
        Some(size)
    }

    /// Reserve, upload, commit. False when the key was taken or a step failed.
    pub async fn save(&self, key: &str, version: &str, size: u64) -> bool {
        let ctx = self.0;
        let started = Instant::now();
        let Some(created) = ctx
            .timed(
                "create",
                ctx.twirp_json(
                    &ctx.no_keep_alive,
                    "CreateCacheEntry",
                    json!({ "key": key, "version": version }),
                ),
            )
            .await
        else {
            return false;
        };
        if created["ok"] != true {
            return false;
        }
        let url = created["signed_upload_url"].as_str().unwrap().to_owned();
        // The Azure SDK keeps its connections alive.
        let client = &ctx.keep_alive;
        let uploaded = if size <= Self::MAX_SINGLE_SHOT {
            ctx.timed("put blob", ctx.put(client, &url, &[], ctx.body(size)))
                .await
                .is_some()
        } else {
            let prefix = uuid::Uuid::new_v4().to_string();
            let ids: Vec<String> = (0..size.div_ceil(Self::BLOCK_SIZE))
                .map(|index| STANDARD.encode(format!("{prefix}{index:012}")))
                .collect();
            let blocks: Vec<_> = ids
                .iter()
                .enumerate()
                .map(|(index, id)| {
                    let len = (size - index as u64 * Self::BLOCK_SIZE).min(Self::BLOCK_SIZE);
                    let url = &url;
                    async move {
                        let query = [("comp", "block"), ("blockid", id.as_str())];
                        ctx.timed("put block", ctx.put(client, url, &query, ctx.body(len)))
                            .await
                    }
                })
                .collect();
            let blocks = bounded(blocks, Self::UPLOAD_CONCURRENCY)
                .await
                .iter()
                .all(Option::is_some);
            blocks
                && ctx
                    .timed("put block list", ctx.put_block_list(client, &url, &ids))
                    .await
                    .is_some()
        };
        if !uploaded {
            return false;
        }
        let finalized = ctx
            .timed(
                "finalize",
                ctx.twirp_json(
                    &ctx.no_keep_alive,
                    "FinalizeCacheEntryUpload",
                    json!({ "key": key, "version": version, "size_bytes": size.to_string() }),
                ),
            )
            .await
            .is_some_and(|finalized| finalized["ok"] == true);
        if finalized {
            ctx.stats.record("save", started.elapsed());
        }
        finalized
    }
}

#[derive(Clone, Debug)]
pub struct Layer {
    pub digest: String,
    pub size: u64,
}

/// BuildKit's `type=gha` cache (go-actions-cache on the v2 service): JSON
/// Twirp on kept-alive connections, one lookup per layer, layers exported one
/// after another in 1 MiB blocks, and a new index entry per export.
pub struct Buildkit<'a> {
    pub ctx: &'a Ctx,
    /// Every key shares one version: sha256("|go-actionscache-1.0").
    version: String,
}

impl<'a> Buildkit<'a> {
    const BLOCK_SIZE: u64 = MIB;

    pub fn new(ctx: &'a Ctx) -> Self {
        Self {
            ctx,
            version: "4f2f0f5d2be5bf6c1c1e9d7a7b6a1a5f3ce1a9a6b49e8f0a3d7c2b1e0f9d8c7b".into(),
        }
    }

    fn blob_key(layer: &Layer) -> String {
        format!("buildkit-blob-1-sha256:{}", layer.digest)
    }

    /// GetCacheEntryDownloadURL with the key as its own restore key.
    async fn load(&self, client: &reqwest::Client, key: &str) -> Option<Value> {
        let ctx = self.ctx;
        let found = ctx
            .timed(
                "lookup",
                ctx.twirp_json(
                    client,
                    "GetCacheEntryDownloadURL",
                    json!({ "key": key, "restore_keys": [key], "version": self.version }),
                ),
            )
            .await?;
        (found["ok"] == true).then_some(found)
    }

    async fn save(&self, key: &str, size: u64) -> bool {
        let ctx = self.ctx;
        let client = &ctx.keep_alive;
        let Some(created) = ctx
            .timed(
                "create",
                ctx.twirp_json(
                    client,
                    "CreateCacheEntry",
                    json!({ "key": key, "version": self.version }),
                ),
            )
            .await
        else {
            return false;
        };
        if created["ok"] != true {
            // BuildKit fails the export on anything but a 409.
            ctx.stats.fail("create", "ok:false (export fails)");
            return false;
        }
        let url = created["signed_upload_url"].as_str().unwrap().to_owned();
        let uploaded = if size < Self::BLOCK_SIZE {
            ctx.timed("put blob", ctx.put(client, &url, &[], ctx.body(size)))
                .await
                .is_some()
        } else {
            let prefix = uuid::Uuid::new_v4();
            let mut ids = Vec::new();
            let mut offset = 0;
            while offset < size {
                let mut id = [0u8; 64];
                id[..16].copy_from_slice(prefix.as_bytes());
                id[16..20].copy_from_slice(&(ids.len() as u32).to_be_bytes());
                let id = STANDARD.encode(id);
                let len = (size - offset).min(Self::BLOCK_SIZE);
                let query = [("comp", "block"), ("blockid", id.as_str())];
                let put = ctx.put(client, &url, &query, ctx.body(len));
                if ctx.timed("put block", put).await.is_none() {
                    return false;
                }
                ids.push(id);
                offset += len;
            }
            ctx.timed("put block list", ctx.put_block_list(client, &url, &ids))
                .await
                .is_some()
        };
        uploaded
            && ctx
                .timed(
                    "finalize",
                    ctx.twirp_json(
                        client,
                        "FinalizeCacheEntryUpload",
                        json!({ "key": key, "size_bytes": size, "version": self.version }),
                    ),
                )
                .await
                .is_some()
    }

    /// `--cache-to type=gha`: existence check per layer, upload of missing
    /// layers, then the next `index-…#N`. `index` is e.g.
    /// `index-buildkit-1-0123abcd`.
    pub async fn export(&self, index: &str, layers: &[Layer], index_size: u64) -> bool {
        let started = Instant::now();
        for layer in layers {
            let key = Self::blob_key(layer);
            if self.load(&self.ctx.keep_alive, &key).await.is_none()
                && !self.save(&key, layer.size).await
            {
                return false;
            }
        }
        let prefix = format!("{index}#");
        let previous = self.load(&self.ctx.keep_alive, &prefix).await;
        // Loaded again to detect a concurrent export.
        let _ = self.load(&self.ctx.keep_alive, &prefix).await;
        let next = previous
            .and_then(|found| {
                found["matched_key"]
                    .as_str()?
                    .rsplit_once('#')?
                    .1
                    .parse::<u64>()
                    .ok()
            })
            .map_or(0, |n| n + 1);
        let saved = self.save(&format!("{prefix}{next}"), index_size).await;
        if saved {
            self.ctx.stats.record("export", started.elapsed());
        }
        saved
    }

    /// `--cache-from type=gha`: the newest index, a burst of lookups for
    /// every layer in the cache chain, then downloads of the layers the build
    /// needs, four at a time. Each request opens a fresh connection.
    pub async fn import(&self, index: &str, chain: &[Layer], needed: &[Layer]) -> bool {
        const LOOKUP_BURST: usize = 11;
        const DOWNLOADS: usize = 4;
        let ctx = self.ctx;
        let client = &ctx.no_keep_alive;
        let started = Instant::now();
        let Some(found) = self.load(client, &format!("{index}#")).await else {
            return false;
        };
        let url = found["signed_download_url"].as_str().unwrap();
        if ctx
            .timed("download", ctx.download(client, url))
            .await
            .is_none()
        {
            return false;
        }
        let lookups: Vec<_> = chain
            .iter()
            .map(|layer| async move {
                let found = self.load(client, &Self::blob_key(layer)).await?;
                Some((
                    layer.digest.clone(),
                    found["signed_download_url"].as_str()?.to_owned(),
                ))
            })
            .collect();
        let Some(urls) = bounded(lookups, LOOKUP_BURST)
            .await
            .into_iter()
            .collect::<Option<std::collections::HashMap<_, _>>>()
        else {
            return false;
        };
        let downloads: Vec<_> = needed
            .iter()
            .map(|layer| {
                let url = &urls[&layer.digest];
                async move { ctx.timed("download", ctx.download(client, url)).await }
            })
            .collect();
        let complete = bounded(downloads, DOWNLOADS)
            .await
            .iter()
            .all(Option::is_some);
        if complete {
            ctx.stats.record("import", started.elapsed());
        }
        complete
    }
}

/// sccache's GHA backend (OpenDAL `ghac`): protobuf Twirp, exact keys, one
/// Put Blob per compilation output, kept-alive connections.
pub struct Sccache<'a> {
    pub ctx: &'a Ctx,
}

impl Sccache<'_> {
    const VERSION: &'static str = "sccache-v0.18.0";

    /// One compilation unit: a hit downloads the output, a miss "compiles"
    /// and writes it. Returns whether it hit.
    pub async fn compile(&self, key: &str, size: u64) -> bool {
        let ctx = self.ctx;
        let client = &ctx.keep_alive;
        let lookup = GetCacheEntryDownloadUrlRequest {
            key: key.to_owned(),
            restore_keys: Vec::new(),
            version: Self::VERSION.to_owned(),
        };
        let found: Option<GetCacheEntryDownloadUrlResponse> = ctx
            .timed(
                "lookup",
                ctx.twirp_protobuf(client, "GetCacheEntryDownloadURL", lookup),
            )
            .await;
        if let Some(found) = found.filter(|found| found.ok) {
            ctx.timed("download", ctx.download(client, &found.signed_download_url))
                .await;
            return true;
        }
        let create = CreateCacheEntryRequest {
            key: key.to_owned(),
            version: Self::VERSION.to_owned(),
        };
        let created: Option<CreateCacheEntryResponse> = ctx
            .timed(
                "create",
                ctx.twirp_protobuf(client, "CreateCacheEntry", create),
            )
            .await;
        let Some(created) = created.filter(|created| created.ok) else {
            return false;
        };
        if ctx
            .timed(
                "put blob",
                ctx.put(client, &created.signed_upload_url, &[], ctx.body(size)),
            )
            .await
            .is_some()
        {
            let finalize = FinalizeCacheEntryUploadRequest {
                key: key.to_owned(),
                version: Self::VERSION.to_owned(),
            };
            ctx.timed(
                "finalize",
                ctx.twirp_protobuf::<cache_server::twirp::FinalizeCacheEntryUploadResponse>(
                    client,
                    "FinalizeCacheEntryUpload",
                    finalize,
                ),
            )
            .await;
        }
        false
    }
}

/// Runs `futures` with at most `limit` in flight, returning their outputs in
/// completion order.
pub async fn bounded<F: Future>(futures: Vec<F>, limit: usize) -> Vec<F::Output> {
    let mut pending = futures.into_iter();
    let mut running: FuturesUnordered<F> = pending.by_ref().take(limit.max(1)).collect();
    let mut outputs = Vec::new();
    while let Some(output) = running.next().await {
        outputs.push(output);
        if let Some(next) = pending.next() {
            running.push(next);
        }
    }
    outputs
}
