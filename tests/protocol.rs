//! The cache protocol end to end over HTTP: save, restore, matching, Twirp
//! wire formats, auth, Results Passthrough and the management API.

mod common;

use axum::body::Bytes;
use common::*;
use prost::Message;
use serde_json::{Value, json};

const MB: usize = 1024 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn saves_and_restores_payloads_of_all_sizes() {
    let server = start().await;
    for (size, block_size) in [
        (1, 4 * MB),
        (2 * MB, 4 * MB),
        (9 * MB, 4 * MB),
        (64 * MB, 8 * MB),
    ] {
        let key = format!("cache-key-{size}");
        let data = random_bytes(size);
        server.save(&key, "v1", &data, block_size).await;

        // The first download streams the Parts while merging them…
        assert!(
            server.restore(&key, "v1").await == data,
            "first restore of {size} bytes"
        );
        server.wait_for_merges().await;
        // …and later downloads read the merged object.
        assert!(
            server.restore(&key, "v1").await == data,
            "merged restore of {size} bytes"
        );
    }

    let merged = std::fs::read_dir(server.storage_path())
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().join("merged").exists())
        .count();
    assert_eq!(merged, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn single_put_blob_uploads_are_part_zero() {
    let server = start().await;
    let upload_url = server.create_entry("put-blob", "v1").await.unwrap();
    // buildx and small uploads send the whole blob without a block id.
    let response = server
        .client
        .put(upload_url.replace("/devstoreaccount1", ""))
        .body("whole blob")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(server.finalize("put-blob", "v1", 10).await.status(), 200);
    assert_eq!(server.restore("put-blob", "v1").await, b"whole blob");
}

#[tokio::test(flavor = "multi_thread")]
async fn matches_primary_and_restore_keys_by_exact_key_then_newest_prefix() {
    let server = start().await;
    server.save("npm-linux-aaa", "v1", b"aaa", 1024).await;
    server.save("npm-linux-bbb", "v1", b"bbb", 1024).await;
    server.save("npm-macos-ccc", "v1", b"ccc", 1024).await;
    server.save("100%_literal", "v1", b"literal", 1024).await;

    // Exact primary key.
    let (_, matched) = server.lookup("npm-linux-aaa", &[], "v1").await.unwrap();
    assert_eq!(matched, "npm-linux-aaa");
    // Primary key as a prefix: the newest entry wins.
    let (_, matched) = server.lookup("npm-linux", &[], "v1").await.unwrap();
    assert_eq!(matched, "npm-linux-bbb");
    // Restore keys in order.
    let (url, matched) = server
        .lookup("npm-windows-zzz", &["npm-windows-", "npm-macos-"], "v1")
        .await
        .unwrap();
    assert_eq!(matched, "npm-macos-ccc");
    assert_eq!(server.download(&url).await.1, b"ccc");
    // LIKE wildcards in keys are literal.
    assert!(server.lookup("100_", &[], "v1").await.is_none());
    assert!(server.lookup("zz", &["1%"], "v1").await.is_none());
    assert_eq!(
        server.lookup("100%", &[], "v1").await.unwrap().1,
        "100%_literal"
    );
    // Versions don't mix.
    assert!(server.lookup("npm-linux-aaa", &[], "v2").await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn match_priority_outranks_recency() {
    let server = start().await;
    server.save("pkg", "v1", b"exact", 1024).await;
    server.save("pkg-newer", "v1", b"prefixed", 1024).await;
    server.save("app-old", "v1", b"primary prefix", 1024).await;
    server.save("lib-x", "v1", b"restore exact", 1024).await;

    // An exact key beats a newer entry it is a prefix of.
    assert_eq!(server.lookup("pkg", &[], "v1").await.unwrap().1, "pkg");
    // A primary-key prefix beats a newer exact restore key.
    assert_eq!(
        server.lookup("app-", &["lib-x"], "v1").await.unwrap().1,
        "app-old"
    );

    // The branch's own scope, even by prefix, beats main's exact key.
    let feature = token(
        json!([
            { "Scope": "refs/heads/main", "Permission": 1 },
            { "Scope": "refs/heads/feature", "Permission": 3 },
        ]),
        "123",
    );
    let created: Value = server
        .twirp_as(
            &feature,
            "CreateCacheEntry",
            json!({ "key": "pkg-feature", "version": "v1" }),
        )
        .await
        .json()
        .await
        .unwrap();
    server
        .upload_blocks(created["signed_upload_url"].as_str().unwrap(), b"f", 1024)
        .await;
    let finalized = server
        .twirp_as(
            &feature,
            "FinalizeCacheEntryUpload",
            json!({ "key": "pkg-feature", "version": "v1" }),
        )
        .await;
    assert_eq!(finalized.status(), 200);
    let found: Value = server
        .twirp_as(
            &feature,
            "GetCacheEntryDownloadURL",
            json!({ "key": "pkg", "version": "v1" }),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(found["matched_key"], "pkg-feature");
}

#[tokio::test(flavor = "multi_thread")]
async fn scopes_and_repositories_isolate_entries() {
    let server = start().await;
    server.save("shared-key", "v1", b"main", 1024).await;

    // A feature branch reads its own scope first, then main's.
    let feature = token(
        &json!([
            { "Scope": "refs/heads/main", "Permission": 1 },
            { "Scope": "refs/heads/feature", "Permission": 3 },
        ]),
        "123",
    );
    let lookup = |token: String| {
        let server = &server;
        async move {
            let body: Value = server
                .twirp_as(
                    &token,
                    "GetCacheEntryDownloadURL",
                    json!({ "key": "shared-key", "version": "v1" }),
                )
                .await
                .json()
                .await
                .unwrap();
            body["signed_download_url"].as_str().map(str::to_owned)
        }
    };
    let main_url = lookup(feature.clone()).await.unwrap();

    let created: Value = server
        .twirp_as(
            &feature,
            "CreateCacheEntry",
            json!({ "key": "shared-key", "version": "v1" }),
        )
        .await
        .json()
        .await
        .unwrap();
    let upload_url = created["signed_upload_url"].as_str().unwrap();
    server.upload_blocks(upload_url, b"feature", 1024).await;
    let finalized = server
        .twirp_as(
            &feature,
            "FinalizeCacheEntryUpload",
            json!({ "key": "shared-key", "version": "v1" }),
        )
        .await;
    assert_eq!(finalized.status(), 200);

    let feature_url = lookup(feature).await.unwrap();
    assert_ne!(feature_url, main_url);
    assert_eq!(server.download(&feature_url).await.1, b"feature");
    assert_eq!(server.restore("shared-key", "v1").await, b"main");

    // Another repository sees nothing.
    let other_repo = token(
        &json!([{ "Scope": "refs/heads/main", "Permission": 3 }]),
        "456",
    );
    assert!(lookup(other_repo).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn saving_an_existing_key_replaces_the_entry() {
    let server = start().await;
    server.save("replaced", "v1", b"old", 1024).await;
    server.save("replaced", "v1", b"new payload", 1024).await;
    assert_eq!(server.restore("replaced", "v1").await, b"new payload");

    // The replaced Storage Location is reclaimed by cleanup.
    let summary = server
        .state
        .cleanup
        .run(cache_server::cleanup::Task::StorageLocations)
        .await
        .unwrap();
    assert_eq!(summary.deleted_locations, Some(1));
    assert_eq!(std::fs::read_dir(server.storage_path()).unwrap().count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_finalizations_complete_the_upload_once() {
    let server = start().await;
    let upload_url = server.create_entry("retried", "v1").await.unwrap();
    server.upload_blocks(&upload_url, b"payload", 1024).await;

    let finalizations = (0..8).map(|_| server.finalize("retried", "v1", 7));
    let statuses: Vec<u16> = futures::future::join_all(finalizations)
        .await
        .iter()
        .map(|response| response.status().as_u16())
        .collect();
    assert_eq!(
        statuses.iter().filter(|status| **status == 200).count(),
        1,
        "{statuses:?}"
    );
    assert!(
        statuses.iter().all(|status| [200, 404].contains(status)),
        "{statuses:?}"
    );

    // No second Storage Location shares the folder, so cleanup can't take it.
    server
        .state
        .cleanup
        .run(cache_server::cleanup::Task::StorageLocations)
        .await
        .unwrap();
    assert_eq!(server.restore("retried", "v1").await, b"payload");
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_uploads_of_the_same_key_are_rejected() {
    let server = start().await;
    assert!(server.create_entry("busy", "v1").await.is_some());
    assert!(server.create_entry("busy", "v1").await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_incomplete_or_unknown_uploads() {
    let server = start().await;
    // Finalizing without any parts abandons the upload.
    server.create_entry("empty", "v1").await.unwrap();
    let response = server.finalize("empty", "v1", 0).await;
    assert_eq!(response.status(), 400);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["code"], "invalid_argument");
    // …so it can be started again.
    assert!(server.create_entry("empty", "v1").await.is_some());

    // Parts must be numbered from 0.
    let upload_url = server.create_entry("gap", "v1").await.unwrap();
    let response = server
        .client
        .put(&upload_url)
        .query(&[("comp", "block"), ("blockid", &block_id(1))])
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(server.finalize("gap", "v1", 1).await.status(), 400);

    assert_eq!(
        server.finalize("never-created", "v1", 1).await.status(),
        404
    );

    let response = server
        .client
        .put(format!("{}/upload/42", server.url))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let response = server
        .client
        .put(format!("{}/upload/abc", server.url))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let response = server
        .client
        .put(format!("{}/upload/42?comp=block&blockid=bad", server.url))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn enforces_tokens_and_permissions() {
    let server = start().await;
    let url = format!(
        "{}/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
        server.url
    );
    let body = json!({ "key": "k", "version": "v" });

    let response = server.client.post(&url).json(&body).send().await.unwrap();
    assert_eq!(response.status(), 401);
    let error: Value = response.json().await.unwrap();
    assert_eq!(error["code"], "unauthenticated");

    for bad in [
        "not-a-jwt".to_owned(),
        token(&json!([]), "123"),
        token(
            &json!([{ "Scope": "refs/heads/main", "Permission": 3 }]),
            "",
        ),
    ] {
        let response = server
            .twirp_as(&bad, "CreateCacheEntry", body.clone())
            .await;
        assert_eq!(response.status(), 401);
    }

    let read_only = token(
        &json!([{ "Scope": "refs/heads/main", "Permission": 1 }]),
        "123",
    );
    let response = server
        .twirp_as(&read_only, "CreateCacheEntry", body.clone())
        .await;
    assert_eq!(response.status(), 403);

    let response = server
        .twirp("CreateCacheEntry", json!({ "key": "", "version": "v" }))
        .await;
    assert_eq!(response.status(), 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_are_verified_against_the_issuer_jwks() {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

    // A fake issuer serving OIDC discovery and a JWKS for a fixed test key.
    const PRIVATE_KEY: &str = include_str!("fixtures/test-key.pem");
    const JWK: &str = include_str!("fixtures/test-key.jwk.json");
    let issuer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", issuer_listener.local_addr().unwrap());
    let jwks_path = "/custom/jwks";
    let discovery = json!({ "issuer": issuer, "jwks_uri": format!("{issuer}{jwks_path}") });
    let issuer_app = axum::Router::new()
        .route(
            "/.well-known/openid-configuration",
            axum::routing::get(move || async move { axum::Json(discovery) }),
        )
        .route(
            jwks_path,
            axum::routing::get(|| async {
                (
                    [("content-type", "application/json")],
                    format!(r#"{{"keys":[{JWK}]}}"#),
                )
            }),
        );
    tokio::spawn(async move { axum::serve(issuer_listener, issuer_app).await.unwrap() });

    let server = start_with(&[
        ("SKIP_TOKEN_VALIDATION", "false"),
        ("ACTIONS_TOKEN_ISSUER", &issuer),
    ])
    .await;
    let sign = |iss: &str| {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".into());
        let claims = json!({
            "iss": iss,
            "exp": chrono::Utc::now().timestamp() + 600,
            "ac": json!([{ "Scope": "refs/heads/main", "Permission": 3 }]).to_string(),
            "repository_id": "123",
        });
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(PRIVATE_KEY.as_bytes()).unwrap(),
        )
        .unwrap()
    };
    let body = json!({ "key": "signed", "version": "v1" });

    let response = server
        .twirp_as(&sign(&issuer), "CreateCacheEntry", body.clone())
        .await;
    assert_eq!(response.status(), 200);
    // Wrong issuer, unsigned tokens and tampered tokens are all rejected.
    let response = server
        .twirp_as(
            &sign("https://evil.example"),
            "CreateCacheEntry",
            body.clone(),
        )
        .await;
    assert_eq!(response.status(), 401);
    let response = server
        .twirp_as(&main_token(), "CreateCacheEntry", body.clone())
        .await;
    assert_eq!(response.status(), 401);
    let mut tampered = sign(&issuer);
    tampered.insert(tampered.find('.').unwrap() + 2, 'x');
    let response = server.twirp_as(&tampered, "CreateCacheEntry", body).await;
    assert_eq!(response.status(), 401);
}

#[derive(Clone, PartialEq, Message)]
struct CacheMetadata {
    #[prost(int64, tag = "1")]
    repository_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct CreateRequest {
    #[prost(message, optional, tag = "1")]
    metadata: Option<CacheMetadata>,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(string, tag = "3")]
    version: String,
}

#[derive(Clone, PartialEq, Message)]
struct CreateResponse {
    #[prost(bool, tag = "1")]
    ok: bool,
    #[prost(string, tag = "2")]
    signed_upload_url: String,
}

#[derive(Clone, PartialEq, Message)]
struct FinalizeRequest {
    #[prost(message, optional, tag = "1")]
    metadata: Option<CacheMetadata>,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(int64, tag = "3")]
    size_bytes: i64,
    #[prost(string, tag = "4")]
    version: String,
}

#[derive(Clone, PartialEq, Message)]
struct FinalizeResponse {
    #[prost(bool, tag = "1")]
    ok: bool,
    #[prost(int64, tag = "2")]
    entry_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct GetRequest {
    #[prost(message, optional, tag = "1")]
    metadata: Option<CacheMetadata>,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(string, repeated, tag = "3")]
    restore_keys: Vec<String>,
    #[prost(string, tag = "4")]
    version: String,
}

#[derive(Clone, PartialEq, Message)]
struct GetResponse {
    #[prost(bool, tag = "1")]
    ok: bool,
    #[prost(string, tag = "2")]
    signed_download_url: String,
    #[prost(string, tag = "3")]
    matched_key: String,
}

#[tokio::test(flavor = "multi_thread")]
#[expect(clippy::too_many_lines, reason = "one scenario over every endpoint")]
async fn speaks_protobuf_to_protobuf_clients() {
    let server = start().await;
    let post = |method: &str, body: Vec<u8>| {
        server
            .client
            .post(format!(
                "{}/twirp/github.actions.results.api.v1.CacheService/{method}",
                server.url
            ))
            .bearer_auth(&server.token)
            .header("content-type", "application/protobuf")
            .body(body)
            .send()
    };
    // `metadata` is a field the server doesn't declare: it must be skipped.
    let metadata = Some(CacheMetadata { repository_id: 123 });

    let response = post(
        "CreateCacheEntry",
        CreateRequest {
            metadata: metadata.clone(),
            key: "protobuf-key".into(),
            version: "v1".into(),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("application/protobuf")
    );
    let created = CreateResponse::decode(response.bytes().await.unwrap()).unwrap();
    assert!(
        created.ok
            && created
                .signed_upload_url
                .contains("/devstoreaccount1/upload/")
    );

    server
        .upload_blocks(&created.signed_upload_url, b"protobuf payload", 4)
        .await;
    let response = post(
        "FinalizeCacheEntryUpload",
        FinalizeRequest {
            metadata: metadata.clone(),
            key: "protobuf-key".into(),
            size_bytes: 16,
            version: "v1".into(),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    let finalized = FinalizeResponse::decode(response.bytes().await.unwrap()).unwrap();
    assert!(finalized.ok && finalized.entry_id > 0);

    let response = post(
        "GetCacheEntryDownloadURL",
        GetRequest {
            metadata: metadata.clone(),
            key: "missing".into(),
            restore_keys: vec!["nope".into(), "protobuf-".into()],
            version: "v1".into(),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    let found = GetResponse::decode(response.bytes().await.unwrap()).unwrap();
    assert!(found.ok);
    assert_eq!(found.matched_key, "protobuf-key");
    assert_eq!(
        server.download(&found.signed_download_url).await.1,
        b"protobuf payload"
    );

    let response = post(
        "GetCacheEntryDownloadURL",
        GetRequest {
            metadata,
            key: "no-such-key".into(),
            restore_keys: vec![],
            version: "v1".into(),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    let miss = GetResponse::decode(response.bytes().await.unwrap()).unwrap();
    assert!(!miss.ok);

    // A missing upload is a 404, proving the protobuf body parsed and validated.
    let response = post(
        "FinalizeCacheEntryUpload",
        FinalizeRequest {
            metadata: None,
            key: "never-uploaded".into(),
            size_bytes: 1,
            version: "v1".into(),
        }
        .encode_to_vec(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 404);
}

#[derive(Clone, Debug, serde::Serialize)]
struct CapturedRequest {
    method: String,
    uri: String,
    headers: Vec<(String, String)>,
    body: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn forwards_unhandled_requests_to_the_results_origin() {
    // A fake Results origin that records requests and answers 409.
    let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<CapturedRequest>::new()));
    let origin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", origin_listener.local_addr().unwrap());
    let origin_app = axum::Router::new().fallback({
        let captured = captured.clone();
        move |request: axum::extract::Request| {
            let captured = captured.clone();
            async move {
                let (parts, body) = request.into_parts();
                let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                captured.lock().await.push(CapturedRequest {
                    method: parts.method.to_string(),
                    uri: parts.uri.to_string(),
                    headers: parts
                        .headers
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
                        .collect(),
                    body: String::from_utf8(body.to_vec()).unwrap(),
                });
                (
                    axum::http::StatusCode::CONFLICT,
                    [
                        ("content-type", "application/json; charset=utf-8"),
                        ("x-results-origin", "fake"),
                    ],
                    r#"{"code":"already_exists","msg":"artifact already exists"}"#,
                )
            }
        }
    });
    tokio::spawn(async move { axum::serve(origin_listener, origin_app).await.unwrap() });

    let server = start_with(&[("DEFAULT_ACTIONS_RESULTS_URL", &origin)]).await;
    let request_body = r#"{"name":"build-output","version":4}"#;
    let path = "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact?api-version=6.0-preview.1";
    let response = server
        .client
        .post(format!("{}{path}", server.url))
        .header("authorization", "Bearer artifact-token")
        .header("content-type", "application/json")
        .header("x-artifact-request", "create")
        .body(request_body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409);
    assert_eq!(
        response.headers()["content-type"],
        "application/json; charset=utf-8"
    );
    assert_eq!(response.headers()["x-results-origin"], "fake");
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"code":"already_exists","msg":"artifact already exists"}"#
    );

    let captured = captured.lock().await;
    assert_eq!(captured.len(), 1);
    let request = &captured[0];
    assert_eq!(
        (request.method.as_str(), request.uri.as_str()),
        ("POST", path)
    );
    assert_eq!(request.body, request_body);
    let header = |name: &str| {
        request
            .headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    assert_eq!(header("authorization"), Some("Bearer artifact-token"));
    assert_eq!(header("content-type"), Some("application/json"));
    assert_eq!(header("x-artifact-request"), Some("create"));
}

#[tokio::test(flavor = "multi_thread")]
async fn exposes_health_and_metrics() {
    let server = start().await;
    let get = |path: &str| server.client.get(format!("{}{path}", server.url)).send();
    assert_eq!(get("/").await.unwrap().text().await.unwrap(), "OK");
    assert_eq!(
        get("/health").await.unwrap().text().await.unwrap(),
        "healthy"
    );

    server.save("metrics", "v1", b"12345", 1024).await;
    server.restore("metrics", "v1").await;
    assert!(server.lookup("absent", &[], "v1").await.is_none());

    let metrics = get("/metrics").await.unwrap().text().await.unwrap();
    for line in [
        r#"cache_requests_total{result="hit"} 1"#,
        r#"cache_requests_total{result="miss"} 1"#,
        "cache_uploads_total 1",
        "cache_storage_bytes 5",
    ] {
        assert!(
            metrics.lines().any(|metric| metric == line),
            "missing `{line}` in\n{metrics}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn management_api_lists_matches_and_deletes_entries() {
    let server = start().await;
    let api = |method: reqwest::Method, path: &str| {
        server
            .client
            .request(method, format!("{}/management-api{path}", server.url))
            .header("x-api-key", "secret")
    };

    let response = server
        .client
        .get(format!("{}/management-api/cache-entries", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);

    server.save("mgmt-a", "v1", b"a", 1024).await;
    server.save("mgmt-b", "v1", b"bb", 1024).await;
    server.save("other", "v2", b"ccc", 1024).await;

    let list: Value = api(
        reqwest::Method::GET,
        "/cache-entries?version=v1&itemsPerPage=1&page=2",
    )
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(list["total"], 2);
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["key"], "mgmt-a");
    assert_eq!(list["items"][0]["repoId"], "123");

    let matched: Value = api(
        reqwest::Method::GET,
        "/cache-entries/match?primaryKey=missing&restoreKeys=mgmt-&scopes=refs/heads/main&repoId=123&version=v1",
    )
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(matched["type"], "prefixed-restore");
    assert_eq!(matched["match"]["key"], "mgmt-b");

    let id = matched["match"]["id"].as_str().unwrap();
    let entry: Value = api(reqwest::Method::GET, &format!("/cache-entries/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let location: Value = api(
        reqwest::Method::GET,
        &format!(
            "/storage-locations/{}",
            entry["locationId"].as_str().unwrap()
        ),
    )
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(location["sizeBytes"], 2);
    assert_eq!(location["partCount"], 1);

    let response = api(
        reqwest::Method::GET,
        &format!("/cache-entries/{}", uuid::Uuid::new_v4()),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(response.json::<Value>().await.unwrap()["code"], "NOT_FOUND");

    assert_eq!(
        api(reqwest::Method::DELETE, &format!("/cache-entries/{id}"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(server.lookup("mgmt-b", &[], "v1").await.is_none());
    assert_eq!(
        api(reqwest::Method::DELETE, "/cache-entries?version=v1")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(server.lookup("mgmt-a", &[], "v1").await.is_none());
    assert!(server.lookup("other", &[], "v2").await.is_some());

    let bad: reqwest::Response = api(reqwest::Method::GET, "/cache-entries?itemsPerPage=500")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn management_api_is_disabled_without_a_key() {
    let listener_server = start_with(&[("MANAGEMENT_API_KEY", "")]).await;
    let response = listener_server
        .client
        .get(format!(
            "{}/management-api/cache-entries",
            listener_server.url
        ))
        .header("x-api-key", "")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let _: Bytes = response.bytes().await.unwrap();
}
