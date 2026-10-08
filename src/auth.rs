//! GitHub Actions runtime token verification and cache scopes (ADR-0006).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, StatusCode, header};
use base64::Engine;
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::error::ApiError;

/// Like `jose`'s remote JWKS: keep a fetched set for 10 minutes, and refetch
/// early for an unknown `kid`, at most every 30 seconds.
const JWKS_MAX_AGE: Duration = Duration::from_secs(10 * 60);
const JWKS_COOLDOWN: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Deserialize)]
pub struct CacheScope {
    #[serde(rename = "Scope")]
    pub scope: String,
    #[serde(rename = "Permission")]
    pub permission: i64,
}

#[derive(Clone, Debug)]
pub struct TokenScopes {
    pub scopes: Vec<CacheScope>,
    pub repo_id: String,
}

impl TokenScopes {
    /// The scope uploads are written to.
    #[must_use]
    pub fn write_scope(&self) -> Option<&CacheScope> {
        self.scopes.iter().find(|scope| scope.permission >= 2)
    }

    /// Scope names to search, highest permission first.
    #[must_use]
    pub fn read_scopes(&self) -> Vec<String> {
        let mut scopes = self.scopes.clone();
        scopes.sort_by_key(|scope| std::cmp::Reverse(scope.permission));
        scopes.into_iter().map(|scope| scope.scope).collect()
    }
}

#[derive(Deserialize)]
struct Claims {
    ac: Option<serde_json::Value>,
    repository_id: Option<serde_json::Value>,
}

struct CachedJwks {
    set: JwkSet,
    fetched_at: Instant,
}

pub struct Auth {
    issuer: String,
    skip_validation: bool,
    override_jwks_url: Option<String>,
    http: reqwest::Client,
    /// Discovered `jwks_uri`, cached for the process once discovery succeeds.
    discovered_jwks_url: Mutex<Option<String>>,
    jwks: Mutex<HashMap<String, CachedJwks>>,
}

impl Auth {
    /// # Panics
    ///
    /// If the HTTP client for fetching JWKS can't be built, i.e. the TLS
    /// backend fails to initialize.
    pub fn new(config: &Config) -> Self {
        if config.skip_token_validation {
            tracing::warn!("Token validation is disabled. This should not be used in production!");
        }
        Self {
            issuer: config.actions_token_issuer.clone(),
            skip_validation: config.skip_token_validation,
            override_jwks_url: config.actions_token_jwks_url.clone(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP client"),
            discovered_jwks_url: Mutex::new(None),
            jwks: Mutex::new(HashMap::new()),
        }
    }

    /// The JWKS URL can't be derived from the issuer: an enterprise with a
    /// custom issuer (`{host}/{enterpriseSlug}`) still serves its JWKS at
    /// `{host}`. Ask the OIDC discovery document instead.
    ///
    /// # Errors
    ///
    /// If the discovery document can't be fetched or has no `jwks_uri`.
    pub async fn discover_jwks_url(&self) -> anyhow::Result<String> {
        let url = format!("{}/.well-known/openid-configuration", self.issuer);
        let response = self.http.get(&url).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("Unexpected status {}", response.status());
        }
        let config: serde_json::Value = response.json().await?;
        match config.get("jwks_uri").and_then(|uri| uri.as_str()) {
            Some(uri) => Ok(uri.to_owned()),
            None => anyhow::bail!("Discovery document at {url} has no `jwks_uri`"),
        }
    }

    async fn jwks_url(&self) -> String {
        if let Some(url) = &self.override_jwks_url {
            return url.clone();
        }
        let mut discovered = self.discovered_jwks_url.lock().await;
        if let Some(url) = discovered.as_ref() {
            return url.clone();
        }
        match self.discover_jwks_url().await {
            Ok(url) => discovered.insert(url).clone(),
            Err(err) => {
                let fallback = format!("{}/.well-known/jwks", self.issuer);
                tracing::warn!(
                    error = %err,
                    "OIDC discovery failed, falling back to {fallback}. Set ACTIONS_TOKEN_JWKS_URL if token validation keeps failing."
                );
                // Deliberately not cached, so the next request retries
                // discovery instead of pinning a URL that may be wrong.
                fallback
            }
        }
    }

    async fn find_key(&self, kid: Option<&str>) -> anyhow::Result<Jwk> {
        let url = self.jwks_url().await;
        let mut cache = self.jwks.lock().await;
        let find = |set: &JwkSet| match kid {
            Some(kid) => set.find(kid).cloned(),
            None if set.keys.len() == 1 => set.keys.first().cloned(),
            None => None,
        };

        if let Some(cached) = cache.get(&url) {
            let fresh = cached.fetched_at.elapsed() < JWKS_MAX_AGE;
            if fresh && let Some(key) = find(&cached.set) {
                return Ok(key);
            }
            if fresh && cached.fetched_at.elapsed() < JWKS_COOLDOWN {
                anyhow::bail!("no matching key in the JWKS");
            }
        }

        let response = self.http.get(&url).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("JWKS request to {url} failed with {}", response.status());
        }
        let set: JwkSet = response.json().await?;
        let key = find(&set);
        cache.insert(
            url,
            CachedJwks {
                set,
                fetched_at: Instant::now(),
            },
        );
        key.ok_or_else(|| anyhow::anyhow!("no matching key in the JWKS"))
    }

    async fn verify(&self, token: &str) -> anyhow::Result<Claims> {
        if self.skip_validation {
            let payload = token
                .split('.')
                .nth(1)
                .ok_or_else(|| anyhow::anyhow!("malformed token"))?;
            let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload.trim_end_matches('='))?;
            return Ok(serde_json::from_slice(&payload)?);
        }

        let header = jsonwebtoken::decode_header(token)?;
        if !matches!(
            header.alg,
            Algorithm::RS256
                | Algorithm::RS384
                | Algorithm::RS512
                | Algorithm::PS256
                | Algorithm::PS384
                | Algorithm::PS512
                | Algorithm::ES256
                | Algorithm::ES384
                | Algorithm::EdDSA
        ) {
            anyhow::bail!("unsupported token algorithm {:?}", header.alg);
        }
        let key = self.find_key(header.kid.as_deref()).await?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        validation.set_required_spec_claims(&["iss"]);
        validation.validate_aud = false;
        let data =
            jsonwebtoken::decode::<Claims>(token, &DecodingKey::from_jwk(&key)?, &validation)?;
        Ok(data.claims)
    }

    /// # Errors
    ///
    /// An unauthorized [`ApiError`] when the `Authorization` header is missing
    /// or malformed, the token fails verification, or it carries no cache scopes.
    pub async fn token_scopes(&self, headers: &HeaderMap) -> Result<TokenScopes, ApiError> {
        let unauthorized = |message: &str| ApiError::new(StatusCode::UNAUTHORIZED, message);
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| unauthorized("Authorization header missing or malformed"))?;

        let claims = self.verify(token).await.map_err(|err| {
            tracing::debug!(error = format!("{err:#}"), "Token verification failed");
            unauthorized("Invalid token")
        })?;

        let scopes = claims
            .ac
            .as_ref()
            .and_then(|ac| ac.as_str())
            .ok_or_else(|| unauthorized("Token does not contain cache scopes"))?;
        let scopes: Vec<CacheScope> = serde_json::from_str(scopes)
            .map_err(|_| unauthorized("Invalid JSON in cache scopes"))?;
        if scopes.is_empty() {
            return Err(unauthorized("Token does not contain any cache scopes"));
        }

        let repo_id = match claims.repository_id {
            Some(serde_json::Value::String(id)) if !id.is_empty() => id,
            Some(serde_json::Value::Number(id)) => id.to_string(),
            _ => return Err(unauthorized("Token does not contain repository id")),
        };

        Ok(TokenScopes { scopes, repo_id })
    }
}
