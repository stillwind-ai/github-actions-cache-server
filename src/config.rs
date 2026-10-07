use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use anyhow::{Context, bail};

/// Server configuration, read from the same environment variables as the
/// original TypeScript server. Only the `postgres` database driver and the
/// `filesystem` storage driver are supported.
#[derive(Clone, Debug)]
pub struct Config {
    /// `None` listens on all interfaces: IPv6 and IPv4 where available.
    pub listen_host: Option<IpAddr>,
    pub port: u16,
    /// Public base URL of this server, without trailing slashes.
    pub api_base_url: String,
    /// Default Results Origin for Results Passthrough, without trailing slashes.
    pub default_actions_results_url: String,
    pub actions_token_issuer: String,
    pub actions_token_jwks_url: Option<String>,
    pub skip_token_validation: bool,

    pub database_url: String,
    pub database_max_connections: u32,

    pub storage_filesystem_path: PathBuf,
    /// Use io_uring for filesystem storage I/O when the kernel allows it.
    pub storage_filesystem_io_uring: bool,
    /// Number of io_uring worker threads, each running its own ring.
    pub storage_filesystem_io_uring_threads: usize,

    /// Delete cache entries not saved or accessed for this many days. 0 disables.
    pub cache_cleanup_older_than_days: f64,
    pub cache_max_size_bytes: Option<u64>,
    pub cache_filesystem_max_usage_percent: f64,
    pub orphaned_storage_grace_period_hours: u64,
    pub disable_cleanup_jobs: bool,
    pub eager_merge: bool,

    pub management_api_key: Option<String>,
    pub debug: bool,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_vars(std::env::vars().collect())
    }

    pub fn from_vars(vars: HashMap<String, String>) -> anyhow::Result<Self> {
        let env = Env(vars);

        if let Some(driver) = env.get("DB_DRIVER")
            && driver != "postgres"
        {
            bail!("DB_DRIVER={driver} is not supported, only `postgres` is");
        }
        if let Some(driver) = env.get("STORAGE_DRIVER")
            && driver != "filesystem"
        {
            bail!("STORAGE_DRIVER={driver} is not supported, only `filesystem` is");
        }
        if env.bool("ENABLE_DIRECT_DOWNLOADS")?.unwrap_or(false) {
            tracing::warn!(
                "ENABLE_DIRECT_DOWNLOADS has no effect: filesystem storage has no direct-download URLs"
            );
        }

        let host = env
            .get("NITRO_HOST")
            .or_else(|| env.get("HOST"))
            .map(|host| {
                host.parse::<IpAddr>()
                    .with_context(|| format!("invalid HOST `{host}`"))
            })
            .transpose()?;
        let port = env
            .parsed::<u16>("NITRO_PORT")?
            .or(env.parsed::<u16>("PORT")?)
            .unwrap_or(3000);

        let api_base_url = env
            .get("API_BASE_URL")
            .context("API_BASE_URL is required")?
            .to_owned();
        url::Url::parse(&api_base_url).context("API_BASE_URL must be a URL")?;

        let default_actions_results_url = env
            .get("DEFAULT_ACTIONS_RESULTS_URL")
            .unwrap_or("https://results-receiver.actions.githubusercontent.com")
            .to_owned();
        url::Url::parse(&default_actions_results_url)
            .context("DEFAULT_ACTIONS_RESULTS_URL must be a URL")?;

        let actions_token_issuer = env
            .get("ACTIONS_TOKEN_ISSUER")
            .unwrap_or("https://token.actions.githubusercontent.com")
            .trim_end_matches('/')
            .to_owned();
        url::Url::parse(&actions_token_issuer).context("ACTIONS_TOKEN_ISSUER must be a URL")?;
        let actions_token_jwks_url = env.get("ACTIONS_TOKEN_JWKS_URL").map(str::to_owned);

        let cache_cleanup_older_than_days = env
            .parsed::<f64>("CACHE_CLEANUP_OLDER_THAN_DAYS")?
            .unwrap_or(90.0);
        if cache_cleanup_older_than_days.is_nan() || cache_cleanup_older_than_days < 0.0 {
            bail!("CACHE_CLEANUP_OLDER_THAN_DAYS must be >= 0");
        }
        let cache_max_size_bytes = env.parsed::<u64>("CACHE_MAX_SIZE_BYTES")?;
        if cache_max_size_bytes == Some(0) {
            bail!("CACHE_MAX_SIZE_BYTES must be > 0");
        }
        let cache_filesystem_max_usage_percent = env
            .parsed::<f64>("CACHE_FILESYSTEM_MAX_USAGE_PERCENT")?
            .unwrap_or(90.0);
        if cache_filesystem_max_usage_percent.is_nan()
            || cache_filesystem_max_usage_percent <= 0.0
            || cache_filesystem_max_usage_percent > 100.0
        {
            bail!("CACHE_FILESYSTEM_MAX_USAGE_PERCENT must be > 0 and <= 100");
        }
        let orphaned_storage_grace_period_hours = env
            .parsed::<u64>("ORPHANED_STORAGE_GRACE_PERIOD_HOURS")?
            .unwrap_or(24);
        if orphaned_storage_grace_period_hours < 1 {
            bail!("ORPHANED_STORAGE_GRACE_PERIOD_HOURS must be >= 1");
        }

        Ok(Self {
            listen_host: host,
            port,
            api_base_url: api_base_url.trim_end_matches('/').to_owned(),
            default_actions_results_url: default_actions_results_url
                .trim_end_matches('/')
                .to_owned(),
            actions_token_issuer,
            actions_token_jwks_url,
            skip_token_validation: env.bool("SKIP_TOKEN_VALIDATION")?.unwrap_or(false),
            database_url: database_url(&env)?,
            database_max_connections: env
                .parsed::<u32>("DB_POSTGRES_MAX_CONNECTIONS")?
                .unwrap_or(10),
            storage_filesystem_path: env
                .get("STORAGE_FILESYSTEM_PATH")
                .unwrap_or(".data/storage/filesystem")
                .into(),
            storage_filesystem_io_uring: env.bool("STORAGE_FILESYSTEM_IO_URING")?.unwrap_or(true),
            storage_filesystem_io_uring_threads: env
                .parsed::<usize>("STORAGE_FILESYSTEM_IO_URING_THREADS")?
                .unwrap_or(2)
                .max(1),
            cache_cleanup_older_than_days,
            cache_max_size_bytes,
            cache_filesystem_max_usage_percent,
            orphaned_storage_grace_period_hours,
            disable_cleanup_jobs: env.bool("DISABLE_CLEANUP_JOBS")?.unwrap_or(false),
            eager_merge: env.bool("EAGER_MERGE")?.unwrap_or(false),
            management_api_key: env.get("MANAGEMENT_API_KEY").map(str::to_owned),
            // The original treated any non-empty DEBUG value as enabled.
            debug: env
                .get("DEBUG")
                .is_some_and(|value| !matches!(value.to_ascii_lowercase().as_str(), "false" | "0")),
        })
    }
}

impl Config {
    /// Addresses to try binding, in order.
    pub fn listen_addrs(&self) -> Vec<SocketAddr> {
        match self.listen_host {
            Some(host) => vec![SocketAddr::new(host, self.port)],
            None => vec![
                SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), self.port),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port),
            ],
        }
    }
}

fn database_url(env: &Env) -> anyhow::Result<String> {
    if let Some(url) = env.get("DB_POSTGRES_URL") {
        return Ok(url.to_owned());
    }

    let required = |name: &str| {
        env.get(name)
            .with_context(|| format!("{name} is required unless DB_POSTGRES_URL is set"))
    };
    let host = required("DB_POSTGRES_HOST")?;
    let database = required("DB_POSTGRES_DATABASE")?;
    let user = required("DB_POSTGRES_USER")?;
    let password = required("DB_POSTGRES_PASSWORD")?;
    let port = env.parsed::<u16>("DB_POSTGRES_PORT")?.unwrap_or(5432);

    let mut url = url::Url::parse("postgres://localhost").expect("static URL");
    url.set_host(Some(host))
        .with_context(|| format!("invalid DB_POSTGRES_HOST `{host}`"))?;
    url.set_port(Some(port)).expect("postgres URLs have ports");
    url.set_username(user)
        .expect("postgres URLs have credentials");
    url.set_password(Some(password))
        .expect("postgres URLs have credentials");
    url.set_path(database);
    Ok(url.into())
}

struct Env(HashMap<String, String>);

impl Env {
    /// Unset and empty variables are treated the same.
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    fn parsed<T: std::str::FromStr>(&self, name: &str) -> anyhow::Result<Option<T>>
    where
        T::Err: std::fmt::Display,
    {
        self.get(name)
            .map(|value| {
                value
                    .parse()
                    .map_err(|err| anyhow::anyhow!("invalid {name} `{value}`: {err}"))
            })
            .transpose()
    }

    fn bool(&self, name: &str) -> anyhow::Result<Option<bool>> {
        self.get(name)
            .map(|value| match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(true),
                "false" | "0" | "no" => Ok(false),
                _ => bail!("invalid {name} `{value}`: expected true or false"),
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn builds_database_url_from_parts() {
        let config = Config::from_vars(vars(&[
            ("API_BASE_URL", "http://localhost:3000/"),
            ("DB_POSTGRES_HOST", "db"),
            ("DB_POSTGRES_PORT", "5433"),
            ("DB_POSTGRES_DATABASE", "cache"),
            ("DB_POSTGRES_USER", "user"),
            ("DB_POSTGRES_PASSWORD", "p@ss/word"),
        ]))
        .unwrap();
        assert_eq!(
            config.database_url,
            "postgres://user:p%40ss%2Fword@db:5433/cache"
        );
        assert_eq!(config.api_base_url, "http://localhost:3000");
        assert_eq!(config.listen_addrs()[0].port(), 3000);
        assert!(!config.debug);
    }

    #[test]
    fn rejects_unsupported_drivers() {
        let base = [
            ("API_BASE_URL", "http://localhost:3000"),
            ("DB_POSTGRES_URL", "postgres://localhost/db"),
        ];
        for (name, value) in [("DB_DRIVER", "sqlite"), ("STORAGE_DRIVER", "s3")] {
            let mut env = vars(&base);
            env.insert(name.into(), value.into());
            assert!(Config::from_vars(env).is_err());
        }
    }

    #[test]
    fn validates_ranges() {
        let mut env = vars(&[
            ("API_BASE_URL", "http://localhost:3000"),
            ("DB_POSTGRES_URL", "postgres://localhost/db"),
            ("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "101"),
        ]);
        assert!(Config::from_vars(env.clone()).is_err());
        env.insert("CACHE_FILESYSTEM_MAX_USAGE_PERCENT".into(), "50".into());
        env.insert("DEBUG".into(), "yes".into());
        let config = Config::from_vars(env).unwrap();
        assert_eq!(config.cache_filesystem_max_usage_percent, 50.0);
        assert!(config.debug);
    }
}
