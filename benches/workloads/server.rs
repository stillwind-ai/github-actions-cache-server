//! The server under test: the real binary as a child process, on a fresh
//! Postgres database and storage directory, so CPU time and peak RSS can be
//! read from `/proc` and attributed to one scenario.

use std::time::Duration;

use sea_orm::{ConnectionTrait, Database, DatabaseConnection};

pub struct Server {
    pub url: String,
    pid: u32,
    child: tokio::process::Child,
    admin: DatabaseConnection,
    database: String,
    _storage: tempfile::TempDir,
}

/// `BENCH_SERVER_BIN` compares another build (say, the base branch's)
/// against this one.
pub fn binary() -> String {
    std::env::var("BENCH_SERVER_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_github-actions-cache-server").to_owned())
}

impl Server {
    /// Starts a server; `env` overrides the inherited environment, so
    /// `STORAGE_FILESYSTEM_IO_URING=false cargo bench` reaches it too.
    pub async fn start(env: &[(&str, &str)]) -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres@127.0.0.1:5432/postgres".into());
        let admin = Database::connect(&admin_url).await.unwrap_or_else(|err| {
            panic!("the benchmarks need Postgres at TEST_DATABASE_URL ({admin_url}): {err}")
        });
        let database = format!("cache_server_bench_{}", uuid::Uuid::new_v4().simple());
        admin
            .execute_unprepared(&format!(r#"CREATE DATABASE "{database}""#))
            .await
            .unwrap();
        let mut database_url = url::Url::parse(&admin_url).unwrap();
        database_url.set_path(&database);

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let url = format!("http://127.0.0.1:{port}");
        let storage = tempfile::Builder::new()
            .prefix("cache-server-bench-")
            .tempdir_in(
                std::env::var("BENCH_STORAGE_DIR")
                    .unwrap_or_else(|_| std::env::temp_dir().display().to_string()),
            )
            .unwrap();
        let mut command = tokio::process::Command::new(binary());
        command
            .env("HOST", "127.0.0.1")
            .env("PORT", port.to_string())
            .env("API_BASE_URL", &url)
            .env("DB_POSTGRES_URL", database_url.as_str())
            .env("STORAGE_FILESYSTEM_PATH", storage.path())
            .env("SKIP_TOKEN_VALIDATION", "true")
            .env("DISABLE_CLEANUP_JOBS", "true")
            .env("CACHE_FILESYSTEM_MAX_USAGE_PERCENT", "100")
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or("error".into()),
            )
            .kill_on_drop(true);
        for (name, value) in env {
            command.env(name, value);
        }
        let child = command.spawn().expect("start the server binary");
        let pid = child.id().unwrap();

        let client = reqwest::Client::new();
        let started = std::time::Instant::now();
        while client.get(format!("{url}/health")).send().await.is_err() {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "server did not come up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Self {
            url,
            pid,
            child,
            admin,
            database,
            _storage: storage,
        }
    }

    /// User and system CPU time so far.
    pub fn cpu(&self) -> (Duration, Duration) {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", self.pid)).unwrap();
        // Fields after the parenthesized command name; utime and stime are 14 and 15.
        let fields: Vec<&str> = stat
            .rsplit_once(')')
            .unwrap()
            .1
            .split_whitespace()
            .collect();
        // `/proc` reports in USER_HZ, which is 100 on every mainstream Linux.
        let ticks =
            |index: usize| Duration::from_millis(fields[index].parse::<u64>().unwrap() * 10);
        (ticks(11), ticks(12))
    }

    /// Peak resident set size in bytes.
    pub fn peak_rss(&self) -> u64 {
        std::fs::read_to_string(format!("/proc/{}/status", self.pid))
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|kib| kib.parse::<u64>().ok())
            .unwrap_or(0)
            * 1024
    }

    pub async fn stop(mut self) {
        // SIGTERM, so the server drains and finishes its merges.
        let _ = std::process::Command::new("kill")
            .arg(self.pid.to_string())
            .status();
        let _ = tokio::time::timeout(Duration::from_secs(30), self.child.wait()).await;
        let _ = self.child.kill().await;
        let _ = self
            .admin
            .execute_unprepared(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database
            ))
            .await;
    }
}
