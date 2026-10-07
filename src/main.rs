use cache_server::{App, Config};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // For local development; containers configure the environment directly.
    let _ = dotenvy::dotenv();
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            init_tracing(false);
            tracing::error!("Invalid configuration: {err:#}");
            return std::process::ExitCode::FAILURE;
        }
    };
    init_tracing(config.debug);

    tracing::info!(
        "🚀 Starting GitHub Actions Cache Server (v{}{})",
        env!("CARGO_PKG_VERSION"),
        option_env!("BUILD_HASH")
            .map(|hash| format!(" [{hash}]"))
            .unwrap_or_default()
    );

    match run(config).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!("{err:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(config: Config) -> anyhow::Result<()> {
    let addrs = config.listen_addrs();
    let app = App::new(config).await?;
    let listener = bind(&addrs).await?;
    tracing::info!("Listening on http://{}", listener.local_addr()?);
    app.serve(listener, shutdown_signal()).await
}

/// Binds the first address that works: the IPv6 wildcard (dual-stack) is
/// unavailable on hosts without IPv6, where the IPv4 wildcard follows.
async fn bind(addrs: &[std::net::SocketAddr]) -> anyhow::Result<tokio::net::TcpListener> {
    let mut last_error = None;
    for addr in addrs {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => return Ok(listener),
            Err(err) => {
                tracing::debug!(%addr, error = %err, "Cannot listen");
                last_error =
                    Some(anyhow::Error::new(err).context(format!("Failed to listen on {addr}")));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no listen address")))
}

fn init_tracing(debug: bool) {
    let default = if debug {
        "debug,sqlx=info,sqlx::postgres::notice=warn,hyper=info,h2=info"
    } else {
        "info,sqlx::postgres::notice=warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default)),
        )
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("Shutting down...");
}
