//! A TCP proxy that delays each direction by half a round-trip time, so the
//! server can be benchmarked against a database that is not on localhost
//! (managed Postgres is typically 0.5-2 ms away). Bytes keep their order and
//! bandwidth is unlimited; only latency is added.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Starts the proxy in the background and returns its address.
pub async fn start(upstream: String, rtt: Duration) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let one_way = rtt / 2;
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            let upstream = upstream.clone();
            tokio::spawn(async move {
                let Ok(server) = TcpStream::connect(&upstream).await else {
                    return;
                };
                let _ = client.set_nodelay(true);
                let _ = server.set_nodelay(true);
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::join!(
                    delayed_pipe(client_read, server_write, one_way),
                    delayed_pipe(server_read, client_write, one_way),
                );
            });
        }
    });
    address
}

async fn delayed_pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
) {
    let (sender, mut receiver) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let reader = async move {
        let mut buffer = vec![0u8; 64 * 1024];
        while let Ok(read) = from.read(&mut buffer).await {
            if read == 0
                || sender
                    .send((Instant::now() + delay, buffer[..read].to_vec()))
                    .is_err()
            {
                break;
            }
        }
    };
    let writer = async move {
        while let Some((due, bytes)) = receiver.recv().await {
            tokio::time::sleep_until(due).await;
            if to.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = to.shutdown().await;
    };
    tokio::join!(reader, writer);
}
