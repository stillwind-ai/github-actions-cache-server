//! An HTTP/1.1 server on the rings of a [`RingPool`], in front of the same
//! axum router `axum::serve` would run.
//!
//! Each ring thread accepts connections from the shared listener and serves
//! them as local tasks: requests are parsed from the receive buffer and
//! handed to the router on the same thread, so a request's socket
//! operations, its file operations and its handler all run on one ring.
//! Request bodies arrive as frames received straight into their own buffers
//! (one `recv` per megabyte with `MSG_WAITALL`). Responses go out with one
//! `sendmsg` for the head and the first body frames. A response carrying a
//! [`FileBody`] is spliced: file to pipe to socket, inside the kernel, so
//! downloads never copy cache data through user space.

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Response, Version};
use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use http_body::Body as _;
use http_body_util::BodyExt;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tower_service::Service;

use super::http::{self, ChannelBody, Framing, Head, ResponseFraming};
use super::{RingPool, ops};
use crate::storage::io::{CHUNK_SIZE, FileBody, FileSegment, consume};

/// Initial receive buffer; grows for large heads.
const READ_BUFFER: usize = 16 * 1024;
/// Body frames are received into buffers of this size, filled completely.
const BODY_CHUNK: usize = CHUNK_SIZE;
/// Frames buffered between the socket and the handler.
const BODY_FRAMES_IN_FLIGHT: usize = 2;
/// Response frames gathered into one `sendmsg` when they are ready together.
const MAX_GATHER: usize = 64;
/// Pipes kept per ring thread for splicing downloads.
const MAX_IDLE_PIPES: usize = 64;
/// Pipe size requested for splicing (the default unprivileged maximum).
const PIPE_SIZE: libc::c_int = 1024 * 1024;
/// Download bodies up to this size are read and sent rather than spliced.
const SMALL_BODY: u64 = 64 * 1024;

/// Serves `router` on `listener` until `shutdown` is cancelled, then lets
/// requests in flight finish. Dropping the returned future also shuts down.
///
/// # Errors
///
/// If the listener can't be prepared or a ring thread has stopped.
pub async fn serve(
    pool: &RingPool,
    listener: std::net::TcpListener,
    router: Router,
    shutdown: CancellationToken,
) -> io::Result<()> {
    // io_uring handles readiness itself; a blocking socket lets it.
    listener.set_nonblocking(false)?;
    let listener = Arc::new(OwnedFd::from(listener));
    let shutdown = shutdown.child_token();
    let _stop_on_drop = shutdown.clone().drop_guard();
    let mut finished = Vec::with_capacity(pool.threads());
    for worker in 0..pool.threads() {
        let (done, finished_receiver) = oneshot::channel::<()>();
        let listener = listener.clone();
        let router = router.clone();
        let shutdown = shutdown.clone();
        pool.spawn_on(
            worker,
            Box::new(move || {
                Box::pin(async move {
                    accept_loop(&listener, router, shutdown).await;
                    let _ = done.send(());
                })
            }),
        )?;
        finished.push(finished_receiver);
    }
    for finished in finished {
        let _ = finished.await;
    }
    Ok(())
}

/// Connections open on this thread, so shutdown can wait for them.
#[derive(Default)]
struct Connections {
    open: Cell<usize>,
    closed: Notify,
}

struct ConnectionGuard(Rc<Connections>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.open.set(self.0.open.get() - 1);
        self.0.closed.notify_waiters();
    }
}

async fn accept_loop(listener: &OwnedFd, router: Router, shutdown: CancellationToken) {
    let connections = Rc::new(Connections::default());
    loop {
        let accepted = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            accepted = ops::accept(listener.as_raw_fd()) => accepted,
        };
        match accepted {
            Ok(socket) => {
                set_nodelay(&socket);
                connections.open.set(connections.open.get() + 1);
                let guard = ConnectionGuard(connections.clone());
                let connection = Connection {
                    socket,
                    buf: BytesMut::with_capacity(READ_BUFFER),
                    router: router.clone(),
                    shutdown: shutdown.clone(),
                };
                tokio::task::spawn_local(async move {
                    let _guard = guard;
                    connection.run().await;
                });
            }
            Err(err) => match err.raw_os_error() {
                Some(libc::EBADF | libc::EINVAL | libc::ENOTSOCK) => {
                    tracing::error!(error = %err, "The listener failed");
                    break;
                }
                // Out of descriptors or memory: give connections a moment to
                // close instead of spinning.
                Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM) => {
                    tracing::warn!(error = %err, "Failed to accept a connection");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                _ => tracing::debug!(error = %err, "Failed to accept a connection"),
            },
        }
    }
    while connections.open.get() > 0 {
        let closed = connections.closed.notified();
        if connections.open.get() == 0 {
            break;
        }
        closed.await;
    }
}

/// Small responses (Twirp, response heads ahead of a download body) go out
/// at once instead of waiting on Nagle and delayed ACKs.
fn set_nodelay(socket: &OwnedFd) {
    let enable: libc::c_int = 1;
    // SAFETY: a valid socket and option buffer.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            (&raw const enable).cast(),
            libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>()).unwrap_or(4),
        )
    };
    if result != 0 {
        tracing::debug!(error = %io::Error::last_os_error(), "Failed to set TCP_NODELAY");
    }
}

/// What happens to a connection after an exchange.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Next {
    KeepAlive,
    Close,
    /// Close, though the client may still be sending the request body.
    CloseUnread,
}

/// How receiving a request body ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fed {
    /// All of it was received; the connection can carry another request.
    Complete,
    /// The handler stopped reading, or receiving failed.
    Incomplete,
}

struct Connection {
    socket: OwnedFd,
    /// Received bytes not yet consumed: the next head, or the start of a body.
    buf: BytesMut,
    router: Router,
    shutdown: CancellationToken,
}

impl Connection {
    fn fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }

    async fn run(mut self) {
        loop {
            let head = match self.read_head().await {
                Ok(Some(head)) => head,
                Ok(None) => return,
                Err(HeadError::Io(err)) => {
                    tracing::debug!(error = %err, "Connection failed before a request");
                    return;
                }
                Err(HeadError::Rejected(status)) => {
                    let _ = self
                        .send_all(vec![http::rejection_response(status)], 0)
                        .await;
                    self.linger().await;
                    return;
                }
            };
            match self.exchange(head).await {
                Ok(Next::KeepAlive) => {}
                Ok(Next::Close) => return,
                Ok(Next::CloseUnread) => {
                    self.linger().await;
                    return;
                }
                Err(err) => {
                    tracing::debug!(error = %err, "Connection failed during a request");
                    return;
                }
            }
        }
    }

    /// Receives more bytes into `buf`. Zero means the peer closed.
    async fn fill(&mut self) -> io::Result<usize> {
        if self.buf.capacity() - self.buf.len() < READ_BUFFER / 4 {
            self.buf.reserve(READ_BUFFER);
        }
        let buf = std::mem::take(&mut self.buf);
        let (received, buf) = ops::recv(self.fd(), buf, usize::MAX, 0).await;
        self.buf = buf;
        received
    }

    async fn read_head(&mut self) -> Result<Option<Head>, HeadError> {
        loop {
            if !self.buf.is_empty()
                && let Some(head) = http::parse_head(&mut self.buf)
                    .map_err(|rejection| HeadError::Rejected(rejection.0))?
            {
                return Ok(Some(head));
            }
            let received = if self.buf.is_empty() {
                // Idle between requests: shutting down closes the connection.
                let shutdown = self.shutdown.clone();
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return Ok(None),
                    received = self.fill() => received,
                }
            } else {
                self.fill().await
            };
            match received.map_err(HeadError::Io)? {
                0 if self.buf.is_empty() => return Ok(None),
                0 => return Err(HeadError::Io(io::ErrorKind::UnexpectedEof.into())),
                _ => {}
            }
        }
    }

    /// Serves one request.
    async fn exchange(&mut self, head: Head) -> io::Result<Next> {
        let Head {
            request,
            framing,
            keep_alive,
            expect_continue,
        } = head;
        let version = request.version();
        let head_request = request.method() == Method::HEAD;

        let (body, body_sender) = match framing {
            Framing::Empty => (Body::empty(), None),
            Framing::Length(length) if self.buf.len() as u64 >= length => {
                // Small bodies usually arrive with their head.
                let body = self
                    .buf
                    .split_to(usize::try_from(length).unwrap_or(usize::MAX));
                (Body::from(body.freeze()), None)
            }
            Framing::Length(length) => {
                let (sender, frames) = mpsc::channel(BODY_FRAMES_IN_FLIGHT);
                (
                    Body::new(ChannelBody::new(frames, Some(length))),
                    Some(sender),
                )
            }
            Framing::Chunked => {
                let (sender, frames) = mpsc::channel(BODY_FRAMES_IN_FLIGHT);
                (Body::new(ChannelBody::new(frames, None)), Some(sender))
            }
        };
        let (parts, ()) = request.into_parts();
        let mut router = self.router.clone();
        let response = router.call(axum::http::Request::from_parts(parts, body));

        let (response, fed) = match body_sender {
            None => (response.await, Fed::Complete),
            Some(sender) => {
                let receiving = self.feed_body(framing, sender, expect_continue);
                let mut receiving = std::pin::pin!(receiving);
                let mut response = std::pin::pin!(response);
                let mut outcome = None;
                let response = loop {
                    tokio::select! {
                        response = &mut response => break response,
                        received = &mut receiving, if outcome.is_none() => outcome = Some(received),
                    }
                };
                // A body still being received when the response is ready is
                // abandoned, with whatever it left unread on the socket.
                (response, outcome.unwrap_or(Fed::Incomplete))
            }
        };
        let Ok(response) = response;
        let keep_alive = keep_alive && fed == Fed::Complete && !self.shutdown.is_cancelled();
        let kept = self
            .respond(response, version, head_request, keep_alive)
            .await?;
        Ok(match (kept, fed) {
            (true, _) => Next::KeepAlive,
            (false, Fed::Complete) => Next::Close,
            (false, Fed::Incomplete) => Next::CloseUnread,
        })
    }

    /// Receives a request body into `sender` while the handler runs.
    async fn feed_body(
        &mut self,
        framing: Framing,
        sender: mpsc::Sender<io::Result<Bytes>>,
        expect_continue: bool,
    ) -> Fed {
        if expect_continue {
            let interim = Bytes::from_static(b"HTTP/1.1 100 Continue\r\n\r\n");
            if let Err(err) = self.send_all(vec![interim], 0).await {
                let _ = sender.send(Err(err)).await;
                return Fed::Incomplete;
            }
        }
        let fed = match framing {
            Framing::Empty => Ok(Fed::Complete),
            Framing::Length(length) => self.feed_length(length, &sender).await,
            Framing::Chunked => self.feed_chunked(&sender).await,
        };
        fed.unwrap_or_else(|err| {
            // The handler sees why its body ended; it may already be gone.
            let _ = sender.try_send(Err(err));
            Fed::Incomplete
        })
    }

    async fn feed_length(
        &mut self,
        length: u64,
        sender: &mpsc::Sender<io::Result<Bytes>>,
    ) -> io::Result<Fed> {
        let mut remaining = length;
        if !self.buf.is_empty() {
            let take = self
                .buf
                .len()
                .min(usize::try_from(remaining).unwrap_or(usize::MAX));
            let frame = self.buf.split_to(take).freeze();
            remaining -= take as u64;
            if sender.send(Ok(frame)).await.is_err() {
                return Ok(if remaining == 0 {
                    Fed::Complete
                } else {
                    Fed::Incomplete
                });
            }
        }
        let mut chunk = BytesMut::new();
        while remaining > 0 {
            let wanted = usize::try_from(remaining).unwrap_or(usize::MAX);
            if chunk.capacity() - chunk.len() < wanted.min(64 * 1024) {
                chunk = BytesMut::with_capacity(wanted.min(BODY_CHUNK));
            }
            let (received, returned) = ops::recv(self.fd(), chunk, wanted, libc::MSG_WAITALL).await;
            chunk = returned;
            let received = received?;
            if received == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "client closed the connection during the request body",
                ));
            }
            remaining -= received as u64;
            if sender.send(Ok(chunk.split().freeze())).await.is_err() {
                return Ok(if remaining == 0 {
                    Fed::Complete
                } else {
                    Fed::Incomplete
                });
            }
        }
        Ok(Fed::Complete)
    }

    async fn feed_chunked(&mut self, sender: &mpsc::Sender<io::Result<Bytes>>) -> io::Result<Fed> {
        loop {
            let line = self.read_line().await?;
            let size = http::chunk_size(&line)?;
            if size == 0 {
                // Trailers are read and dropped.
                while !self.read_line().await?.is_empty() {}
                return Ok(Fed::Complete);
            }
            let mut left = size;
            while left > 0 {
                if self.buf.is_empty() && self.fill().await? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                let take = self
                    .buf
                    .len()
                    .min(usize::try_from(left).unwrap_or(usize::MAX));
                let frame = self.buf.split_to(take).freeze();
                left -= take as u64;
                if sender.send(Ok(frame)).await.is_err() {
                    return Ok(Fed::Incomplete);
                }
            }
            if !self.read_line().await?.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid chunked request body",
                ));
            }
        }
    }

    /// The next CRLF-terminated line, without the CRLF.
    async fn read_line(&mut self) -> io::Result<Bytes> {
        loop {
            if let Some(end) = self.buf.windows(2).position(|pair| pair == b"\r\n") {
                let line = self.buf.split_to(end).freeze();
                let _ = self.buf.split_to(2);
                return Ok(line);
            }
            if self.buf.len() > 4096 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunked body line too long",
                ));
            }
            if self.fill().await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
        }
    }

    /// Sends a response. True when the connection can be reused.
    async fn respond(
        &mut self,
        response: Response<Body>,
        version: Version,
        head_request: bool,
        mut keep_alive: bool,
    ) -> io::Result<bool> {
        let (mut parts, body) = response.into_parts();
        let files = parts.extensions.remove::<FileBody>();
        let framing = http::response_framing(
            parts.status,
            &mut parts.headers,
            &body.size_hint(),
            head_request,
            version,
            &mut keep_alive,
        );
        let head = http::encode_head(parts.status, &mut parts.headers, version, keep_alive);
        match (framing, files) {
            (ResponseFraming::None, _) => self.send_all(vec![head], 0).await?,
            (ResponseFraming::Length(length), Some(files)) if files.len() == length => {
                drop(body);
                self.send_files(head, &files).await?;
            }
            (framing, _) => self.send_body(head, body, framing).await?,
        }
        Ok(keep_alive)
    }

    /// Streams a response body, gathering whatever frames are ready into
    /// each `sendmsg` (the head goes with the first).
    async fn send_body(
        &mut self,
        head: Bytes,
        body: Body,
        framing: ResponseFraming,
    ) -> io::Result<()> {
        let mut body = std::pin::pin!(body);
        let mut pending = vec![head];
        let mut sent = 0u64;
        loop {
            // Gather frames while they are ready; flush before waiting.
            let frame = if pending.is_empty() {
                body.frame().await
            } else if let Some(frame) = (pending.len() < MAX_GATHER)
                .then(|| body.frame().now_or_never())
                .flatten()
            {
                frame
            } else {
                self.send_all(std::mem::take(&mut pending), 0).await?;
                continue;
            };
            let Some(frame) = frame else { break };
            let Ok(data) = frame.map_err(io::Error::other)?.into_data() else {
                continue;
            };
            if data.is_empty() {
                continue;
            }
            sent += data.len() as u64;
            match framing {
                ResponseFraming::Length(length) if sent > length => {
                    return Err(io::Error::other(
                        "response body longer than its content-length",
                    ));
                }
                ResponseFraming::Chunked => {
                    pending.push(Bytes::from(format!("{:x}\r\n", data.len())));
                    pending.push(data);
                    pending.push(Bytes::from_static(b"\r\n"));
                }
                _ => pending.push(data),
            }
        }
        match framing {
            ResponseFraming::Chunked => pending.push(Bytes::from_static(b"0\r\n\r\n")),
            // The client would wait for the rest: abort instead.
            ResponseFraming::Length(length) if sent != length => {
                return Err(io::Error::other(
                    "response body shorter than its content-length",
                ));
            }
            _ => {}
        }
        self.send_all(pending, 0).await
    }

    /// Sends the head, then the files of `files`: spliced through a pipe,
    /// or for a small body read into one buffer that leaves with the head
    /// (splicing costs two trips through `io_uring`'s worker threads, which a
    /// small copy doesn't).
    async fn send_files(&mut self, head: Bytes, files: &FileBody) -> io::Result<()> {
        let total = files.len();
        if total <= SMALL_BODY {
            let body = read_files(files).await?;
            files.check()?;
            return self.send_all(vec![head, body], 0).await;
        }
        // The head waits for the first data, to leave in the same segment.
        self.send_all(vec![head], libc::MSG_MORE).await?;
        let pipe = Pipe::take()?;
        let pipe_fds = (pipe.read.as_raw_fd(), pipe.write.as_raw_fd());
        let socket = self.fd();
        let mut sent = 0u64;
        for segment in files.segments() {
            let opened;
            // Splicing exactly a file's size saves a splice to find its end.
            let (fd, size) = match segment {
                FileSegment::Open(file, size) => (file.as_raw_fd(), *size),
                FileSegment::Path(path) => {
                    opened = ops::open(path, libc::O_RDONLY, 0).await?;
                    let fd = opened.as_raw_fd();
                    (fd, ops::file_size(fd).await?)
                }
            };
            let mut offset = 0u64;
            while offset < size {
                let wanted = usize::try_from((size - offset).min(total - sent))
                    .unwrap_or(usize::MAX)
                    .min(pipe.size);
                if wanted == 0 {
                    break;
                }
                let flags = if sent + (wanted as u64) < total {
                    libc::SPLICE_F_MORE
                } else {
                    0
                };
                let (moved, out) = ops::splice_through_pipe(
                    fd,
                    offset.cast_signed(),
                    pipe_fds,
                    socket,
                    wanted,
                    flags,
                )
                .await;
                let moved = moved?;
                // Short of `wanted` (the end of this file), the link is broken
                // and what was moved is still in the pipe.
                let mut left = moved
                    - match out {
                        Ok(out) => out,
                        Err(err) if err.raw_os_error() == Some(libc::ECANCELED) => 0,
                        Err(err) => return Err(err),
                    };
                while left > 0 {
                    let out = ops::splice(pipe_fds.0, -1, socket, -1, left, flags).await?;
                    if out == 0 {
                        return Err(io::ErrorKind::WriteZero.into());
                    }
                    left -= out;
                }
                if moved == 0 {
                    break;
                }
                offset += moved as u64;
                sent += moved as u64;
                files.check()?;
            }
        }
        if sent != total {
            return Err(io::Error::other(format!(
                "files hold {sent} bytes, expected {total}"
            )));
        }
        // Only an empty pipe goes back.
        pipe.give_back();
        Ok(())
    }

    /// Sends all of `buffers`.
    async fn send_all(&mut self, mut buffers: Vec<Bytes>, flags: i32) -> io::Result<()> {
        while !buffers.is_empty() {
            let (sent, returned) = ops::send(
                self.fd(),
                buffers,
                libc::MSG_NOSIGNAL | libc::MSG_WAITALL | flags,
            )
            .await;
            buffers = returned;
            let sent = sent?;
            if sent == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            consume(&mut buffers, sent);
        }
        Ok(())
    }

    /// Before closing a connection whose request may not have been read in
    /// full, stops sending and drains what the client still sends for a
    /// moment: closing with unread data resets the connection, which can
    /// destroy the response before the client reads it.
    async fn linger(&mut self) {
        // SAFETY: a valid socket.
        if unsafe { libc::shutdown(self.fd(), libc::SHUT_WR) } != 0 {
            return;
        }
        let fd = self.fd();
        let drain = async move {
            let mut drained = 0;
            let mut buf = BytesMut::with_capacity(64 * 1024);
            while drained < 16 * 1024 * 1024 {
                buf.clear();
                let (received, returned) = ops::recv(fd, buf, usize::MAX, 0).await;
                buf = returned;
                match received {
                    Ok(0) | Err(_) => break,
                    Ok(received) => drained += received,
                }
            }
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), drain).await;
    }
}

/// Reads the (small) files of `files` into one buffer.
async fn read_files(files: &FileBody) -> io::Result<Bytes> {
    let total = usize::try_from(files.len()).unwrap_or(usize::MAX);
    let mut buf = BytesMut::with_capacity(total);
    for segment in files.segments() {
        let opened;
        let fd = match segment {
            FileSegment::Open(file, _) => file.as_raw_fd(),
            FileSegment::Path(path) => {
                opened = ops::open(path, libc::O_RDONLY, 0).await?;
                opened.as_raw_fd()
            }
        };
        let mut offset = 0u64;
        while buf.len() < total {
            let wanted = total - buf.len();
            let (read, returned) = ops::read_at(fd, buf, wanted, offset).await;
            buf = returned;
            match read? {
                0 => break,
                read => offset += read as u64,
            }
        }
    }
    if buf.len() != total {
        return Err(io::Error::other(format!(
            "files hold {} bytes, expected {total}",
            buf.len()
        )));
    }
    Ok(buf.freeze())
}

enum HeadError {
    Io(io::Error),
    Rejected(axum::http::StatusCode),
}

/// A pipe for splicing, reused by later downloads on the same thread.
struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
    size: usize,
}

thread_local! {
    static IDLE_PIPES: RefCell<Vec<Pipe>> = const { RefCell::new(Vec::new()) };
}

impl Pipe {
    fn take() -> io::Result<Self> {
        if let Some(pipe) = IDLE_PIPES.with(|pipes| pipes.borrow_mut().pop()) {
            return Ok(pipe);
        }
        let mut fds = [0; 2];
        // SAFETY: pipe2 fills both descriptors on success.
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both are new descriptors nothing else owns.
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        // A larger pipe moves more per splice. Past the per-user pipe budget
        // the kernel refuses, and the default size works too.
        // SAFETY: fcntl on a valid descriptor.
        let size = unsafe {
            libc::fcntl(write.as_raw_fd(), libc::F_SETPIPE_SZ, PIPE_SIZE);
            libc::fcntl(write.as_raw_fd(), libc::F_GETPIPE_SZ)
        };
        Ok(Self {
            read,
            write,
            size: usize::try_from(size)
                .ok()
                .filter(|&size| size > 0)
                .unwrap_or(64 * 1024),
        })
    }

    fn give_back(self) {
        IDLE_PIPES.with(|pipes| {
            let mut pipes = pipes.borrow_mut();
            if pipes.len() < MAX_IDLE_PIPES {
                pipes.push(self);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::path::PathBuf;

    use axum::routing::{get, post, put};
    use futures::StreamExt;

    use super::*;

    struct TestServer {
        addr: SocketAddr,
        shutdown: CancellationToken,
        served: tokio::task::JoinHandle<io::Result<()>>,
        dir: tempfile::TempDir,
        _pool: Arc<RingPool>,
    }

    fn file_body(path: PathBuf, len: u64) -> Response<Body> {
        let files = FileBody::new(vec![FileSegment::Path(path)], len, Arc::new(()), || None);
        let mut response = Response::new(Body::from("not spliced"));
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_LENGTH, len.into());
        response.extensions_mut().insert(files);
        response
    }

    fn start() -> Option<TestServer> {
        let pool = match RingPool::start(2) {
            Ok(pool) => Arc::new(pool),
            Err(err) => {
                eprintln!("io_uring unavailable on this host ({err}), skipping");
                return None;
            }
        };
        let dir = tempfile::tempdir().unwrap();
        let big: Vec<u8> = (0..3 * 1024 * 1024 + 123)
            .map(|i: u32| (i % 251) as u8)
            .collect();
        std::fs::write(dir.path().join("big"), &big).unwrap();
        std::fs::write(dir.path().join("small"), b"small file").unwrap();
        let (big_path, small_path, missing) = (
            dir.path().join("big"),
            dir.path().join("small"),
            dir.path().join("missing"),
        );
        let router = Router::new()
            .route("/echo", post(|body: Bytes| async move { body }))
            .route(
                "/count",
                put(|body: Body| async move {
                    let mut stream = body.into_data_stream();
                    let mut count = 0;
                    while let Some(frame) = stream.next().await {
                        count += frame.unwrap().len();
                    }
                    count.to_string()
                }),
            )
            .route(
                "/reject",
                put(|| async { axum::http::StatusCode::NOT_FOUND }),
            )
            .route(
                "/stream",
                get(|| async {
                    Body::from_stream(futures::stream::iter(
                        ["ab", "", "cde"].map(|chunk| Ok::<_, io::Error>(Bytes::from(chunk))),
                    ))
                }),
            )
            .route(
                "/big",
                get(move || async move { file_body(big_path, 3 * 1024 * 1024 + 123) }),
            )
            .route(
                "/small",
                get(move || async move { file_body(small_path, 10) }),
            )
            .route(
                "/missing",
                get(move || async move { file_body(missing, 100_000) }),
            );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let served = {
            let (pool, shutdown) = (pool.clone(), shutdown.clone());
            tokio::spawn(async move { serve(&pool, listener, router, shutdown).await })
        };
        Some(TestServer {
            addr,
            shutdown,
            served,
            dir,
            _pool: pool,
        })
    }

    struct Reply {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Reply {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    /// Reads one response; `head` for a response without a body.
    fn read_reply(reader: &mut impl BufRead, head: bool) -> io::Result<Reply> {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let status = line
            .split_whitespace()
            .nth(1)
            .and_then(|status| status.parse().ok())
            .ok_or_else(|| io::Error::other(format!("bad status line {line:?}")))?;
        let mut headers = Vec::new();
        loop {
            line.clear();
            reader.read_line(&mut line)?;
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(": ").unwrap();
            headers.push((name.to_owned(), value.to_owned()));
        }
        let mut reply = Reply {
            status,
            headers,
            body: Vec::new(),
        };
        if head || status == 100 {
            return Ok(reply);
        }
        if let Some(length) = reply.header("content-length") {
            reply.body = vec![0; length.parse().unwrap()];
            reader.read_exact(&mut reply.body)?;
        } else if reply.header("transfer-encoding") == Some("chunked") {
            loop {
                line.clear();
                reader.read_line(&mut line)?;
                let size = usize::from_str_radix(line.trim_end(), 16).unwrap();
                let mut chunk = vec![0; size + 2];
                reader.read_exact(&mut chunk)?;
                if size == 0 {
                    break;
                }
                reply.body.extend_from_slice(&chunk[..size]);
            }
        } else {
            reader.read_to_end(&mut reply.body)?;
        }
        Ok(reply)
    }

    /// Sends `raw` and reads `count` responses in a blocking thread.
    async fn exchange(
        addr: SocketAddr,
        raw: Vec<u8>,
        count: usize,
        head: bool,
    ) -> Vec<io::Result<Reply>> {
        tokio::task::spawn_blocking(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(&raw).unwrap();
            let mut reader = BufReader::new(stream);
            (0..count).map(|_| read_reply(&mut reader, head)).collect()
        })
        .await
        .unwrap()
    }

    async fn one(addr: SocketAddr, raw: &str) -> Reply {
        exchange(addr, raw.as_bytes().to_vec(), 1, false)
            .await
            .pop()
            .unwrap()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn serves_keep_alive_and_pipelined_requests() {
        let Some(server) = start() else { return };
        let raw = "POST /echo HTTP/1.1\r\nContent-Length: 5\r\n\r\nhelloGET /stream HTTP/1.1\r\n\r\nPOST /echo HTTP/1.1\r\nContent-Length: 3\r\nConnection: close\r\n\r\nbye";
        let replies = exchange(server.addr, raw.as_bytes().to_vec(), 3, false).await;
        let replies: Vec<Reply> = replies.into_iter().map(Result::unwrap).collect();
        assert_eq!(replies[0].body, b"hello");
        assert_eq!(replies[0].header("connection"), None);
        assert!(replies[0].header("date").is_some());
        assert_eq!(replies[1].header("transfer-encoding"), Some("chunked"));
        assert_eq!(replies[1].body, b"abcde");
        assert_eq!(replies[2].body, b"bye");
        assert_eq!(replies[2].header("connection"), Some("close"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receives_large_and_chunked_bodies() {
        let Some(server) = start() else { return };
        let mut raw =
            b"PUT /count HTTP/1.1\r\nContent-Length: 5000000\r\nConnection: close\r\n\r\n".to_vec();
        raw.extend(std::iter::repeat_n(7u8, 5_000_000));
        let reply = exchange(server.addr, raw, 1, false)
            .await
            .pop()
            .unwrap()
            .unwrap();
        assert_eq!(reply.body, b"5000000");

        let reply = one(
            server.addr,
            "POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5;ext=1\r\nhello\r\n1\r\n \r\n5\r\nworld\r\n0\r\nTrailer: x\r\n\r\n",
        )
        .await;
        assert_eq!(reply.body, b"hello world");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn answers_expect_continue() {
        let Some(server) = start() else { return };
        let addr = server.addr;
        let replies = tokio::task::spawn_blocking(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .write_all(b"POST /echo HTTP/1.1\r\nContent-Length: 4\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let interim = read_reply(&mut reader, false).unwrap();
            stream.write_all(b"data").unwrap();
            (interim.status, read_reply(&mut reader, false).unwrap())
        })
        .await
        .unwrap();
        assert_eq!(replies.0, 100);
        assert_eq!(replies.1.body, b"data");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn speaks_http_1_0_and_head() {
        let Some(server) = start() else { return };
        // Unknown length for an HTTP/1.0 client: delimited by closing.
        let reply = one(server.addr, "GET /stream HTTP/1.0\r\n\r\n").await;
        assert_eq!(reply.body, b"abcde");
        assert_eq!(reply.header("connection"), Some("close"));
        assert_eq!(reply.header("transfer-encoding"), None);

        let replies = exchange(
            server.addr,
            b"HEAD /big HTTP/1.1\r\nConnection: close\r\n\r\n".to_vec(),
            1,
            true,
        )
        .await;
        let reply = replies.into_iter().next().unwrap().unwrap();
        assert_eq!(reply.status, 200);
        assert_eq!(reply.header("content-length"), Some("3145851"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_malformed_requests() {
        let Some(server) = start() else { return };
        let reply = one(server.addr, "NOT HTTP\r\n\r\n").await;
        assert_eq!(reply.status, 400);
        assert_eq!(reply.header("connection"), Some("close"));
        let reply = one(
            server.addr,
            "POST /echo HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n",
        )
        .await;
        assert_eq!(reply.status, 501);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn answers_before_reading_the_whole_body() {
        let Some(server) = start() else { return };
        let addr = server.addr;
        let reply = tokio::task::spawn_blocking(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            // Most of the announced body never comes.
            stream
                .write_all(b"PUT /reject HTTP/1.1\r\nContent-Length: 100000000\r\n\r\npartial")
                .unwrap();
            read_reply(&mut BufReader::new(stream), false).unwrap()
        })
        .await
        .unwrap();
        assert_eq!(reply.status, 404);
        assert_eq!(reply.header("connection"), Some("close"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sends_file_bodies() {
        let Some(server) = start() else { return };
        let expected = std::fs::read(server.dir.path().join("big")).unwrap();
        let raw = b"GET /big HTTP/1.1\r\n\r\nGET /small HTTP/1.1\r\n\r\nGET /big HTTP/1.1\r\nConnection: close\r\n\r\n";
        let replies = exchange(server.addr, raw.to_vec(), 3, false).await;
        let replies: Vec<Reply> = replies.into_iter().map(Result::unwrap).collect();
        assert!(replies[0].body == expected, "spliced body differs");
        assert_eq!(replies[1].body, b"small file");
        assert!(replies[2].body == expected, "spliced body differs");

        // A file that vanished aborts the response short of its length.
        let replies = exchange(
            server.addr,
            b"GET /missing HTTP/1.1\r\n\r\n".to_vec(),
            1,
            false,
        )
        .await;
        assert!(replies.into_iter().next().unwrap().is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shuts_down_gracefully() {
        let Some(server) = start() else { return };
        let addr = server.addr;
        // An idle keep-alive connection is closed by the shutdown.
        let idle = tokio::task::spawn_blocking(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .write_all(b"POST /echo HTTP/1.1\r\nContent-Length: 1\r\n\r\nx")
                .unwrap();
            let mut reader = BufReader::new(stream);
            let reply = read_reply(&mut reader, false).unwrap();
            let mut rest = Vec::new();
            reader.read_to_end(&mut rest).unwrap();
            (reply.body, rest)
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        server.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), server.served)
            .await
            .expect("serve returns after shutdown")
            .unwrap()
            .unwrap();
        let (body, rest) = idle.await.unwrap();
        assert_eq!(body, b"x");
        assert!(rest.is_empty());
    }
}
