//! File I/O for filesystem storage.
//!
//! With the `io-uring` feature on Linux, file data and metadata operations
//! (open, read, write, close, statx, rename, unlink, mkdir) run on a small pool
//! of `tokio-uring` worker threads, each owning its own ring and current-thread
//! runtime; the multi-threaded server runtime hands them jobs over channels.
//! Operations `io_uring` has no opcode for (readdir, statvfs, recursive delete)
//! run on tokio's blocking pool. When `io_uring` is disabled or the kernel refuses
//! a ring at startup, everything falls back to `tokio::fs`.

use std::io;
use std::path::PathBuf;
use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use tokio::io::AsyncWriteExt;

pub type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

/// Size of each read, and the size writes are coalesced to before submission.
const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Clone)]
pub enum FileIo {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring(std::sync::Arc<uring::UringPool>),
    Tokio,
}

impl FileIo {
    /// `io_uring` when requested and available, otherwise `tokio::fs`.
    pub fn new(use_io_uring: bool, threads: usize) -> Self {
        if !use_io_uring {
            tracing::info!("Filesystem storage I/O uses tokio::fs (io_uring disabled)");
            return Self::Tokio;
        }

        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        match uring::UringPool::start(threads) {
            Ok(pool) => {
                tracing::info!(threads, "Filesystem storage I/O uses io_uring");
                return Self::Uring(std::sync::Arc::new(pool));
            }
            Err(err) => {
                tracing::warn!(error = %err, "io_uring is unavailable, falling back to tokio::fs");
            }
        }

        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        {
            let _ = threads;
            tracing::info!("Filesystem storage I/O uses tokio::fs (built without io_uring)");
        }
        Self::Tokio
    }

    #[must_use]
    pub fn is_io_uring(&self) -> bool {
        !matches!(self, Self::Tokio)
    }

    /// Opens `path` and streams its contents. Opening happens before this
    /// returns, so a missing file surfaces as `NotFound` here rather than as a
    /// stream error.
    ///
    /// # Errors
    ///
    /// If the file can't be opened.
    pub async fn read_file(&self, path: PathBuf) -> io::Result<ByteStream> {
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => pool.read_file(path).await,
            Self::Tokio => {
                let file = tokio::fs::File::open(&path).await?;
                Ok(Box::pin(tokio_util::io::ReaderStream::with_capacity(
                    file, CHUNK_SIZE,
                )))
            }
        }
    }

    /// Writes `stream` to a newly created (or truncated) file at `path` and
    /// returns the number of bytes written. An error item aborts the write and
    /// is returned; the partial file is left for the caller to remove.
    ///
    /// # Errors
    ///
    /// If `stream` yields an error or the file can't be written.
    pub async fn write_file<S>(&self, path: PathBuf, stream: S) -> io::Result<u64>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + 'static,
    {
        let chunks = coalesce(stream);
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => pool.write_file(path, chunks).await,
            Self::Tokio => {
                let mut file = tokio::fs::File::create(&path).await?;
                let mut written = 0u64;
                let mut chunks = std::pin::pin!(chunks);
                while let Some(chunk) = chunks.next().await {
                    let chunk = chunk?;
                    file.write_all(&chunk).await?;
                    written += chunk.len() as u64;
                }
                file.flush().await?;
                Ok(written)
            }
        }
    }

    /// Size of the file at `path`, or `None` when nothing exists there.
    ///
    /// # Errors
    ///
    /// If `path` can't be inspected for a reason other than not existing.
    pub async fn file_size(&self, path: PathBuf) -> io::Result<Option<u64>> {
        let result = match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => pool
                .run(move || async move {
                    tokio_uring::fs::statx(&path)
                        .await
                        .map(|stat| stat.stx_size)
                })
                .await
                .and_then(|result| result),
            Self::Tokio => tokio::fs::metadata(&path).await.map(|meta| meta.len()),
        };
        match result {
            Ok(size) => Ok(Some(size)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// # Errors
    ///
    /// If the rename fails.
    pub async fn rename(&self, from: PathBuf, to: PathBuf) -> io::Result<()> {
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => {
                pool.run(move || async move { tokio_uring::fs::rename(&from, &to).await })
                    .await?
            }
            Self::Tokio => tokio::fs::rename(from, to).await,
        }
    }

    /// # Errors
    ///
    /// If the file can't be removed.
    pub async fn remove_file(&self, path: PathBuf) -> io::Result<()> {
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => {
                pool.run(move || async move { tokio_uring::fs::remove_file(&path).await })
                    .await?
            }
            Self::Tokio => tokio::fs::remove_file(path).await,
        }
    }

    /// # Errors
    ///
    /// If a directory can't be created.
    pub async fn create_dir_all(&self, path: PathBuf) -> io::Result<()> {
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => {
                pool.run(move || async move { tokio_uring::fs::create_dir_all(&path).await })
                    .await?
            }
            Self::Tokio => tokio::fs::create_dir_all(path).await,
        }
    }
}

/// Merges small body frames (hyper yields ~16 KiB) into `CHUNK_SIZE` writes.
fn coalesce<S>(stream: S) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static
where
    S: Stream<Item = io::Result<Bytes>> + Send + 'static,
{
    async_stream::stream! {
        let mut stream = std::pin::pin!(stream);
        let mut buffer = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };
            if buffer.is_empty() && chunk.len() >= CHUNK_SIZE {
                yield Ok(chunk);
                continue;
            }
            buffer.extend_from_slice(&chunk);
            if buffer.len() >= CHUNK_SIZE {
                yield Ok(buffer.split().freeze());
            }
        }
        if !buffer.is_empty() {
            yield Ok(buffer.freeze());
        }
    }
}

/// Blocking-pool helper for operations `io_uring` has no opcode for.
///
/// # Errors
///
/// Whatever `f` fails with, or an error if the blocking task panics or is
/// cancelled.
pub async fn blocking<T, F>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|err| io::Error::other(format!("blocking task failed: {err}")))?
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring {
    use std::future::Future;
    use std::io;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::{Bytes, BytesMut};
    use futures::{Stream, StreamExt};
    use tokio::sync::{mpsc, oneshot};

    use super::{ByteStream, CHUNK_SIZE};

    type Job = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send>;

    /// Worker threads that each run a `tokio-uring` runtime. Jobs are closures
    /// producing `!Send` futures, spawned onto the worker's local executor.
    pub struct UringPool {
        workers: Vec<mpsc::UnboundedSender<Job>>,
        next: AtomicUsize,
    }

    impl UringPool {
        pub fn start(threads: usize) -> io::Result<Self> {
            // tokio-uring panics if it cannot create a ring, so probe first:
            // seccomp profiles and `kernel.io_uring_disabled` refuse it here.
            io_uring::IoUring::new(8)?;

            let workers = (0..threads)
                .map(|index| {
                    let (sender, mut receiver) = mpsc::unbounded_channel::<Job>();
                    std::thread::Builder::new()
                        .name(format!("io-uring-{index}"))
                        .spawn(move || {
                            tokio_uring::builder().entries(256).start(async move {
                                while let Some(job) = receiver.recv().await {
                                    tokio_uring::spawn(job());
                                }
                            });
                        })?;
                    Ok(sender)
                })
                .collect::<io::Result<_>>()?;

            Ok(Self {
                workers,
                next: AtomicUsize::new(0),
            })
        }

        fn submit(&self, job: Job) -> io::Result<()> {
            let index = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
            self.workers[index]
                .send(job)
                .map_err(|_| io::Error::other("io_uring worker has stopped"))
        }

        /// Runs `f` on a worker and returns its output. The job runs to
        /// completion even if the caller stops waiting for it.
        pub async fn run<F, Fut, T>(&self, f: F) -> io::Result<T>
        where
            F: FnOnce() -> Fut + Send + 'static,
            Fut: Future<Output = T> + 'static,
            T: Send + 'static,
        {
            let (sender, receiver) = oneshot::channel();
            self.submit(Box::new(move || {
                Box::pin(async move {
                    let _ = sender.send(f().await);
                })
            }))?;
            receiver
                .await
                .map_err(|_| io::Error::other("io_uring worker dropped the operation"))
        }

        pub async fn read_file(&self, path: PathBuf) -> io::Result<ByteStream> {
            let (opened_sender, opened) = oneshot::channel::<io::Result<()>>();
            // Two chunks of read-ahead keep the ring busy while the consumer sends.
            let (chunks_sender, chunks) = mpsc::channel::<io::Result<Bytes>>(2);

            self.submit(Box::new(move || {
                Box::pin(async move {
                    let file = match tokio_uring::fs::File::open(&path).await {
                        Ok(file) => file,
                        Err(err) => {
                            let _ = opened_sender.send(Err(err));
                            return;
                        }
                    };
                    let _ = opened_sender.send(Ok(()));

                    let mut position = 0u64;
                    loop {
                        let (result, mut buffer) = file
                            .read_at(BytesMut::with_capacity(CHUNK_SIZE), position)
                            .await;
                        let item = match result {
                            Ok(0) => break,
                            Ok(read) => {
                                position += read as u64;
                                Ok(buffer.split().freeze())
                            }
                            Err(err) => Err(err),
                        };
                        let failed = item.is_err();
                        // A send error means the reader went away.
                        if chunks_sender.send(item).await.is_err() || failed {
                            break;
                        }
                    }
                    let _ = file.close().await;
                })
            }))?;

            opened
                .await
                .map_err(|_| io::Error::other("io_uring worker dropped the operation"))??;
            Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(
                chunks,
            )))
        }

        pub async fn write_file<S>(&self, path: PathBuf, stream: S) -> io::Result<u64>
        where
            S: Stream<Item = io::Result<Bytes>> + Send + 'static,
        {
            // The body stream is driven here, on the server runtime; only the
            // file operations run on the ring.
            let (chunks_sender, mut chunks) = mpsc::channel::<Bytes>(2);
            let write = self.run(move || async move {
                let file = tokio_uring::fs::File::create(&path).await?;
                let mut position = 0u64;
                let mut result = Ok(());
                while let Some(chunk) = chunks.recv().await {
                    let len = chunk.len() as u64;
                    let (written, _) = file.write_all_at(chunk, position).await;
                    if let Err(err) = written {
                        result = Err(err);
                        break;
                    }
                    position += len;
                }
                let closed = file.close().await;
                result.and(closed).map(|()| position)
            });

            let feed = async move {
                let mut stream = std::pin::pin!(stream);
                while let Some(chunk) = stream.next().await {
                    // The writer stopped early: its own error is the one to report.
                    if chunks_sender.send(chunk?).await.is_err() {
                        break;
                    }
                }
                Ok::<_, io::Error>(())
            };

            let (written, body) = tokio::join!(write, feed);
            // A failed body must not look like a complete file.
            body?;
            written?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn round_trip(io: FileIo) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        let payload: Vec<u8> = (0..(CHUNK_SIZE * 3 + 123))
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();

        // Small frames, to exercise coalescing.
        let frames: Vec<io::Result<Bytes>> = payload
            .chunks(10_000)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let written = io
            .write_file(path.clone(), futures::stream::iter(frames))
            .await
            .unwrap();
        assert_eq!(written, payload.len() as u64);
        assert_eq!(
            io.file_size(path.clone()).await.unwrap(),
            Some(payload.len() as u64)
        );

        let mut read = Vec::new();
        let mut stream = io.read_file(path.clone()).await.unwrap();
        while let Some(chunk) = stream.next().await {
            read.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(read, payload);

        let renamed = dir.path().join("nested/dir/renamed");
        io.create_dir_all(renamed.parent().unwrap().to_path_buf())
            .await
            .unwrap();
        io.rename(path.clone(), renamed.clone()).await.unwrap();
        assert_eq!(io.file_size(path.clone()).await.unwrap(), None);
        assert!(matches!(
            io.read_file(path).await.map(|_| ()).unwrap_err().kind(),
            io::ErrorKind::NotFound
        ));
        io.remove_file(renamed.clone()).await.unwrap();
        assert_eq!(io.file_size(renamed).await.unwrap(), None);

        // A failing body aborts the write with that error.
        let failing = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"partial")),
            Err(io::Error::other("client went away")),
        ]);
        let err = io
            .write_file(dir.path().join("failed"), failing)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "client went away");
    }

    #[tokio::test]
    async fn tokio_fs_round_trip() {
        round_trip(FileIo::Tokio).await;
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn io_uring_round_trip() {
        let io = FileIo::new(true, 2);
        if !io.is_io_uring() {
            eprintln!("io_uring unavailable on this host, skipping");
            return;
        }
        round_trip(io).await;
    }
}
