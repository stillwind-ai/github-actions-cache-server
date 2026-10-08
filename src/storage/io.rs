//! File I/O for filesystem storage.
//!
//! With the `io-uring` feature on Linux, file data and metadata operations
//! (open, read, write, close, statx, rename, unlink, mkdir) run on a small pool
//! of `tokio-uring` worker threads, each owning its own ring and current-thread
//! runtime; the multi-threaded server runtime hands them jobs over channels.
//! Operations io_uring has no opcode for (readdir, statvfs, recursive delete,
//! copy_file_range) run on tokio's blocking pool. When io_uring is disabled or
//! the kernel refuses a ring at startup, file data moves through `std::fs` on
//! tokio's blocking pool and metadata operations through `tokio::fs`.
//!
//! Data is never copied in user space: body frames are written as they arrive,
//! batched into vectored writes, and each read lands in a buffer sized to what
//! is left of the file, which then goes out as-is.

use std::io::{self, IoSlice, Read, Write};
use std::path::PathBuf;
use std::pin::Pin;

use bytes::{Buf, Bytes};
use futures::{Stream, StreamExt};

pub type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

/// Largest single read, and the size writes are batched to before submission.
const CHUNK_SIZE: usize = 1024 * 1024;

/// Most buffers in one vectored write, well under `IOV_MAX`.
const MAX_BATCH_BUFFERS: usize = 64;

/// The next read's size: a whole chunk, or what is left of a `size`-byte file.
fn chunk_len(size: u64, position: u64) -> usize {
    size.saturating_sub(position).min(CHUNK_SIZE as u64) as usize
}

#[derive(Clone)]
pub enum FileIo {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring(std::sync::Arc<uring::UringPool>),
    Tokio,
}

impl FileIo {
    /// io_uring when requested and available, otherwise blocking I/O on
    /// tokio's blocking pool.
    pub fn new(use_io_uring: bool, threads: usize) -> Self {
        if !use_io_uring {
            tracing::info!("Filesystem storage I/O uses blocking I/O (io_uring disabled)");
            return Self::Tokio;
        }

        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        match uring::UringPool::start(threads) {
            Ok(pool) => {
                tracing::info!(threads, "Filesystem storage I/O uses io_uring");
                return Self::Uring(std::sync::Arc::new(pool));
            }
            Err(err) => {
                tracing::warn!(error = %err, "io_uring is unavailable, falling back to blocking I/O");
            }
        }

        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        {
            let _ = threads;
            tracing::info!("Filesystem storage I/O uses blocking I/O (built without io_uring)");
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
                let (file, size) = blocking(move || {
                    let file = std::fs::File::open(&path)?;
                    let size = file.metadata()?.len();
                    Ok((file, size))
                })
                .await?;
                Ok(read_blocking(file, size))
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
        let batches = batch(stream);
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => pool.write_file(path, batches).await,
            Self::Tokio => {
                let mut file = blocking(move || std::fs::File::create(&path)).await?;
                let mut written = 0u64;
                let mut batches = std::pin::pin!(batches);
                while let Some(batch) = batches.next().await {
                    let batch = batch?;
                    written += batch_len(&batch);
                    file = blocking(move || write_all_vectored(&mut file, batch).map(|()| file))
                        .await?;
                }
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

/// Groups body frames (hyper yields up to a few hundred KiB each) into batches
/// of about `CHUNK_SIZE` bytes, each written with one vectored write instead of
/// being copied together first.
fn batch<S>(stream: S) -> impl Stream<Item = io::Result<Vec<Bytes>>> + Send + 'static
where
    S: Stream<Item = io::Result<Bytes>> + Send + 'static,
{
    async_stream::try_stream! {
        let mut stream = std::pin::pin!(stream);
        let mut batch = Vec::new();
        let mut len = 0;
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            if frame.is_empty() {
                continue;
            }
            len += frame.len();
            batch.push(frame);
            if len >= CHUNK_SIZE || batch.len() >= MAX_BATCH_BUFFERS {
                yield std::mem::take(&mut batch);
                len = 0;
            }
        }
        if !batch.is_empty() {
            yield batch;
        }
    }
}

fn batch_len(batch: &[Bytes]) -> u64 {
    batch.iter().map(|buffer| buffer.len() as u64).sum()
}

/// Drops the first `written` bytes of `batch` after a short write.
fn consume(batch: &mut Vec<Bytes>, mut written: usize) {
    let mut done = 0;
    for buffer in batch.iter_mut() {
        if written < buffer.len() {
            buffer.advance(written);
            break;
        }
        written -= buffer.len();
        done += 1;
    }
    batch.drain(..done);
}

fn write_all_vectored(file: &mut std::fs::File, mut batch: Vec<Bytes>) -> io::Result<()> {
    while !batch.is_empty() {
        let slices: Vec<IoSlice> = batch.iter().map(|buffer| IoSlice::new(buffer)).collect();
        match file.write_vectored(&slices) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => consume(&mut batch, written),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Streams a `size`-byte file from the blocking pool, reading the next chunk
/// while the current one is being sent.
fn read_blocking(file: std::fs::File, size: u64) -> ByteStream {
    let read = move |file: std::fs::File, position: u64| {
        tokio::task::spawn_blocking(move || {
            let len = chunk_len(size, position);
            let mut chunk = Vec::with_capacity(len);
            (&file).take(len as u64).read_to_end(&mut chunk)?;
            Ok::<_, io::Error>((file, chunk))
        })
    };
    Box::pin(async_stream::try_stream! {
        let mut position = 0;
        let mut next = (size > 0).then(|| read(file, 0));
        while let Some(pending) = next.take() {
            let (file, chunk) = pending.await.map_err(join_error)??;
            if chunk.is_empty() {
                break;
            }
            position += chunk.len() as u64;
            if position < size {
                next = Some(read(file, position));
            }
            yield Bytes::from(chunk);
        }
    })
}

fn join_error(err: tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("blocking task failed: {err}"))
}

/// Runs blocking file operations on tokio's blocking pool: the ones io_uring
/// has no opcode for, and all file data without io_uring.
pub async fn blocking<T, F>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(join_error)?
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

    use super::{ByteStream, chunk_len, consume};

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
                    // The size bounds every buffer, so a small file never holds
                    // a whole chunk, and the end needs no extra read to find.
                    let size = match file.statx().await {
                        Ok(stat) => stat.stx_size,
                        Err(err) => {
                            let _ = opened_sender.send(Err(err));
                            let _ = file.close().await;
                            return;
                        }
                    };
                    let _ = opened_sender.send(Ok(()));

                    let mut position = 0u64;
                    while position < size {
                        let buffer = BytesMut::with_capacity(chunk_len(size, position));
                        let (result, buffer) = file.read_at(buffer, position).await;
                        let item = match result {
                            Ok(0) => break,
                            Ok(read) => {
                                position += read as u64;
                                Ok(buffer.freeze())
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
            S: Stream<Item = io::Result<Vec<Bytes>>> + Send + 'static,
        {
            // The body stream is driven here, on the server runtime; only the
            // file operations run on the ring. One batch waits while another
            // is written.
            let (batches_sender, mut batches) = mpsc::channel::<Vec<Bytes>>(1);
            let write = self.run(move || async move {
                let file = tokio_uring::fs::File::create(&path).await?;
                let written = async {
                    let mut position = 0u64;
                    while let Some(mut batch) = batches.recv().await {
                        while !batch.is_empty() {
                            let (result, returned) = file.writev_at(batch, position).await;
                            batch = returned;
                            let written = result?;
                            if written == 0 {
                                return Err(io::ErrorKind::WriteZero.into());
                            }
                            position += written as u64;
                            consume(&mut batch, written);
                        }
                    }
                    Ok(position)
                }
                .await;
                let closed = file.close().await;
                written.and_then(|position| closed.map(|()| position))
            });

            let feed = async move {
                let mut stream = std::pin::pin!(stream);
                while let Some(batch) = stream.next().await {
                    // The writer stopped early: its own error is the one to report.
                    if batches_sender.send(batch?).await.is_err() {
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

        // Empty files, and files ending exactly on a chunk boundary.
        for len in [0, CHUNK_SIZE] {
            let path = dir.path().join(format!("sized-{len}"));
            let data = Bytes::from(vec![7u8; len]);
            let frames = futures::stream::iter([Ok(Bytes::new()), Ok(data.clone())]);
            assert_eq!(
                io.write_file(path.clone(), frames).await.unwrap(),
                len as u64
            );
            let chunks: Vec<Bytes> = io
                .read_file(path)
                .await
                .unwrap()
                .map(Result::unwrap)
                .collect()
                .await;
            assert_eq!(chunks.len(), len.div_ceil(CHUNK_SIZE));
            assert_eq!(chunks.concat(), data);
        }

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

    #[test]
    fn consumes_short_writes() {
        let mut batch = vec![
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"de"),
            Bytes::from_static(b"fgh"),
        ];
        consume(&mut batch, 4);
        assert_eq!(batch, [&b"e"[..], &b"fgh"[..]]);
        consume(&mut batch, 1);
        assert_eq!(batch, [&b"fgh"[..]]);
        consume(&mut batch, 3);
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn blocking_round_trip() {
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
