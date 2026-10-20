//! File I/O for filesystem storage.
//!
//! With the `io-uring` feature on Linux, file data and metadata operations
//! (open, read, write, close, statx, rename, unlink, mkdir) go through the
//! rings of a [`RingPool`], the same rings that serve client connections. On
//! a ring thread (where requests are handled) an operation runs on that
//! thread's ring as a local task; elsewhere it is handed to a ring thread.
//! Operations `io_uring` has no opcode for (readdir, statvfs, recursive
//! delete, `copy_file_range`) run on tokio's blocking pool. When `io_uring` is
//! disabled or the kernel refuses a ring at startup, file data moves through
//! `std::fs` on tokio's blocking pool and metadata operations through
//! `tokio::fs`.
//!
//! Data is never copied in user space: body frames are written as they
//! arrive, batched into vectored writes, and each read lands in a buffer sized
//! to what is left of the file, which then goes out as-is. Downloads served
//! by the `io_uring` server don't read at all: a [`FileBody`] tells it which
//! files to splice to the socket.

use std::any::Any;
use std::io::{self, IoSlice, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures::{Stream, StreamExt};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use crate::uring::RingPool;

pub type ByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

/// Largest single read, and the size writes are batched to before submission.
pub const CHUNK_SIZE: usize = 1024 * 1024;

/// Most buffers in one vectored write, well under `IOV_MAX`.
const MAX_BATCH_BUFFERS: usize = 64;

/// The next read's size: a whole chunk, or what is left of a `size`-byte file.
fn chunk_len(size: u64, position: u64) -> usize {
    usize::try_from(size.saturating_sub(position)).map_or(CHUNK_SIZE, |left| left.min(CHUNK_SIZE))
}

/// A download body made of whole files in order, which the `io_uring`
/// server splices from the page cache to the socket instead of reading. It
/// travels as a response extension next to an ordinary body streaming the
/// same bytes, for every other way of sending the response.
#[derive(Clone)]
pub struct FileBody(Arc<FileBodyInner>);

struct FileBodyInner {
    segments: Vec<FileSegment>,
    len: u64,
    /// Lives as long as the body: the download's Storage Reader Lease.
    _guard: Arc<dyn Any + Send + Sync>,
    /// Why sending must stop, if it must; checked between chunks.
    abort: Box<dyn Fn() -> Option<String> + Send + Sync>,
}

#[derive(Clone)]
pub enum FileSegment {
    /// Opened up front, so a missing file was noticed before the response.
    Open(Arc<std::fs::File>, u64),
    /// Opened when its turn comes.
    Path(PathBuf),
}

impl FileBody {
    pub fn new(
        segments: Vec<FileSegment>,
        len: u64,
        guard: Arc<dyn Any + Send + Sync>,
        abort: impl Fn() -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(FileBodyInner {
            segments,
            len,
            _guard: guard,
            abort: Box::new(abort),
        }))
    }

    /// Total length of the files.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.0.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.len == 0
    }

    #[must_use]
    pub fn segments(&self) -> &[FileSegment] {
        &self.0.segments
    }

    /// # Errors
    ///
    /// If sending must stop, for instance because the download lost its lease.
    pub fn check(&self) -> io::Result<()> {
        match (self.0.abort)() {
            Some(reason) => Err(io::Error::other(reason)),
            None => Ok(()),
        }
    }
}

#[derive(Clone)]
pub enum FileIo {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring(Arc<RingPool>),
    Tokio,
}

impl FileIo {
    /// `io_uring` on `pool`'s rings when there is a pool, otherwise blocking
    /// I/O on tokio's blocking pool.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    #[must_use]
    pub fn new(pool: Option<Arc<RingPool>>) -> Self {
        if let Some(pool) = pool {
            Self::Uring(pool)
        } else {
            tracing::info!("Filesystem storage I/O uses blocking I/O");
            Self::Tokio
        }
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
        let (file, size) = self.open(path).await?;
        Ok(self.read_open(file, size))
    }

    /// Opens `path` for reading, returning the file and its size.
    ///
    /// # Errors
    ///
    /// If the file can't be opened.
    pub async fn open(&self, path: PathBuf) -> io::Result<(Arc<std::fs::File>, u64)> {
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => {
                pool.run(move || async move {
                    let fd = crate::uring::open(&path, libc::O_RDONLY, 0).await?;
                    let size =
                        crate::uring::file_size(std::os::fd::AsRawFd::as_raw_fd(&fd)).await?;
                    Ok((Arc::new(std::fs::File::from(fd)), size))
                })
                .await?
            }
            Self::Tokio => {
                blocking(move || {
                    let file = std::fs::File::open(&path)?;
                    let size = file.metadata()?.len();
                    Ok((Arc::new(file), size))
                })
                .await
            }
        }
    }

    /// Streams the `size` bytes of an open file.
    #[must_use = "the stream reads nothing until polled"]
    pub fn read_open(&self, file: Arc<std::fs::File>, size: u64) -> ByteStream {
        match self {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(pool) => uring_io::read(pool, file, size),
            Self::Tokio => read_blocking(file, size),
        }
    }

    /// Streams the files of `body` one after another.
    #[must_use = "the stream reads nothing until polled"]
    pub fn read_segments(&self, body: &FileBody) -> ByteStream {
        let io = self.clone();
        let segments = body.segments().to_vec();
        Box::pin(async_stream::stream! {
            for segment in segments {
                let mut stream = match segment {
                    FileSegment::Open(file, size) => io.read_open(file, size),
                    FileSegment::Path(path) => match io.read_file(path).await {
                        Ok(stream) => stream,
                        Err(err) => {
                            yield Err(err);
                            return;
                        }
                    },
                };
                while let Some(chunk) = stream.next().await {
                    let failed = chunk.is_err();
                    yield chunk;
                    if failed {
                        return;
                    }
                }
            }
        })
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
            Self::Uring(pool) => uring_io::write(pool, path, batches).await,
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
                .run(move || async move { crate::uring::path_size(&path).await })
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
                pool.run(move || async move { crate::uring::rename(&from, &to).await })
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
                pool.run(move || async move { crate::uring::unlink(&path).await })
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
                pool.run(move || async move { crate::uring::create_dir_all(&path).await })
                    .await?
            }
            Self::Tokio => tokio::fs::create_dir_all(path).await,
        }
    }
}

/// Groups body frames into batches of about `CHUNK_SIZE` bytes, each written
/// with one vectored write instead of being copied together first.
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
pub fn consume(batch: &mut Vec<Bytes>, mut written: usize) {
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
fn read_blocking(file: Arc<std::fs::File>, size: u64) -> ByteStream {
    let read = move |file: Arc<std::fs::File>, position: u64| {
        tokio::task::spawn_blocking(move || {
            let mut chunk = vec![0; chunk_len(size, position)];
            let mut filled = 0;
            while filled < chunk.len() {
                match file.read_at(&mut chunk[filled..], position + filled as u64) {
                    Ok(0) => break,
                    Ok(read) => filled += read,
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                    Err(err) => return Err(err),
                }
            }
            chunk.truncate(filled);
            Ok::<_, io::Error>((file, chunk))
        })
    };
    Box::pin(async_stream::try_stream! {
        let mut position = 0;
        let mut next = (size > 0).then(|| read(file, 0));
        while let Some(pending) = next.take() {
            let (file, chunk) = pending.await.map_err(|err| join_error(&err))??;
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

fn join_error(err: &tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("blocking task failed: {err}"))
}

/// Runs blocking file operations on tokio's blocking pool: the ones `io_uring`
/// has no opcode for, and all file data without `io_uring`.
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
        .map_err(|err| join_error(&err))?
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_io {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::path::PathBuf;
    use std::sync::Arc;

    use bytes::{Bytes, BytesMut};
    use futures::{Stream, StreamExt};
    use tokio::sync::mpsc;

    use super::{ByteStream, chunk_len, consume};
    use crate::uring::{self, RingPool};

    /// Streams an open file, read on a ring with two chunks of read-ahead
    /// keeping the ring busy while the consumer sends.
    pub fn read(pool: &RingPool, file: Arc<std::fs::File>, size: u64) -> ByteStream {
        let (chunks_sender, chunks) = mpsc::channel::<io::Result<Bytes>>(2);
        let spawned = pool.spawn(move || async move {
            let fd = file.as_raw_fd();
            let mut position = 0u64;
            while position < size {
                // Sized to what is left, so a small file never holds a whole
                // chunk, and the end needs no extra read to find.
                let len = chunk_len(size, position);
                let (result, buffer) =
                    uring::read_at(fd, BytesMut::with_capacity(len), len, position).await;
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
        });
        if let Err(err) = spawned {
            return Box::pin(futures::stream::once(async move { Err(err) }));
        }
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(chunks))
    }

    pub async fn write<S>(pool: &RingPool, path: PathBuf, stream: S) -> io::Result<u64>
    where
        S: Stream<Item = io::Result<Vec<Bytes>>> + Send + 'static,
    {
        // The body stream is driven by the caller; only the file operations
        // run on the ring. One batch waits while another is written.
        let (batches_sender, mut batches) = mpsc::channel::<Vec<Bytes>>(1);
        let write = pool.run(move || async move {
            let file =
                uring::open(&path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o666).await?;
            let fd = file.as_raw_fd();
            let written = async {
                let mut position = 0u64;
                while let Some(mut batch) = batches.recv().await {
                    while !batch.is_empty() {
                        let (result, returned) = uring::writev_at(fd, batch, position).await;
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
            let closed = uring::close(file).await;
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
        assert_eq!(batch, [] as [&[u8]; 0]);
    }

    #[tokio::test]
    async fn blocking_round_trip() {
        round_trip(FileIo::Tokio).await;
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn io_uring_round_trip() {
        let pool = match RingPool::start(2) {
            Ok(pool) => std::sync::Arc::new(pool),
            Err(err) => {
                eprintln!("io_uring unavailable on this host ({err}), skipping");
                return;
            }
        };
        round_trip(FileIo::new(Some(pool))).await;
    }
}
