//! Threads that each own a ring and run a current-thread tokio runtime
//! around it. Connections are served on them, and the multi-threaded
//! runtime hands them file operations over channels.

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{mpsc, oneshot};

use super::Ring;

/// Work for a ring thread: a closure producing a `!Send` future, which runs
/// as a local task there.
pub type Job = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send>;

/// Submission queue entries per ring; completions get four times as many.
const RING_ENTRIES: u32 = 1024;

thread_local! {
    /// The ring thread's local task set. Spawning through it (rather than
    /// `spawn_local`) also works from tasks started with `tokio::spawn`,
    /// which run on the thread but outside the set.
    static LOCAL_SET: RefCell<Option<Rc<tokio::task::LocalSet>>> = const { RefCell::new(None) };
}

/// Runs `job` as a local task of the current ring thread.
fn spawn_here(job: Job) -> io::Result<()> {
    LOCAL_SET.with(|local| match local.borrow().as_ref() {
        Some(local) => {
            local.spawn_local(job());
            Ok(())
        }
        None => Err(io::Error::other("io_uring thread has stopped")),
    })
}

pub struct RingPool {
    workers: Vec<mpsc::UnboundedSender<Job>>,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    next: AtomicUsize,
}

impl RingPool {
    /// Starts `threads` ring threads.
    ///
    /// # Errors
    ///
    /// If the kernel refuses `io_uring` (seccomp profiles and
    /// `kernel.io_uring_disabled` do) or a thread can't be started.
    pub fn start(threads: usize) -> io::Result<Self> {
        let mut workers = Vec::with_capacity(threads);
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads.max(1) {
            let (sender, mut jobs) = mpsc::unbounded_channel::<Job>();
            let (started_sender, started) = std::sync::mpsc::channel::<io::Result<()>>();
            let handle = std::thread::Builder::new()
                .name(format!("io-uring-{index}"))
                .spawn(move || {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(err) => {
                            let _ = started_sender.send(Err(err));
                            return;
                        }
                    };
                    let local = Rc::new(tokio::task::LocalSet::new());
                    LOCAL_SET.with(|set| *set.borrow_mut() = Some(local.clone()));
                    local.block_on(&runtime, async move {
                        let ring = match Ring::new(RING_ENTRIES) {
                            Ok(ring) => ring,
                            Err(err) => {
                                let _ = started_sender.send(Err(err));
                                return;
                            }
                        };
                        Ring::set_current(Some(ring.clone()));
                        tokio::task::spawn_local(ring.drive());
                        let _ = started_sender.send(Ok(()));
                        // Runs until the pool is dropped.
                        while let Some(job) = jobs.recv().await {
                            tokio::task::spawn_local(job());
                        }
                    });
                    // Tasks still holding operations go first, then the ring.
                    LOCAL_SET.with(|set| set.borrow_mut().take());
                    drop(local);
                    Ring::set_current(None);
                })?;
            match started.recv() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err(io::Error::other("io_uring thread failed to start")),
            }
            workers.push(sender);
            handles.push(handle);
        }
        Ok(Self {
            workers,
            threads: Mutex::new(handles),
            next: AtomicUsize::new(0),
        })
    }

    #[must_use]
    pub fn threads(&self) -> usize {
        self.workers.len()
    }

    /// Runs `job` as a local task on ring thread `worker`.
    ///
    /// # Errors
    ///
    /// If that thread has stopped.
    pub fn spawn_on(&self, worker: usize, job: Job) -> io::Result<()> {
        self.workers[worker % self.workers.len()]
            .send(job)
            .map_err(|_| io::Error::other("io_uring thread has stopped"))
    }

    /// Runs `f`'s future on a ring and returns its output: as a local task
    /// when called on a ring thread (any pool's), otherwise on the next
    /// thread of this pool. The future runs to completion even if the caller
    /// stops waiting.
    ///
    /// # Errors
    ///
    /// If the ring thread has stopped.
    pub async fn run<F, Fut, T>(&self, f: F) -> io::Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        self.spawn(move || async move {
            let _ = sender.send(f().await);
        })?;
        receiver
            .await
            .map_err(|_| io::Error::other("io_uring thread dropped the operation"))
    }

    /// Spawns `f`'s future as a local task on the current ring thread, or
    /// on the next thread of this pool.
    ///
    /// # Errors
    ///
    /// If the ring thread has stopped.
    pub fn spawn<F, Fut>(&self, f: F) -> io::Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let job: Job = Box::new(move || Box::pin(f()));
        if Ring::on_ring_thread() {
            return spawn_here(job);
        }
        let worker = self.next.fetch_add(1, Ordering::Relaxed);
        self.spawn_on(worker, job)
    }
}

impl Drop for RingPool {
    fn drop(&mut self) {
        // Closing the channels ends each thread's job loop.
        self.workers.clear();
        let threads = std::mem::take(&mut *self.threads.lock().expect("not poisoned"));
        // A ring thread can't wait for itself; it finishes on its own.
        if !Ring::on_ring_thread() {
            for thread in threads {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Bytes, BytesMut};

    use super::*;
    use crate::uring;

    fn pool() -> Option<RingPool> {
        match RingPool::start(2) {
            Ok(pool) => Some(pool),
            Err(err) => {
                eprintln!("io_uring unavailable on this host ({err}), skipping");
                None
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn file_operations() {
        let Some(pool) = pool() else { return };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/b/c/file");
        let result = pool
            .run(move || async move {
                uring::create_dir_all(path.parent().unwrap()).await?;
                let fd = uring::open(&path, libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC, 0o644)
                    .await?;
                let raw = std::os::fd::AsRawFd::as_raw_fd(&fd);
                let (written, _) = uring::writev_at(
                    raw,
                    vec![Bytes::from_static(b"hello "), Bytes::from_static(b"world")],
                    0,
                )
                .await;
                assert_eq!(written?, 11);
                assert_eq!(uring::file_size(raw).await?, 11);
                uring::close(fd).await?;
                assert_eq!(uring::path_size(&path).await?, 11);

                let fd = uring::open(&path, libc::O_RDONLY, 0).await?;
                let raw = std::os::fd::AsRawFd::as_raw_fd(&fd);
                let (read, buf) = uring::read_at(raw, BytesMut::with_capacity(64), 64, 6).await;
                assert_eq!(read?, 5);
                assert_eq!(&buf[..], b"world");

                let renamed = path.with_file_name("renamed");
                uring::rename(&path, &renamed).await?;
                assert_eq!(
                    uring::path_size(&path).await.unwrap_err().kind(),
                    io::ErrorKind::NotFound
                );
                uring::unlink(&renamed).await?;
                Ok::<_, io::Error>(())
            })
            .await
            .unwrap();
        result.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sockets_and_splice() {
        let Some(pool) = pool() else { return };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let listener = Arc::new(std::os::fd::OwnedFd::from(listener));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("payload");
        let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&file, &payload).unwrap();

        let server = pool.run(move || async move {
            let raw = std::os::fd::AsRawFd::as_raw_fd(&*listener);
            let socket = uring::accept(raw).await?;
            let socket_fd = std::os::fd::AsRawFd::as_raw_fd(&socket);
            let (received, buf) =
                uring::recv(socket_fd, BytesMut::with_capacity(16), 5, libc::MSG_WAITALL).await;
            assert_eq!(received?, 5);
            assert_eq!(&buf[..], b"hello");
            let (sent, _) = uring::send(
                socket_fd,
                vec![Bytes::from_static(b"head:")],
                libc::MSG_NOSIGNAL,
            )
            .await;
            assert_eq!(sent?, 5);

            // File to socket through a pipe, as downloads go out.
            let file = uring::open(&file, libc::O_RDONLY, 0).await?;
            let file_fd = std::os::fd::AsRawFd::as_raw_fd(&file);
            let mut fds = [0; 2];
            // SAFETY: pipe2 fills both descriptors on success.
            assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
            let mut offset = 0i64;
            loop {
                let moved = uring::splice(file_fd, offset, fds[1], -1, 65536, 0).await?;
                if moved == 0 {
                    break;
                }
                offset += i64::try_from(moved).unwrap();
                let mut left = moved;
                while left > 0 {
                    left -= uring::splice(fds[0], -1, socket_fd, -1, left, 0).await?;
                }
            }
            // SAFETY: the pipe is ours.
            unsafe {
                libc::close(fds[0]);
                libc::close(fds[1]);
            }
            Ok::<_, io::Error>(offset)
        });

        let client = tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut stream = std::net::TcpStream::connect(addr).unwrap();
            stream.write_all(b"hello").unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).unwrap();
            received
        });
        let spliced = server.await.unwrap().unwrap();
        assert_eq!(spliced, i64::try_from(payload.len()).unwrap());
        let received = client.await.unwrap();
        assert_eq!(&received[..5], b"head:");
        assert_eq!(&received[5..], &payload[..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn linked_splices_stop_at_the_end_of_the_file() {
        let Some(pool) = pool() else { return };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short");
        std::fs::write(&path, [1u8; 100]).unwrap();
        let (sink, mut drain) = std::os::unix::net::UnixStream::pair().unwrap();
        let moved = pool
            .run(move || async move {
                let file = std::fs::File::open(&path).unwrap();
                let mut fds = [0; 2];
                // SAFETY: pipe2 fills both descriptors on success.
                assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
                // Asks for more than the file holds: the link must break
                // rather than leave the second splice waiting on the pipe.
                let (moved_in, moved_out) = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    uring::splice_through_pipe(
                        std::os::fd::AsRawFd::as_raw_fd(&file),
                        0,
                        (fds[0], fds[1]),
                        std::os::fd::AsRawFd::as_raw_fd(&sink),
                        65536,
                        0,
                    ),
                )
                .await
                .expect("linked splice finishes");
                assert_eq!(moved_out.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
                let moved = moved_in.unwrap();
                // What was moved is still in the pipe.
                let out = uring::splice(
                    fds[0],
                    -1,
                    std::os::fd::AsRawFd::as_raw_fd(&sink),
                    -1,
                    moved,
                    0,
                )
                .await
                .unwrap();
                // SAFETY: the pipe is ours.
                unsafe {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                }
                (moved, out)
            })
            .await
            .unwrap();
        assert_eq!(moved, (100, 100));
        let mut received = [0u8; 100];
        std::io::Read::read_exact(&mut drain, &mut received).unwrap();
        assert_eq!(received, [1u8; 100]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runs_operations_for_tasks_outside_the_local_set() {
        let Some(pool) = pool() else { return };
        let pool = Arc::new(pool);
        let inner = pool.clone();
        // Like a Merge a request handler starts with `tokio::spawn`.
        let found = pool
            .run(move || async move {
                tokio::spawn(async move {
                    inner
                        .run(|| async { uring::path_size(std::path::Path::new("/")).await.is_ok() })
                        .await
                        .unwrap()
                })
                .await
                .unwrap()
            })
            .await
            .unwrap();
        assert!(found);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abandoned_operations_are_cancelled() {
        let Some(pool) = pool() else { return };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listener = Arc::new(std::os::fd::OwnedFd::from(listener));
        pool.run(move || async move {
            let raw = std::os::fd::AsRawFd::as_raw_fd(&*listener);
            // Nobody connects: the accept is dropped while in flight.
            let timeout =
                tokio::time::timeout(std::time::Duration::from_millis(20), uring::accept(raw))
                    .await;
            assert!(timeout.is_err());
            // The ring keeps working.
            assert_eq!(
                uring::path_size(std::path::Path::new("/nonexistent"))
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::NotFound
            );
        })
        .await
        .unwrap();
    }
}
