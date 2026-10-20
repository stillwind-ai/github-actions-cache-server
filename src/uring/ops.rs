//! The operations the server needs, each owning its buffers until the
//! kernel is done with them. Descriptors are borrowed as raw fds: the caller
//! keeps them open for the duration (the kernel holds its own reference once
//! an operation is submitted, so closing early is safe, merely pointless).

use std::ffi::CString;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use bytes::{Bytes, BytesMut};
use io_uring::{opcode, squeue, types};

use super::Op;

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))
}

fn len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// Receives into `buf`'s spare capacity, at most `max` bytes, returning the
/// count (0 at end of stream) with the buffer grown by it.
pub async fn recv(
    fd: RawFd,
    buf: BytesMut,
    max: usize,
    flags: i32,
) -> (io::Result<usize>, BytesMut) {
    let op = Op::submit(buf, |buf| {
        let spare = buf.capacity() - buf.len();
        // SAFETY: the spare capacity is allocated, and the buffer's heap data
        // doesn't move while the operation owns it.
        let ptr = unsafe { buf.as_mut_ptr().add(buf.len()) };
        opcode::Recv::new(types::Fd(fd), ptr, len_u32(spare.min(max)))
            .flags(flags)
            .build()
    });
    let op = match op {
        Ok(op) => op,
        Err(err) => return (Err(err), BytesMut::new()),
    };
    let (completion, mut buf) = op.await;
    let result = completion.into_result().map(|read| {
        let read = read as usize;
        // SAFETY: the kernel initialized `read` bytes of spare capacity.
        unsafe { buf.set_len(buf.len() + read) };
        read
    });
    (result, *buf)
}

struct Message {
    buffers: Vec<Bytes>,
    iovecs: Vec<libc::iovec>,
    header: libc::msghdr,
}

/// Sends `buffers` with one `sendmsg`, returning how much went out.
pub async fn send(fd: RawFd, buffers: Vec<Bytes>, flags: i32) -> (io::Result<usize>, Vec<Bytes>) {
    let iovecs: Vec<libc::iovec> = buffers
        .iter()
        .map(|buffer| libc::iovec {
            iov_base: buffer.as_ptr().cast_mut().cast(),
            iov_len: buffer.len(),
        })
        .collect();
    // SAFETY: an all-zero msghdr is a valid empty one.
    let header: libc::msghdr = unsafe { std::mem::zeroed() };
    let message = Message {
        buffers,
        iovecs,
        header,
    };
    let op = Op::submit(message, |message| {
        message.header.msg_iov = message.iovecs.as_mut_ptr();
        message.header.msg_iovlen = message.iovecs.len() as _;
        opcode::SendMsg::new(types::Fd(fd), &raw const message.header)
            .flags(u32::try_from(flags).unwrap_or(0))
            .build()
    });
    let op = match op {
        Ok(op) => op,
        Err(err) => return (Err(err), Vec::new()),
    };
    let (completion, message) = op.await;
    (
        completion.into_result().map(|sent| sent as usize),
        message.buffers,
    )
}

/// Reads up to `len` bytes at `offset` into `buf`'s spare capacity.
pub async fn read_at(
    fd: RawFd,
    buf: BytesMut,
    len: usize,
    offset: u64,
) -> (io::Result<usize>, BytesMut) {
    let op = Op::submit(buf, |buf| {
        let spare = buf.capacity() - buf.len();
        // SAFETY: as for `recv`.
        let ptr = unsafe { buf.as_mut_ptr().add(buf.len()) };
        opcode::Read::new(types::Fd(fd), ptr, len_u32(spare.min(len)))
            .offset(offset)
            .build()
    });
    let op = match op {
        Ok(op) => op,
        Err(err) => return (Err(err), BytesMut::new()),
    };
    let (completion, mut buf) = op.await;
    let result = completion.into_result().map(|read| {
        let read = read as usize;
        // SAFETY: the kernel initialized `read` bytes of spare capacity.
        unsafe { buf.set_len(buf.len() + read) };
        read
    });
    (result, *buf)
}

struct Vectored {
    buffers: Vec<Bytes>,
    iovecs: Vec<libc::iovec>,
}

/// Writes `buffers` at `offset` with one `pwritev`.
pub async fn writev_at(
    fd: RawFd,
    buffers: Vec<Bytes>,
    offset: u64,
) -> (io::Result<usize>, Vec<Bytes>) {
    let iovecs = buffers
        .iter()
        .map(|buffer| libc::iovec {
            iov_base: buffer.as_ptr().cast_mut().cast(),
            iov_len: buffer.len(),
        })
        .collect();
    let op = Op::submit(Vectored { buffers, iovecs }, |vectored| {
        opcode::Writev::new(
            types::Fd(fd),
            vectored.iovecs.as_ptr(),
            len_u32(vectored.iovecs.len()),
        )
        .offset(offset)
        .build()
    });
    let op = match op {
        Ok(op) => op,
        Err(err) => return (Err(err), Vec::new()),
    };
    let (completion, vectored) = op.await;
    (
        completion.into_result().map(|written| written as usize),
        vectored.buffers,
    )
}

/// `splice(2)`: moves up to `len` bytes between a pipe and another
/// descriptor inside the kernel. An offset of -1 means the descriptor's own
/// position (required for pipes and sockets).
///
/// # Errors
///
/// The `errno` splice fails with.
pub async fn splice(
    fd_in: RawFd,
    off_in: i64,
    fd_out: RawFd,
    off_out: i64,
    len: usize,
    flags: u32,
) -> io::Result<usize> {
    let (completion, ()) = Op::submit((), |()| {
        opcode::Splice::new(
            types::Fd(fd_in),
            off_in,
            types::Fd(fd_out),
            off_out,
            len_u32(len),
        )
        .flags(flags)
        .build()
    })?
    .await
    .map_resources();
    Ok(completion.into_result()? as usize)
}

/// Splices up to `len` bytes from `fd_in` at `off_in` into a pipe and from
/// the pipe on to `fd_out`, as two linked operations: the second starts in
/// the kernel as soon as the first completes, without a round trip through
/// the ring. Returns what each moved. When the first moves less than `len`
/// (end of file, or an error) the second is cancelled (`ECANCELED`), and the
/// pipe holds whatever the first moved.
pub async fn splice_through_pipe(
    fd_in: RawFd,
    off_in: i64,
    pipe: (RawFd, RawFd),
    fd_out: RawFd,
    len: usize,
    flags_out: u32,
) -> (io::Result<usize>, io::Result<usize>) {
    let (pipe_read, pipe_write) = pipe;
    let Some(ring) = super::Ring::current() else {
        let err = || io::Error::other("io_uring operation outside of an io_uring thread");
        return (Err(err()), Err(err()));
    };
    // A link only holds within one submission.
    ring.reserve(2);
    let first = Op::submit((), |()| {
        opcode::Splice::new(
            types::Fd(fd_in),
            off_in,
            types::Fd(pipe_write),
            -1,
            len_u32(len),
        )
        .build()
        .flags(squeue::Flags::IO_LINK)
    });
    let first = match first {
        Ok(first) => first,
        Err(err) => return (Err(err), Err(io::ErrorKind::Interrupted.into())),
    };
    let second = Op::submit((), |()| {
        opcode::Splice::new(
            types::Fd(pipe_read),
            -1,
            types::Fd(fd_out),
            -1,
            len_u32(len),
        )
        .flags(flags_out)
        .build()
    });
    let second = match second {
        Ok(second) => second,
        Err(err) => {
            return (
                first.await.0.into_result().map(|moved| moved as usize),
                Err(err),
            );
        }
    };
    let ((moved_in, ()), (moved_out, ())) =
        futures::join!(async { first.await.map_resources() }, async {
            second.await.map_resources()
        });
    (
        moved_in.into_result().map(|moved| moved as usize),
        moved_out.into_result().map(|moved| moved as usize),
    )
}

/// Accepts a connection on a listening socket.
///
/// # Errors
///
/// The `errno` accept fails with.
pub async fn accept(fd: RawFd) -> io::Result<OwnedFd> {
    let (completion, ()) = Op::submit((), |()| {
        opcode::Accept::new(types::Fd(fd), std::ptr::null_mut(), std::ptr::null_mut())
            .flags(libc::SOCK_CLOEXEC)
            .build()
    })?
    .await
    .map_resources();
    let fd = completion.into_result()?;
    // SAFETY: accept returned a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd.cast_signed()) })
}

/// Opens `path` (`openat(2)` relative to the working directory).
///
/// # Errors
///
/// The `errno` open fails with.
pub async fn open(path: &Path, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    let path = c_path(path)?;
    let (completion, _) = Op::submit(path, |path| {
        opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
            .flags(flags | libc::O_CLOEXEC)
            .mode(mode)
            .build()
    })?
    .await;
    let fd = completion.into_result()?;
    // SAFETY: open returned a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd.cast_signed()) })
}

struct Stat {
    path: CString,
    statx: libc::statx,
}

async fn statx(dirfd: RawFd, path: CString, flags: i32) -> io::Result<libc::statx> {
    // SAFETY: an all-zero statx is valid output space.
    let statx: libc::statx = unsafe { std::mem::zeroed() };
    let (completion, stat) = Op::submit(Stat { path, statx }, |stat| {
        opcode::Statx::new(
            types::Fd(dirfd),
            stat.path.as_ptr(),
            (&raw mut stat.statx).cast(),
        )
        .flags(flags)
        .mask(libc::STATX_SIZE)
        .build()
    })?
    .await;
    completion.into_result()?;
    Ok(stat.statx)
}

/// Size of an open file.
///
/// # Errors
///
/// The `errno` statx fails with.
pub async fn file_size(fd: RawFd) -> io::Result<u64> {
    let empty = CString::default();
    Ok(statx(fd, empty, libc::AT_EMPTY_PATH).await?.stx_size)
}

/// Size of the file at `path`.
///
/// # Errors
///
/// The `errno` statx fails with.
pub async fn path_size(path: &Path) -> io::Result<u64> {
    Ok(statx(libc::AT_FDCWD, c_path(path)?, 0).await?.stx_size)
}

/// # Errors
///
/// The `errno` renameat fails with.
pub async fn rename(from: &Path, to: &Path) -> io::Result<()> {
    let paths = (c_path(from)?, c_path(to)?);
    let (completion, _) = Op::submit(paths, |(from, to)| {
        opcode::RenameAt::new(
            types::Fd(libc::AT_FDCWD),
            from.as_ptr(),
            types::Fd(libc::AT_FDCWD),
            to.as_ptr(),
        )
        .build()
    })?
    .await;
    completion.into_result().map(drop)
}

/// # Errors
///
/// The `errno` unlinkat fails with.
pub async fn unlink(path: &Path) -> io::Result<()> {
    let (completion, _) = Op::submit(c_path(path)?, |path| {
        opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr()).build()
    })?
    .await;
    completion.into_result().map(drop)
}

/// # Errors
///
/// The `errno` mkdirat fails with.
pub async fn mkdir(path: &Path, mode: u32) -> io::Result<()> {
    let (completion, _) = Op::submit(c_path(path)?, |path| {
        opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
            .mode(mode)
            .build()
    })?
    .await;
    completion.into_result().map(drop)
}

/// Creates `path` and any missing parents.
///
/// # Errors
///
/// If a directory can't be created.
pub async fn create_dir_all(path: &Path) -> io::Result<()> {
    // Most calls find the parent present: try the leaf first.
    match mkdir(path, 0o777).await {
        Ok(()) => return Ok(()),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => return Ok(()),
        Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
        Err(_) => {}
    }
    let mut missing = vec![path];
    let mut current = path;
    while let Some(parent) = current.parent() {
        if parent.as_os_str().is_empty() {
            break;
        }
        match mkdir(parent, 0o777).await {
            Ok(()) => break,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => break,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                missing.push(parent);
                current = parent;
            }
            Err(err) => return Err(err),
        }
    }
    for dir in missing.into_iter().rev() {
        match mkdir(dir, 0o777).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Closes a descriptor through the ring.
///
/// # Errors
///
/// The `errno` close fails with (the descriptor is gone either way).
pub async fn close(fd: OwnedFd) -> io::Result<()> {
    use std::os::fd::IntoRawFd;
    let raw = fd.into_raw_fd();
    let op = Op::submit((), |()| opcode::Close::new(types::Fd(raw)).build());
    match op {
        Ok(op) => op.await.0.into_result().map(drop),
        Err(err) => {
            // SAFETY: still ours: the close was never submitted.
            drop(unsafe { OwnedFd::from_raw_fd(raw) });
            Err(err)
        }
    }
}

trait MapResources<T> {
    fn map_resources(self) -> (super::Completion, T);
}

impl<T> MapResources<T> for (super::Completion, Box<T>) {
    fn map_resources(self) -> (super::Completion, T) {
        (self.0, *self.1)
    }
}
