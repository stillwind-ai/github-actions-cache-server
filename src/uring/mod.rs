//! A small `io_uring` driver for the critical path: client sockets and cache
//! files go through the same ring.
//!
//! Each ring belongs to one thread of a [`RingPool`], which runs a
//! current-thread tokio runtime around it. The ring's eventfd is registered
//! with that runtime, so completions wake the runtime like any other I/O, and
//! ordinary tokio code (the database pool, timers, channels) runs alongside.
//! Submissions are batched: an operation only queues its entry, and the
//! driver task submits everything queued since it last ran with one
//! `io_uring_enter`, which also collects completions.
//!
//! An operation owns everything the kernel reads or writes (buffers, paths,
//! `msghdr`s) until its completion arrives. Dropping an unfinished operation
//! hands those resources to the ring, which keeps them until the kernel is
//! done and asks the kernel to cancel the operation.

pub mod http;
mod ops;
mod pool;
pub mod server;

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use io_uring::{IoUring, opcode, squeue};
use tokio::io::unix::AsyncFd;

pub use self::ops::*;
pub use self::pool::RingPool;

const ENTER_GETEVENTS: u32 = 1;

/// `user_data` of cancellation requests, whose completions are ignored.
const CANCEL_TAG: u64 = u64::MAX;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Ring>>> = const { RefCell::new(None) };
}

/// What is known about an operation the kernel has been handed.
enum Slot {
    /// Submitted; the waker (if any) belongs to the task awaiting it.
    Pending(Option<Waker>),
    /// The kernel's result, not yet picked up by the operation.
    Done(Completion),
    /// The operation was dropped before completing. Its resources stay here
    /// until the kernel is done with them.
    Abandoned(Box<dyn Any>),
    Free,
}

#[derive(Clone, Copy, Debug)]
pub struct Completion {
    pub result: i32,
    pub flags: u32,
}

impl Completion {
    /// The result as a byte count or other non-negative value.
    ///
    /// # Errors
    ///
    /// The `errno` the operation failed with.
    pub fn into_result(self) -> io::Result<u32> {
        u32::try_from(self.result).map_err(|_| io::Error::from_raw_os_error(-self.result))
    }
}

pub struct Ring {
    uring: RefCell<IoUring>,
    slots: RefCell<Vec<Slot>>,
    free: RefCell<Vec<usize>>,
    eventfd: AsyncFd<OwnedFd>,
    /// Entries are queued that the driver has not submitted yet.
    unsubmitted: Cell<bool>,
    driver: RefCell<Option<Waker>>,
    /// Completions are only posted from inside `io_uring_enter`.
    defer_taskrun: bool,
}

impl Ring {
    /// A ring for the current thread, with the most efficient setup the
    /// kernel accepts: a single submitting thread whose completions are
    /// processed when it asks for them (Linux 6.1), cooperative task work
    /// (5.19), or the defaults.
    fn new(entries: u32) -> io::Result<Rc<Self>> {
        let deferred = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_submit_all()
            .setup_cqsize(entries * 4)
            .build(entries);
        let (uring, defer_taskrun) = match deferred {
            Ok(uring) => (uring, true),
            Err(_) => match IoUring::builder()
                .setup_coop_taskrun()
                .setup_submit_all()
                .setup_cqsize(entries * 4)
                .build(entries)
            {
                Ok(uring) => (uring, false),
                Err(_) => (IoUring::new(entries)?, false),
            },
        };

        // SAFETY: eventfd returns a new descriptor or -1.
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if eventfd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: just created and owned by nobody else.
        let eventfd = unsafe { OwnedFd::from_raw_fd(eventfd) };
        uring.submitter().register_eventfd(eventfd.as_raw_fd())?;

        Ok(Rc::new(Self {
            uring: RefCell::new(uring),
            slots: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
            eventfd: AsyncFd::with_interest(eventfd, tokio::io::Interest::READABLE)?,
            unsubmitted: Cell::new(false),
            driver: RefCell::new(None),
            defer_taskrun,
        }))
    }

    /// The ring of the current thread, if it is a [`RingPool`] thread.
    #[must_use]
    pub fn current() -> Option<Rc<Self>> {
        CURRENT.with(|current| current.borrow().clone())
    }

    /// Whether the current thread is a [`RingPool`] thread.
    #[must_use]
    pub fn on_ring_thread() -> bool {
        CURRENT.with(|current| current.borrow().is_some())
    }

    fn set_current(ring: Option<Rc<Self>>) {
        CURRENT.with(|current| *current.borrow_mut() = ring);
    }

    /// Queues an entry; the driver submits it.
    fn push(&self, entry: &squeue::Entry) {
        loop {
            {
                let mut uring = self.uring.borrow_mut();
                // SAFETY: every entry's pointers stay valid until its
                // completion, as the operation (or its abandoned slot) owns
                // what they point to.
                if unsafe { uring.submission().push(entry) }.is_ok() {
                    break;
                }
            }
            // The submission queue is full: make room now.
            self.submit_and_reap();
        }
        if !self.unsubmitted.replace(true)
            && let Some(driver) = self.driver.borrow().as_ref()
        {
            driver.wake_by_ref();
        }
    }

    /// Makes room for `entries` entries in the submission queue, so the next
    /// ones pushed go to the kernel together.
    fn reserve(&self, entries: usize) {
        let free = {
            let mut uring = self.uring.borrow_mut();
            let sq = uring.submission();
            sq.capacity() - sq.len()
        };
        if free < entries {
            self.submit_and_reap();
        }
    }

    fn insert(&self) -> usize {
        let mut slots = self.slots.borrow_mut();
        if let Some(index) = self.free.borrow_mut().pop() {
            slots[index] = Slot::Pending(None);
            index
        } else {
            slots.push(Slot::Pending(None));
            slots.len() - 1
        }
    }

    fn release(&self, index: usize) {
        self.slots.borrow_mut()[index] = Slot::Free;
        self.free.borrow_mut().push(index);
    }

    /// Submits what is queued and dispatches the completions that are ready.
    fn submit_and_reap(&self) {
        self.unsubmitted.set(false);
        let mut completions: Vec<(u64, Completion)> = Vec::new();
        {
            let mut uring = self.uring.borrow_mut();
            let pending = uring.submission().len();
            let wants_events = self.defer_taskrun
                || uring.submission().taskrun()
                || uring.submission().cq_overflow();
            if pending > 0 || wants_events {
                loop {
                    // SAFETY: plain io_uring_enter without an argument.
                    let entered = unsafe {
                        uring.submitter().enter::<libc::sigset_t>(
                            u32::try_from(pending).unwrap_or(u32::MAX),
                            0,
                            ENTER_GETEVENTS,
                            None,
                        )
                    };
                    match entered {
                        Ok(_) => break,
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                        // The completion queue is full: reap first, submit on
                        // the next pass.
                        Err(err)
                            if matches!(err.raw_os_error(), Some(libc::EBUSY | libc::EAGAIN)) =>
                        {
                            self.unsubmitted.set(true);
                            break;
                        }
                        Err(err) => {
                            tracing::error!(error = %err, "io_uring_enter failed");
                            break;
                        }
                    }
                }
            }
            let mut cq = uring.completion();
            cq.sync();
            completions.extend(cq.map(|cqe| {
                (
                    cqe.user_data(),
                    Completion {
                        result: cqe.result(),
                        flags: cqe.flags(),
                    },
                )
            }));
        }
        for (user_data, completion) in completions {
            if user_data == CANCEL_TAG {
                continue;
            }
            let Ok(index) = usize::try_from(user_data) else {
                continue;
            };
            let slot =
                std::mem::replace(&mut self.slots.borrow_mut()[index], Slot::Done(completion));
            match slot {
                Slot::Pending(waker) => {
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
                Slot::Abandoned(resources) => {
                    self.release(index);
                    drop(resources);
                }
                Slot::Done(_) | Slot::Free => {
                    tracing::error!(index, "io_uring completion for an unknown operation");
                }
            }
        }
        if self.unsubmitted.get()
            && let Some(driver) = self.driver.borrow().as_ref()
        {
            driver.wake_by_ref();
        }
    }

    /// Runs the ring until the thread's runtime stops: submits queued
    /// entries and dispatches completions whenever the eventfd fires.
    async fn drive(self: Rc<Self>) {
        std::future::poll_fn(|cx| {
            {
                let mut driver = self.driver.borrow_mut();
                if !driver
                    .as_ref()
                    .is_some_and(|waker| waker.will_wake(cx.waker()))
                {
                    *driver = Some(cx.waker().clone());
                }
            }
            loop {
                let mut work = self.unsubmitted.get();
                match self.eventfd.poll_read_ready(cx) {
                    // Cleared before reaping, so a completion that arrives
                    // meanwhile fires the eventfd again. The counter is never
                    // read: each signal is a new edge.
                    Poll::Ready(Ok(mut guard)) => {
                        guard.clear_ready();
                        work = true;
                    }
                    Poll::Ready(Err(err)) => {
                        tracing::error!(error = %err, "io_uring eventfd failed");
                        return Poll::Ready(());
                    }
                    Poll::Pending => {}
                }
                if !work {
                    return Poll::Pending;
                }
                self.submit_and_reap();
            }
        })
        .await;
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // Unfinished operations may still be written by the kernel until the
        // ring is torn down, after this returns: leak what they own.
        for slot in self.slots.get_mut().drain(..) {
            if let Slot::Abandoned(resources) = slot {
                std::mem::forget(resources);
            }
        }
    }
}

/// An operation in flight, resolving to its completion and the resources
/// it was given. `T` is boxed so the addresses the kernel holds stay put.
pub struct Op<T: 'static> {
    ring: Rc<Ring>,
    index: usize,
    resources: Option<Box<T>>,
}

impl<T: 'static> Op<T> {
    /// Queues the entry `build` makes from the (already boxed) resources.
    ///
    /// # Errors
    ///
    /// If the current thread has no ring.
    pub fn submit(resources: T, build: impl FnOnce(&mut T) -> squeue::Entry) -> io::Result<Self> {
        let ring = Ring::current()
            .ok_or_else(|| io::Error::other("io_uring operation outside of an io_uring thread"))?;
        let mut resources = Box::new(resources);
        let index = ring.insert();
        let entry = build(&mut resources).user_data(index as u64);
        ring.push(&entry);
        Ok(Self {
            ring,
            index,
            resources: Some(resources),
        })
    }
}

impl<T: 'static> Future for Op<T> {
    type Output = (Completion, Box<T>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut slots = this.ring.slots.borrow_mut();
        match &mut slots[this.index] {
            Slot::Done(completion) => {
                let completion = *completion;
                drop(slots);
                this.ring.release(this.index);
                let resources = this.resources.take().expect("polled after completion");
                Poll::Ready((completion, resources))
            }
            Slot::Pending(waker) => {
                if !waker
                    .as_ref()
                    .is_some_and(|waker| waker.will_wake(cx.waker()))
                {
                    *waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
            Slot::Abandoned(_) | Slot::Free => unreachable!("operation polled after release"),
        }
    }
}

impl<T: 'static> Drop for Op<T> {
    fn drop(&mut self) {
        let Some(resources) = self.resources.take() else {
            return;
        };
        let done = {
            let mut slots = self.ring.slots.borrow_mut();
            let slot = &mut slots[self.index];
            if matches!(slot, Slot::Done(_)) {
                true
            } else {
                *slot = Slot::Abandoned(resources);
                false
            }
        };
        if done {
            self.ring.release(self.index);
        } else {
            // The slot is only reused after this operation's completion is
            // reaped, which happens after this cancellation is submitted.
            let cancel = opcode::AsyncCancel::new(self.index as u64)
                .build()
                .user_data(CANCEL_TAG);
            self.ring.push(&cancel);
        }
    }
}
