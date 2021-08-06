#![allow(non_camel_case_types)]

use std::cell::UnsafeCell;
use std::fmt;
use std::fs::File;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::{TcpListener, TcpStream};
use std::ops::Neg;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::ptr::null_mut;
use std::slice::from_raw_parts_mut;
use std::sync::{atomic::*, *};
use std::{convert::TryFrom, io};

use libc::{c_int, c_long, c_uint, syscall};

use super::Lazy;

pub static IO_URING: Lazy<Uring, fn() -> Uring> =
    Lazy::new(|| Config::default().start().expect("unable to start io_uring"));

const IORING_OP_NOP: u8 = 0;
const IORING_OP_READV: u8 = 1;
const IORING_OP_WRITEV: u8 = 2;
const IORING_OP_FSYNC: u8 = 3;
const IORING_OP_READ_FIXED: u8 = 4;
const IORING_OP_WRITE_FIXED: u8 = 5;
const IORING_OP_POLL_ADD: u8 = 6;
const IORING_OP_POLL_REMOVE: u8 = 7;
const IORING_OP_SYNC_FILE_RANGE: u8 = 8;
const IORING_OP_SENDMSG: u8 = 9;
const IORING_OP_RECVMSG: u8 = 10;
const IORING_OP_TIMEOUT: u8 = 11;
const IORING_OP_TIMEOUT_REMOVE: u8 = 12;
const IORING_OP_ACCEPT: u8 = 13;
const IORING_OP_ASYNC_CANCEL: u8 = 14;
const IORING_OP_LINK_TIMEOUT: u8 = 15;
const IORING_OP_CONNECT: u8 = 16;
const IORING_OP_FALLOCATE: u8 = 17;
const IORING_OP_OPENAT: u8 = 18;
const IORING_OP_CLOSE: u8 = 19;
const IORING_OP_FILES_UPDATE: u8 = 20;
const IORING_OP_STATX: u8 = 21;
const IORING_OP_READ: u8 = 22;
const IORING_OP_WRITE: u8 = 23;
const IORING_OP_FADVISE: u8 = 24;
const IORING_OP_MADVISE: u8 = 25;
const IORING_OP_SEND: u8 = 26;
const IORING_OP_RECV: u8 = 27;
const IORING_OP_OPENAT2: u8 = 28;
const IORING_OP_LAST: u8 = 29;
const IOSQE_FIXED_FILE: u8 = 1;
const IOSQE_IO_DRAIN: u8 = 2;
const IOSQE_IO_LINK: u8 = 4;
const IOSQE_IO_HARDLINK: u8 = 8;
const IOSQE_ASYNC: u8 = 16;
const IOSQE_PERSONALITY: u8 = 32;
const IORING_SETUP_IOPOLL: u32 = 1;
const IORING_SETUP_SQPOLL: u32 = 2;
const IORING_SETUP_SQ_AFF: u32 = 4;
const IORING_SETUP_CQSIZE: u32 = 8;
const IORING_SETUP_CLAMP: u32 = 16;
const IORING_FSYNC_DATASYNC: u8 = 1;
const IORING_TIMEOUT_ABS: u32 = 1;
const IORING_OFF_SQ_RING: i64 = 0;
const IORING_OFF_CQ_RING: i64 = 0x0800_0000;
const IORING_OFF_SQES: i64 = 0x1000_0000;
const IORING_SQ_NEED_WAKEUP: u32 = 1;
const IORING_ENTER_GETEVENTS: u32 = 1;
const IORING_ENTER_SQ_WAKEUP: u32 = 2;
const IORING_FEAT_SINGLE_MMAP: u32 = 1;
const IORING_FEAT_NODROP: u32 = 2;
const IORING_FEAT_SUBMIT_STABLE: u32 = 4;
const IORING_FEAT_RW_CUR_POS: u32 = 8;
const IORING_REGISTER_BUFFERS: u32 = 0;
const IORING_UNREGISTER_BUFFERS: u32 = 1;
const IORING_REGISTER_FILES: u32 = 2;
const IORING_UNREGISTER_FILES: u32 = 3;
const IORING_REGISTER_EVENTFD: u32 = 4;
const IORING_UNREGISTER_EVENTFD: u32 = 5;
const IORING_REGISTER_FILES_UPDATE: u32 = 6;
const IORING_REGISTER_EVENTFD_ASYNC: u32 = 7;

/// Configuration for the underlying `io_uring` system.
#[derive(Clone, Debug, Copy)]
pub struct Config {
    /// The number of entries in the submission queue.
    /// The completion queue size may be specified by
    /// using `raw_params` instead. By default, the
    /// kernel will choose a completion queue that is 2x
    /// the submission queue's size.
    pub depth: usize,
    /// Enable `SQPOLL` mode, which spawns a kernel
    /// thread that polls for submissions without
    /// needing to block as often to submit.
    ///
    /// This is a privileged operation, and
    /// will cause `start` to fail if run
    /// by a non-privileged user.
    pub sq_poll: bool,
    /// Specify a particular CPU to pin the
    /// `SQPOLL` thread onto.
    pub sq_poll_affinity: u32,
    /// Specify that the user will directly
    /// poll the hardware for operation completion
    /// rather than using the completion queue.
    ///
    /// CURRENTLY UNSUPPORTED
    pub io_poll: bool,
    /// setting `raw_params` overrides everything else
    pub raw_params: Option<Params>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            depth: 256,
            sq_poll: false,
            io_poll: false,
            sq_poll_affinity: 0,
            raw_params: None,
        }
    }
}

impl Config {
    /// Start the io_uring system.
    pub fn start(mut self) -> io::Result<Uring> {
        let mut params = if let Some(params) = self.raw_params.take() {
            params
        } else {
            let mut params = Params::default();

            if self.sq_poll {
                // set SQPOLL mode to avoid needing wakeup
                params.flags = IORING_SETUP_SQPOLL;
                params.sq_thread_cpu = self.sq_poll_affinity;
            }

            params
        };

        let params_ptr: *mut Params = &mut params;

        let ring_fd = setup(u32::try_from(self.depth).unwrap(), params_ptr)?;

        if ring_fd < 0 {
            let mut err = io::Error::last_os_error();
            if let Some(12) = err.raw_os_error() {
                err = io::Error::new(
                    io::ErrorKind::Other,
                    "Not enough lockable memory. You probably \
                 need to raise the memlock rlimit, which \
                 often defaults to a pretty low number.",
                );
            }
            return Err(err);
        }

        let in_flight = Arc::new(InFlight::new(params.cq_entries as usize));

        let ticket_queue = Arc::new(TicketQueue::new(params.cq_entries as usize));

        let sq = Sq::new(&params, ring_fd)?;
        let cq = Cq::new(&params, ring_fd, in_flight.clone(), ticket_queue.clone())?;

        std::thread::spawn(move || {
            let mut cq = cq;
            cq.reaper(ring_fd)
        });

        Ok(Uring::new(
            self,
            params.flags,
            ring_fd,
            sq,
            in_flight,
            ticket_queue,
        ))
    }
}

fn setup(entries: c_uint, p: *mut Params) -> io::Result<c_int> {
    assert!(
        (1..=4096).contains(&entries),
        "entries must be between 1 and 4096 (inclusive)"
    );
    assert_eq!(entries.count_ones(), 1, "entries must be a power of 2");
    #[allow(unsafe_code)]
    let ret = unsafe { syscall(libc::SYS_io_uring_setup, i64::from(entries), p as c_long) };
    if ret < 0 {
        let mut err = io::Error::last_os_error();
        if let Some(12) = err.raw_os_error() {
            err = io::Error::new(
                io::ErrorKind::Other,
                "Not enough lockable memory. You probably \
                 need to raise the memlock rlimit, which \
                 often defaults to a pretty low number.",
            );
        }
        return Err(err);
    }
    Ok(i32::try_from(ret).unwrap())
}

fn enter(
    fd: c_int,
    to_submit: c_uint,
    min_complete: c_uint,
    flags: c_uint,
    sig: *mut libc::sigset_t,
) -> io::Result<c_int> {
    loop {
        // this is strapped into an interruption
        // diaper loop because it's the one that
        // might actually block a lot
        #[allow(unsafe_code)]
        let ret = unsafe {
            syscall(
                libc::SYS_io_uring_enter,
                i64::from(fd),
                i64::from(to_submit),
                i64::from(min_complete),
                i64::from(flags),
                sig as c_long,
                core::mem::size_of::<libc::sigset_t>() as c_long,
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        } else {
            return Ok(i32::try_from(ret).unwrap());
        }
    }
}

fn register(
    fd: c_int,
    opcode: c_uint,
    arg: *const libc::c_void,
    nr_args: c_uint,
) -> io::Result<c_int> {
    #[allow(unsafe_code)]
    let ret = unsafe {
        syscall(
            libc::SYS_io_uring_register,
            i64::from(fd),
            i64::from(opcode),
            arg as c_long,
            i64::from(nr_args),
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(i32::try_from(ret).unwrap())
}

/// A trait for describing transformations from the
/// `Cqe` type into an expected meaningful
/// high-level result.
pub trait FromCqe {
    /// Describes a conversion from a successful
    /// `Cqe` to a desired output type.
    fn from_cqe(cqe: Cqe) -> Self;
}

impl FromCqe for usize {
    fn from_cqe(cqe: Cqe) -> usize {
        usize::try_from(cqe.res).unwrap()
    }
}

impl FromCqe for () {
    fn from_cqe(_: Cqe) {}
}

impl FromCqe for File {
    fn from_cqe(cqe: Cqe) -> File {
        unsafe { File::from_raw_fd(cqe.res) }
    }
}

impl FromCqe for TcpStream {
    fn from_cqe(cqe: Cqe) -> TcpStream {
        unsafe { TcpStream::from_raw_fd(cqe.res) }
    }
}

impl FromCqe for TcpListener {
    fn from_cqe(cqe: Cqe) -> TcpListener {
        unsafe { TcpListener::from_raw_fd(cqe.res) }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone)]
pub struct Params {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub flags: u32,
    pub sq_thread_cpu: u32,
    pub sq_thread_idle: u32,
    pub resv: [u32; 5_usize],
    pub sq_off: io_sqring_offsets,
    pub cq_off: io_cqring_offsets,
}

pub type __kernel_rwf_t = ::std::os::raw::c_int;

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct Sqe {
    pub opcode: u8,
    pub flags: u8,
    pub ioprio: u16,
    pub fd: i32,
    pub off: u64,
    pub addr: u64,
    pub len: u32,
    pub __bindgen_anon_1: Sqe__bindgen_ty_1,
    pub user_data: u64,
    pub __bindgen_anon_2: Sqe__bindgen_ty_2,
}

impl Sqe {
    fn prep_rw(
        &mut self,
        opcode: u8,
        file_descriptor: i32,
        len: usize,
        off: u64,
        ordering: EventOrdering,
    ) {
        *self = Sqe {
            opcode,
            flags: 0,
            ioprio: 0,
            fd: file_descriptor,
            len: u32::try_from(len).unwrap(),
            off,
            ..*self
        };

        self.__bindgen_anon_1.rw_flags = 0;
        self.__bindgen_anon_2.__pad2 = [0; 3];

        self.apply_order(ordering);
    }

    fn apply_order(&mut self, ordering: EventOrdering) {
        match ordering {
            EventOrdering::None => {}
            EventOrdering::Link => self.flags |= IOSQE_IO_LINK,
            EventOrdering::Drain => self.flags |= IOSQE_IO_DRAIN,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union Sqe__bindgen_ty_1 {
    pub rw_flags: __kernel_rwf_t,
    pub fsync_flags: u32,
    pub poll_events: u16,
    pub sync_range_flags: u32,
    pub msg_flags: u32,
    _bindgen_union_align: u32,
}

impl Default for Sqe__bindgen_ty_1 {
    fn default() -> Sqe__bindgen_ty_1 {
        #[allow(unsafe_code)]
        unsafe {
            std::mem::zeroed()
        }
    }
}

impl fmt::Debug for Sqe__bindgen_ty_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sqe__bindgen_ty_1")
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union Sqe__bindgen_ty_2 {
    pub buf_index: u16,
    pub __pad2: [u64; 3_usize],
    _bindgen_union_align: [u64; 3_usize],
}

impl fmt::Debug for Sqe__bindgen_ty_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sqe__bindgen_ty_2")
    }
}

impl Default for Sqe__bindgen_ty_2 {
    fn default() -> Sqe__bindgen_ty_2 {
        #[allow(unsafe_code)]
        unsafe {
            std::mem::zeroed()
        }
    }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone)]
pub struct io_sqring_offsets {
    pub head: u32,
    pub tail: u32,
    pub ring_mask: u32,
    pub ring_entries: u32,
    pub flags: u32,
    pub dropped: u32,
    pub array: u32,
    pub resv1: u32,
    pub resv2: u64,
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone)]
pub struct io_cqring_offsets {
    pub head: u32,
    pub tail: u32,
    pub ring_mask: u32,
    pub ring_entries: u32,
    pub overflow: u32,
    pub cqes: u32,
    pub resv: [u64; 2_usize],
}

/// Specify whether `io_uring` should
/// run operations in a specific order.
/// By default, it will run independent
/// operations in any order it can to
/// speed things up. This can be constrained
/// by either submitting chains of `Link`
/// events, which are executed one after the other,
/// or by specifying the `Drain` ordering
/// which causes all previously submitted operations
/// to complete first.
#[derive(Clone, Debug, Copy)]
pub enum EventOrdering {
    /// No ordering requirements
    None,
    /// `Ordering::Link` causes the next
    /// submitted operation to wait until
    /// this one finishes. Useful for
    /// things like file copy, fsync-after-write,
    /// or proxies.
    Link,
    /// `Ordering::Drain` causes all previously
    /// submitted operations to complete before
    /// this one begins.
    Drain,
}

fn uring_mmap(size: usize, ring_fd: i32, offset: i64) -> io::Result<*mut libc::c_void> {
    #[allow(unsafe_code)]
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            ring_fd,
            offset,
        )
    };

    if ptr.is_null() || ptr == libc::MAP_FAILED {
        let mut err = io::Error::last_os_error();
        if let Some(12) = err.raw_os_error() {
            err = io::Error::new(
                io::ErrorKind::Other,
                "Not enough lockable memory. You probably \
                 need to raise the memlock rlimit, which \
                 often defaults to a pretty low number.",
            );
        }
        return Err(err);
    }

    Ok(ptr)
}

/// The top-level `io_uring` structure.
#[derive(Debug)]
pub struct Uring {
    sq: Mutex<Sq>,
    ticket_queue: Arc<TicketQueue>,
    in_flight: Arc<InFlight>,
    flags: u32,
    ring_fd: i32,
    config: Config,
    loaded: AtomicU64,
    submitted: AtomicU64,
}

#[allow(unsafe_code)]
unsafe impl Send for Uring {}

#[allow(unsafe_code)]
unsafe impl Sync for Uring {}

impl Drop for Uring {
    fn drop(&mut self) {
        let poison_pill_res = self.with_sqe::<_, ()>(None, false, |sqe| {
            sqe.prep_rw(IORING_OP_NOP, 0, 1, 0, EventOrdering::Drain);
            // set the poison pill
            sqe.user_data ^= u64::max_value();
        });

        // this waits for the NOP event to complete.
        drop(poison_pill_res);
    }
}

impl Uring {
    fn new(
        config: Config,
        flags: u32,
        ring_fd: i32,
        sq: Sq,
        in_flight: Arc<InFlight>,
        ticket_queue: Arc<TicketQueue>,
    ) -> Uring {
        Uring {
            flags,
            ring_fd,
            sq: Mutex::new(sq),
            config,
            in_flight,
            ticket_queue,
            loaded: 0.into(),
            submitted: 0.into(),
        }
    }

    fn ensure_submitted(&self, sqe_id: u64) -> io::Result<()> {
        let current = self.submitted.load(Ordering::Acquire);
        if current >= sqe_id {
            return Ok(());
        }
        let mut sq = self.sq.lock().unwrap();
        let submitted = sq.submit_all(self.flags, self.ring_fd);
        let old = self.submitted.fetch_add(submitted, Ordering::Release);

        if self.flags & IORING_SETUP_SQPOLL == 0 {
            // we only check this if we're running in
            // non-SQPOLL mode where we have to manually
            // push our submissions to the kernel.
            assert!(
                old + submitted >= sqe_id,
                "failed to submit our expected SQE on ensure_submitted. \
                expected old {} + submitted {} to be >= sqe_id {}",
                old,
                submitted,
                sqe_id,
            );
        }

        Ok(())
    }

    /// Asynchronously accepts a `TcpStream` from
    /// a provided `TcpListener`.
    ///
    /// # Warning
    ///
    /// This only becomes usable on linux kernels
    /// 5.5 and up.
    pub fn accept<'a>(&'a self, tcp_listener: &'a TcpListener) -> Completion<'a, TcpStream> {
        self.with_sqe(None, false, |sqe| {
            sqe.prep_rw(
                IORING_OP_ACCEPT,
                tcp_listener.as_raw_fd(),
                0,
                0,
                EventOrdering::None,
            )
        })
    }

    /// Asynchronously connects a `TcpStream` from
    /// a provided `SocketAddr`.
    ///
    /// # Warning
    ///
    /// This only becomes usable on linux kernels
    /// 5.5 and up.
    pub fn connect<'a, F>(
        &'a self,
        socket: &'a F,
        address: &std::net::SocketAddr,
        ordering: EventOrdering,
    ) -> Completion<'a, ()>
    where
        F: AsRawFd,
    {
        let (addr, len) = addr2raw(address);
        self.with_sqe(None, false, |sqe| {
            sqe.prep_rw(
                IORING_OP_CONNECT,
                socket.as_raw_fd(),
                0,
                len as u64,
                ordering,
            );

            sqe.addr = addr as u64;
        })
    }

    /// Send a buffer to the target socket
    /// or file-like destination.
    ///
    /// Returns the length that was successfully
    /// written.
    ///
    /// Accepts an `EventOrdering` specification.
    ///
    /// # Warning
    ///
    /// This only becomes usable on linux kernels
    /// 5.6 and up.
    pub fn send<'a, F, B>(
        &'a self,
        stream: &'a F,
        iov: &'a B,
        ordering: EventOrdering,
    ) -> Completion<'a, usize>
    where
        F: AsRawFd,
        B: 'a + AsIoVec,
    {
        let iov = iov.into_new_iovec();

        self.with_sqe(None, true, |sqe| {
            sqe.prep_rw(IORING_OP_SEND, stream.as_raw_fd(), 0, 0, ordering);
            sqe.addr = iov.iov_base as u64;
            sqe.len = u32::try_from(iov.iov_len).unwrap();
        })
    }

    /// Receive data from the target socket
    /// or file-like destination, and place
    /// it in the given buffer.
    ///
    /// Returns the length that was successfully
    /// read.
    ///
    /// Accepts an `EventOrdering` specification.
    ///
    /// # Warning
    ///
    /// This only becomes usable on linux kernels
    /// 5.6 and up.
    pub fn recv<'a, F, B>(
        &'a self,
        stream: &'a F,
        iov: &'a B,
        ordering: EventOrdering,
    ) -> Completion<'a, usize>
    where
        F: AsRawFd,
        B: AsIoVec + AsIoVecMut,
    {
        let iov = iov.into_new_iovec();

        self.with_sqe(Some(iov), true, |sqe| {
            sqe.prep_rw(IORING_OP_RECV, stream.as_raw_fd(), 0, 0, ordering);
            sqe.len = u32::try_from(iov.iov_len).unwrap();
        })
    }

    /// Flushes all buffered writes, and associated
    /// metadata changes.
    ///
    /// You probably want to
    /// either use a `Link` ordering on a previous
    /// write (or chain of separate writes), or
    /// use the `Drain` ordering on this operation.
    ///
    /// You may pass in an `EventOrdering` to specify
    /// two different optional behaviors:
    ///
    /// * `EventOrdering::Link` causes the next
    ///   submitted operation to wait until
    ///   this one finishes. Useful for
    ///   things like file copy, fsync-after-write,
    ///   or proxies.
    /// * `EventOrdering::Drain` causes all previously
    ///   submitted operations to complete before
    ///   this one begins.
    ///
    /// # Warning
    ///
    /// fsync does not ensure that
    /// the file actually exists in its parent
    /// directory. So, for new files, you must
    /// also fsync the parent directory.
    /// This does nothing for files opened in
    /// `O_DIRECT` mode.
    pub fn fsync<'a>(&'a self, file: &'a File, ordering: EventOrdering) -> Completion<'a, ()> {
        self.with_sqe(None, false, |sqe| {
            sqe.prep_rw(IORING_OP_FSYNC, file.as_raw_fd(), 0, 0, ordering)
        })
    }

    /// Flushes all buffered writes, and the specific
    /// metadata required to access the data. This
    /// will skip syncing metadata like atime.
    ///
    /// You probably want to
    /// either use a `Link` ordering on a previous
    /// write (or chain of separate writes), or
    /// use the `Drain` ordering on this operation.
    ///
    /// You may pass in an `EventOrdering` to specify
    /// two different optional behaviors:
    ///
    /// * `EventOrdering::Link` causes the next
    ///   submitted operation to wait until
    ///   this one finishes. Useful for
    ///   things like file copy, fsync-after-write,
    ///   or proxies.
    /// * `EventOrdering::Drain` causes all previously
    ///   submitted operations to complete before
    ///   this one begins.
    ///
    /// # Warning
    ///
    /// fdatasync does not ensure that
    /// the file actually exists in its parent
    /// directory. So, for new files, you must
    /// also fsync the parent directory.
    /// This does nothing for files opened in
    /// `O_DIRECT` mode.
    pub fn fdatasync<'a>(&'a self, file: &'a File, ordering: EventOrdering) -> Completion<'a, ()> {
        self.with_sqe(None, false, |mut sqe| {
            sqe.prep_rw(IORING_OP_FSYNC, file.as_raw_fd(), 0, 0, ordering);
            sqe.flags |= IORING_FSYNC_DATASYNC;
        })
    }

    /// Synchronizes the data associated with a range
    /// in a file. Does not synchronize any metadata
    /// updates, which can cause data loss if you
    /// are not writing to a file whose metadata
    /// has previously been synchronized.
    ///
    /// You probably want to have a prior write
    /// linked to this, or set `EventOrdering::Drain`.
    ///
    /// Under the hood, this uses the "pessimistic"
    /// set of flags:
    /// `SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER`
    pub fn sync_file_range<'a>(
        &'a self,
        file: &'a File,
        offset: u64,
        len: usize,
        ordering: EventOrdering,
    ) -> Completion<'a, ()> {
        self.with_sqe(None, false, |mut sqe| {
            sqe.prep_rw(
                IORING_OP_SYNC_FILE_RANGE,
                file.as_raw_fd(),
                len,
                offset,
                ordering,
            );
            sqe.flags |= u8::try_from(
                // We don't use this because it causes
                // EBADF to be thrown. Looking at
                // linux's fs/sync.c, it seems as though
                // it performs an identical operation
                // as SYNC_FILE_RANGE_WAIT_AFTER.
                // libc::SYNC_FILE_RANGE_WAIT_BEFORE |
                libc::SYNC_FILE_RANGE_WRITE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
            .unwrap();
        })
    }

    /// Writes data at the provided buffer using
    /// vectored IO.
    ///
    /// Be sure to check the returned
    /// `Cqe`'s `res` field to see if a
    /// short write happened. This will contain
    /// the number of bytes written.
    ///
    /// You may pass in an `EventOrdering` to specify
    /// two different optional behaviors:
    ///
    /// * `EventOrdering::Link` causes the next
    ///   submitted operation to wait until
    ///   this one finishes. Useful for
    ///   things like file copy, fsync-after-write,
    ///   or proxies.
    /// * `EventOrdering::Drain` causes all previously
    ///   submitted operations to complete before
    ///   this one begins.
    ///
    /// Note that the file argument is generic
    /// for anything that supports AsRawFd:
    /// sockets, files, etc...
    pub fn write_at<'a, F, B>(
        &'a self,
        file: &'a F,
        iov: &'a B,
        at: u64,
        ordering: EventOrdering,
    ) -> Completion<'a, usize>
    where
        F: AsRawFd,
        B: 'a + AsIoVec,
    {
        self.with_sqe(Some(iov.into_new_iovec()), false, |sqe| {
            sqe.prep_rw(IORING_OP_WRITEV, file.as_raw_fd(), 1, at, ordering)
        })
    }

    /// Reads data into the provided buffer using
    /// vectored IO. Be sure to check the returned
    /// `Cqe`'s `res` field to see if a
    /// short read happened. This will contain
    /// the number of bytes read.
    ///
    /// You may pass in an `EventOrdering` to specify
    /// two different optional behaviors:
    ///
    /// * `EventOrdering::Link` causes the next
    ///   submitted operation to wait until
    ///   this one finishes. Useful for
    ///   things like file copy, fsync-after-write,
    ///   or proxies.
    /// * `EventOrdering::Drain` causes all previously
    ///   submitted operations to complete before
    ///   this one begins.
    ///
    /// Note that the file argument is generic
    /// for anything that supports AsRawFd:
    /// sockets, files, etc...
    pub fn read_at<'a, F, B>(
        &'a self,
        file: &'a F,
        iov: &'a B,
        at: u64,
        ordering: EventOrdering,
    ) -> Completion<'a, usize>
    where
        F: AsRawFd,
        B: AsIoVec + AsIoVecMut,
    {
        self.with_sqe(Some(iov.into_new_iovec()), false, |sqe| {
            sqe.prep_rw(IORING_OP_READV, file.as_raw_fd(), 1, at, ordering)
        })
    }

    /// Don't do anything. This is
    /// mostly for debugging and tuning.
    pub fn nop<'a>(&'a self, ordering: EventOrdering) -> Completion<'a, ()> {
        self.with_sqe(None, false, |sqe| {
            sqe.prep_rw(IORING_OP_NOP, 0, 1, 0, ordering)
        })
    }

    /// Block until all items in the submission queue
    /// are submitted to the kernel. This can
    /// be avoided by using the `SQPOLL` mode
    /// (a privileged operation) on the `Config`
    /// struct.
    ///
    /// Note that this is performed automatically
    /// and in a more fine-grained way when a
    /// `Completion` is consumed via `Completion::wait`
    /// or awaited in a Future context.
    ///
    /// You don't need to call this if you are
    /// calling `.wait()` or `.await` on the
    /// `Completion` quickly, but if you are
    /// doing some other stuff that could take
    /// a while first, calling this will ensure
    /// that the operation is being executed
    /// by the kernel in the mean time.
    pub fn submit_all(&self) {
        let mut sq = self.sq.lock().unwrap();
        sq.submit_all(self.flags, self.ring_fd);
    }

    fn with_sqe<'a, F, C>(
        &'a self,
        iovec: Option<libc::iovec>,
        msghdr: bool,
        f: F,
    ) -> Completion<'a, C>
    where
        F: FnOnce(&mut Sqe),
        C: FromCqe,
    {
        let ticket = self.ticket_queue.pop();
        let (mut completion, filler) = pair(self);

        let data_ptr = self.in_flight.insert(ticket, iovec, msghdr, filler);

        let mut sq = self.sq.lock().unwrap();

        completion.sqe_id = self.loaded.fetch_add(1, Ordering::Release) + 1;

        let sqe = {
            loop {
                if let Some(sqe) = sq.try_get_sqe(self.flags) {
                    break sqe;
                } else {
                    let submitted = sq.submit_all(self.flags, self.ring_fd);
                    self.submitted.fetch_add(submitted, Ordering::Release);
                };
            }
        };

        sqe.user_data = ticket as u64;
        sqe.addr = data_ptr;
        f(sqe);

        completion
    }
}

fn addr2raw(addr: &std::net::SocketAddr) -> (*const libc::sockaddr, libc::socklen_t) {
    match *addr {
        std::net::SocketAddr::V4(ref a) => {
            let b: *const std::net::SocketAddrV4 = a;
            (b as *const _, std::mem::size_of_val(a) as libc::socklen_t)
        }
        std::net::SocketAddr::V6(ref a) => {
            let b: *const std::net::SocketAddrV6 = a;
            (b as *const _, std::mem::size_of_val(a) as libc::socklen_t)
        }
    }
}

/// Exerts backpressure on submission threads
/// to ensure that there are never more submissions
/// in-flight than available slots in the completion
/// queue. Normally io_uring would accept the excess,
/// and just drop the overflowing completions.
#[derive(Debug)]
struct TicketQueue {
    tickets: Mutex<Vec<usize>>,
    cv: Condvar,
}

impl TicketQueue {
    fn new(size: usize) -> TicketQueue {
        let tickets = Mutex::new((0..size).collect());
        TicketQueue {
            tickets,
            cv: Condvar::new(),
        }
    }

    fn push_multi(&self, mut new_tickets: Vec<usize>) {
        let mut tickets = self.tickets.lock().unwrap();
        tickets.append(&mut new_tickets);
        self.cv.notify_one();
    }

    fn pop(&self) -> usize {
        let mut tickets = self.tickets.lock().unwrap();
        while tickets.is_empty() {
            tickets = self.cv.wait(tickets).unwrap();
        }
        tickets.pop().unwrap()
    }
}

/// Sprays uring submissions.
#[derive(Debug)]
struct Sq {
    khead: *mut AtomicU32,
    ktail: *mut AtomicU32,
    kring_mask: *mut u32,
    kflags: *mut AtomicU32,
    kdropped: *mut AtomicU32,
    array: &'static mut [AtomicU32],
    sqes: &'static mut [Sqe],
    sqe_head: u32,
    sqe_tail: u32,
    ring_ptr: *const libc::c_void,
    ring_mmap_sz: usize,
    sqes_mmap_sz: usize,
}

impl Drop for Sq {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.sqes.as_ptr() as *mut libc::c_void, self.sqes_mmap_sz);
        }
        unsafe {
            libc::munmap(self.ring_ptr as *mut libc::c_void, self.ring_mmap_sz);
        }
    }
}

impl Sq {
    fn new(params: &Params, ring_fd: i32) -> io::Result<Sq> {
        let sq_ring_mmap_sz = params.sq_off.array as usize
            + (params.sq_entries as usize * std::mem::size_of::<u32>());

        // TODO IORING_FEAT_SINGLE_MMAP for sq

        let sq_ring_ptr = uring_mmap(sq_ring_mmap_sz, ring_fd, IORING_OFF_SQ_RING)?;

        let sqes_mmap_sz: usize = params.sq_entries as usize * std::mem::size_of::<Sqe>();

        let sqes_ptr: *mut Sqe = uring_mmap(sqes_mmap_sz, ring_fd, IORING_OFF_SQES)? as _;

        Ok(unsafe {
            Sq {
                sqe_head: 0,
                sqe_tail: 0,
                ring_ptr: sq_ring_ptr,
                ring_mmap_sz: sq_ring_mmap_sz,
                sqes_mmap_sz,
                khead: sq_ring_ptr.add(params.sq_off.head as usize) as *mut AtomicU32,
                ktail: sq_ring_ptr.add(params.sq_off.tail as usize) as *mut AtomicU32,
                kring_mask: sq_ring_ptr.add(params.sq_off.ring_mask as usize) as *mut u32,
                kflags: sq_ring_ptr.add(params.sq_off.flags as usize) as *mut AtomicU32,
                kdropped: sq_ring_ptr.add(params.sq_off.dropped as usize) as *mut AtomicU32,
                array: from_raw_parts_mut(
                    sq_ring_ptr.add(params.sq_off.array as usize) as _,
                    params.sq_entries as usize,
                ),
                sqes: from_raw_parts_mut(sqes_ptr, params.sq_entries as usize),
            }
        })
    }

    fn try_get_sqe(&mut self, ring_flags: u32) -> Option<&mut Sqe> {
        let next = self.sqe_tail + 1;

        let head = if (ring_flags & IORING_SETUP_SQPOLL) == 0 {
            // non-polling mode
            self.sqe_head
        } else {
            // polling mode
            unsafe { &*self.khead }.load(Ordering::Acquire)
        };

        if next - head <= self.sqes.len() as u32 {
            let idx = self.sqe_tail & unsafe { *self.kring_mask };
            let sqe = &mut self.sqes[idx as usize];
            self.sqe_tail = next;

            Some(sqe)
        } else {
            None
        }
    }

    // sets sq.array to point to current sq.sqe_head
    fn flush(&mut self) -> u32 {
        let mask: u32 = unsafe { *self.kring_mask };
        let to_submit = self.sqe_tail - self.sqe_head;

        let mut ktail = unsafe { &*self.ktail }.load(Ordering::Acquire);

        for _ in 0..to_submit {
            let index = ktail & mask;
            self.array[index as usize].store(self.sqe_head & mask, Ordering::Release);
            ktail += 1;
            self.sqe_head += 1;
        }

        let swapped = unsafe { &*self.ktail }.swap(ktail, Ordering::Release);

        assert_eq!(swapped, ktail - to_submit);

        to_submit
    }

    fn submit_all(&mut self, ring_flags: u32, ring_fd: i32) -> u64 {
        let submitted = if ring_flags & IORING_SETUP_SQPOLL == 0 {
            // non-SQPOLL mode, we need to use
            // `enter` to submit our SQEs.

            // TODO for polling, keep flags at 0

            let flags = IORING_ENTER_GETEVENTS;
            let flushed = self.flush();
            let mut to_submit = flushed;
            while to_submit > 0 {
                let ret = enter(ring_fd, to_submit, 0, flags, std::ptr::null_mut()).expect(
                    "Failed to submit items to kernel via \
                     io_uring. This should never fail.",
                );
                to_submit -= u32::try_from(ret).unwrap();
            }
            flushed
        } else if unsafe { &*self.kflags }.load(Ordering::Acquire) & IORING_SQ_NEED_WAKEUP != 0 {
            // the kernel has signalled to us that the
            // SQPOLL thread that checks the submission
            // queue has terminated due to inactivity,
            // and needs to be restarted.
            let to_submit = self.sqe_tail - self.sqe_head;
            enter(
                ring_fd,
                to_submit,
                0,
                IORING_ENTER_SQ_WAKEUP,
                std::ptr::null_mut(),
            )
            .expect(
                "Failed to wake up SQPOLL io_uring \
                 kernel thread. This should never fail.",
            );
            0
        } else {
            0
        };
        assert_eq!(unsafe { &*self.kdropped }.load(Ordering::Relaxed), 0);
        u64::from(submitted)
    }
}

struct InFlight {
    iovecs: UnsafeCell<Vec<libc::iovec>>,
    msghdrs: UnsafeCell<Vec<libc::msghdr>>,
    fillers: UnsafeCell<Vec<Option<Filler>>>,
}

impl std::fmt::Debug for InFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InFlight {{ .. }}")
    }
}

impl InFlight {
    fn new(size: usize) -> InFlight {
        let iovecs = UnsafeCell::new(vec![
            libc::iovec {
                iov_base: null_mut(),
                iov_len: 0
            };
            size
        ]);
        let msghdrs = UnsafeCell::new(vec![
            #[allow(unsafe_code)]
            unsafe {
                MaybeUninit::<libc::msghdr>::zeroed().assume_init()
            };
            size
        ]);

        let mut filler_vec = Vec::with_capacity(size);
        for _ in 0..size {
            filler_vec.push(None);
        }
        let fillers = UnsafeCell::new(filler_vec);
        InFlight {
            iovecs,
            msghdrs,
            fillers,
        }
    }

    fn insert(
        &self,
        ticket: usize,
        iovec: Option<libc::iovec>,
        msghdr: bool,
        filler: Filler,
    ) -> u64 {
        #[allow(unsafe_code)]
        unsafe {
            let iovec_ptr = self.iovecs.get();
            let msghdr_ptr = self.msghdrs.get();
            if let Some(iovec) = iovec {
                (*iovec_ptr)[ticket] = iovec;

                if msghdr {
                    (*msghdr_ptr)[ticket].msg_iov = (*iovec_ptr).as_mut_ptr().add(ticket);
                    (*msghdr_ptr)[ticket].msg_iovlen = 1;
                }
            }
            (*self.fillers.get())[ticket] = Some(filler);
            if iovec.is_some() {
                if msghdr {
                    (*msghdr_ptr).as_mut_ptr().add(ticket) as u64
                } else {
                    (*iovec_ptr).as_mut_ptr().add(ticket) as u64
                }
            } else {
                0
            }
        }
    }

    fn take_filler(&self, ticket: usize) -> Filler {
        #[allow(unsafe_code)]
        unsafe {
            (*self.fillers.get())[ticket].take().unwrap()
        }
    }
}

/// Consumes uring completions.
#[derive(Debug)]
pub struct Cq {
    khead: *mut AtomicU32,
    ktail: *mut AtomicU32,
    kring_mask: *mut u32,
    koverflow: *mut AtomicU32,
    cqes: *mut [Cqe],
    ticket_queue: Arc<TicketQueue>,
    in_flight: Arc<InFlight>,
    ring_ptr: *const libc::c_void,
    ring_mmap_sz: usize,
}

#[allow(unsafe_code)]
unsafe impl Send for Cq {}

impl Drop for Cq {
    fn drop(&mut self) {
        #[allow(unsafe_code)]
        unsafe {
            libc::munmap(self.ring_ptr as *mut libc::c_void, self.ring_mmap_sz);
        }
    }
}

impl Cq {
    fn new(
        params: &Params,
        ring_fd: i32,
        in_flight: Arc<InFlight>,
        ticket_queue: Arc<TicketQueue>,
    ) -> io::Result<Cq> {
        // TODO IORING_FEAT_SINGLE_MMAP for cq
        let cq_ring_mmap_sz =
            params.cq_off.cqes as usize + (params.cq_entries as usize * std::mem::size_of::<Cqe>());

        let cq_ring_ptr = uring_mmap(cq_ring_mmap_sz, ring_fd, IORING_OFF_CQ_RING)?;

        #[allow(unsafe_code)]
        Ok(unsafe {
            Cq {
                ring_ptr: cq_ring_ptr,
                ring_mmap_sz: cq_ring_mmap_sz,
                khead: cq_ring_ptr.add(params.cq_off.head as usize) as *mut AtomicU32,
                ktail: cq_ring_ptr.add(params.cq_off.tail as usize) as *mut AtomicU32,
                kring_mask: cq_ring_ptr.add(params.cq_off.ring_mask as usize) as *mut u32,
                koverflow: cq_ring_ptr.add(params.cq_off.overflow as usize) as *mut AtomicU32,
                cqes: from_raw_parts_mut(
                    cq_ring_ptr.add(params.cq_off.cqes as usize) as _,
                    params.cq_entries as usize,
                ),
                in_flight: in_flight.clone(),
                ticket_queue: ticket_queue.clone(),
            }
        })
    }

    fn reaper(&mut self, ring_fd: i32) {
        fn block_for_cqe(ring_fd: i32) -> io::Result<()> {
            let flags = IORING_ENTER_GETEVENTS;
            let submit = 0;
            let wait = 1;
            let sigset = std::ptr::null_mut();

            enter(ring_fd, submit, wait, flags, sigset)?;

            Ok(())
        }

        loop {
            if let Err(e) = block_for_cqe(ring_fd) {
                panic!("error in cqe reaper: {:?}", e);
            } else {
                assert_eq!(unsafe { (*self.koverflow).load(Ordering::Relaxed) }, 0);
                if self.reap_ready_cqes().is_none() {
                    // poison pill detected, time to shut down
                    return;
                }
            }
        }
    }

    fn reap_ready_cqes(&mut self) -> Option<usize> {
        let mut head = unsafe { &*self.khead }.load(Ordering::Acquire);
        let tail = unsafe { &*self.ktail }.load(Ordering::Acquire);
        let count = tail - head;

        // hack to get around mutable usage in loop
        // limitation as of rust 1.40
        let mut cq_opt = Some(self);

        let mut to_push = Vec::with_capacity(count as usize);

        while head != tail {
            let cq = cq_opt.take().unwrap();
            let index = head & unsafe { *cq.kring_mask };
            let cqe = &unsafe { &*cq.cqes }[index as usize];

            // we detect a poison pill by seeing if
            // the user_data is really big, which it
            // will tend not to be. if it's not a
            // poison pill, it will be up to as large
            // as the completion queue length.
            let (ticket, poisoned) = if cqe.user_data > u64::max_value() / 2 {
                (cqe.user_data ^ u64::max_value(), true)
            } else {
                (cqe.user_data, false)
            };

            let res = cqe.res;

            let completion_filler = cq.in_flight.take_filler(ticket as usize);
            to_push.push(ticket as usize);

            let result = if res < 0 {
                Err(io::Error::from_raw_os_error(res.neg()))
            } else {
                Ok(*cqe)
            };

            completion_filler.fill(result);

            unsafe { &*cq.khead }.fetch_add(1, Ordering::Release);
            cq_opt = Some(cq);
            head += 1;

            if poisoned {
                return None;
            }
        }

        cq_opt.take().unwrap().ticket_queue.push_multi(to_push);

        Some(count as usize)
    }
}

/// Encompasses various types of IO structures that
/// can be operated on as if they were a libc::iovec
pub trait AsIoVec {
    /// Returns the address of this object.
    fn into_new_iovec(&self) -> libc::iovec;
}

impl<A: ?Sized + AsRef<[u8]>> AsIoVec for A {
    fn into_new_iovec(&self) -> libc::iovec {
        let self_ref: &[u8] = self.as_ref();
        let self_ptr: *const [u8] = self_ref;
        libc::iovec {
            iov_base: self_ptr as *mut _,
            iov_len: self_ref.len(),
        }
    }
}

/// We use this internally as a way of communicating
/// that for certain operations, we cannot accept a
/// reference into read-only memory, like for reads.
///
/// If your compilation fails because of something
/// related to this, it's because you are trying
/// to use memory as a destination for a read
/// that could never actually be written to anyway,
/// which the compiler may place in read-only
/// memory in your process that cannot be written
/// to by anybody.
///
/// # Examples
///
/// This will cause the following code to break,
/// which would have caused an IO error anyway
/// due to trying to write to static read-only
/// memory:
///
/// ```compile_fail
/// let ring = &rsdb::io_uring::IO_URING;
/// let file = std::fs::File::open("failure").unwrap();
///
/// // the following buffer is placed in
/// // static, read-only memory and would
/// // never be valid to write to
/// let buffer: &[u8] = b"this is read-only";
///
/// // this fails to compile, because &[u8]
/// // does not implement `AsIoVecMut`:
/// ring.read_at(&file, &buffer, 0, rsdb::io_uring::EventOrdering::None).unwrap();
/// ```
///
/// which can be fixed by making it a mutable
/// slice:
///
/// ```no_run
/// let ring = &rsdb::io_uring::IO_URING;
/// let file = std::fs::File::open("failure").unwrap();
///
/// // the following buffer is placed in
/// // readable and writable memory, due to
/// // its mutability
/// let buffer: &mut [u8] = &mut [0; 42];
///
/// // this now works
/// ring.read_at(&file, &buffer, 0, rsdb::io_uring::EventOrdering::None).wait();
/// ```
pub trait AsIoVecMut {}

impl<A: ?Sized + AsMut<[u8]>> AsIoVecMut for A {}

#[derive(Debug)]
struct CompletionState {
    done: bool,
    item: Option<io::Result<Cqe>>,
}

impl Default for CompletionState {
    fn default() -> CompletionState {
        CompletionState {
            done: false,
            item: None,
        }
    }
}

/// A future value which may or may not be filled
///
/// # Safety
///
/// To prevent undefined behavior in the form of
/// use-after-free, never allow a Completion's
/// lifetime to end without dropping it. This can
/// happen with `std::mem::forget`, cycles in
/// `Arc` or `Rc`, and in other ways.
#[derive(Debug)]
pub struct Completion<'a, C: FromCqe> {
    lifetime: PhantomData<&'a C>,
    mu: Arc<Mutex<CompletionState>>,
    cv: Arc<Condvar>,
    uring: &'a Uring,
    sqe_id: u64,
}

/// The completer side of the fillable `Completion`
#[derive(Debug)]
pub struct Filler {
    mu: Arc<Mutex<CompletionState>>,
    cv: Arc<Condvar>,
}

/// Create a new `Filler` and the `Completion`
/// that will be filled by its completion.
pub fn pair<'a, C: FromCqe>(uring: &'a Uring) -> (Completion<'a, C>, Filler) {
    let mu = Arc::new(Mutex::new(CompletionState::default()));
    let cv = Arc::new(Condvar::new());
    let future = Completion {
        lifetime: PhantomData,
        mu: mu.clone(),
        cv: cv.clone(),
        sqe_id: 0,
        uring,
    };
    let filler = Filler { mu, cv };

    (future, filler)
}

impl<'a, C: FromCqe> Completion<'a, C> {
    /// Block on the `Completion`'s completion
    /// or dropping of the `Filler`
    pub fn wait(self) -> io::Result<C>
    where
        C: FromCqe,
    {
        self.wait_inner().unwrap()
    }

    fn wait_inner(&self) -> Option<io::Result<C>>
    where
        C: FromCqe,
    {
        debug_assert_ne!(
            self.sqe_id, 0,
            "sqe_id was never filled-in for this Completion",
        );

        self.uring
            .ensure_submitted(self.sqe_id)
            .expect("failed to submit SQE from wait_inner");

        let mut inner = self.mu.lock().unwrap();

        while !inner.done {
            inner = self.cv.wait(inner).unwrap();
        }

        inner
            .item
            .take()
            .map(|io_result| io_result.map(FromCqe::from_cqe))
    }
}

impl<'a, C: FromCqe> Drop for Completion<'a, C> {
    fn drop(&mut self) {
        self.wait_inner();
    }
}

impl Filler {
    /// Complete the `Completion`
    fn fill(self, inner: io::Result<Cqe>) {
        let mut state = self.mu.lock().unwrap();

        state.item = Some(inner);
        state.done = true;

        self.cv.notify_all();
    }
}
