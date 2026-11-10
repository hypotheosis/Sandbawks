//! PTY (pseudo-terminal) support for the `Process` service.
//!
//! The upstream E2B `process.proto` does not define a separate PTY
//! service — instead, the existing `Process` RPCs grow PTY behavior
//! whenever the client sets the `pty` field on a `StartRequest`:
//!
//! - `Start` + `pty: Some(PTY{size})` allocates a PTY pair, spawns the
//!   child with the slave as its controlling terminal, and emits the
//!   merged tty output via `ProcessEvent::Data { pty: bytes }` instead
//!   of `stdout` / `stderr`.
//! - `SendInput` (and `StreamInput`) accept `ProcessInput::Pty(bytes)`
//!   and write them to the master fd — the kernel's line discipline
//!   handles cooked-mode echo and delivery to the child.
//! - `Update` with a new `pty.size` issues `TIOCSWINSZ` on the master
//!   to resize the terminal; the child receives `SIGWINCH` from the
//!   kernel.
//! - `SendSignal` works unchanged: the registered pid is the child's
//!   pid, so the existing signalling path covers PTY processes too.
//!
//! This module provides the building blocks used by
//! [`crate::process`] to implement that surface:
//!
//! - [`open`] — `nix::pty::openpty` wrapper that returns the
//!   `(master, slave)` owned-fd pair.
//! - [`PtyHandle`] — non-blocking `tokio::io::unix::AsyncFd` wrapper
//!   around the master fd, exposing async `read`, `write_all`, and a
//!   `TIOCSWINSZ`-based `resize`.
//! - [`install_controlling_tty`] — `pre_exec` callback that runs in
//!   the freshly forked child to `setsid()` and acquire the slave
//!   (already inherited as fd 0) as the controlling terminal.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use nix::libc;
use nix::pty::{openpty, Winsize};
use tokio::io::unix::AsyncFd;

/// Async-aware wrapper around the PTY master fd.
///
/// The master fd is set to `O_NONBLOCK` so the `AsyncFd` readability /
/// writability polling actually surfaces `EAGAIN` instead of blocking
/// the tokio reactor. All I/O uses raw `libc::read` / `libc::write`
/// under the readiness guard rather than `std::fs::File` semantics, so
/// the wrapper plays well with the PTY's line discipline (which can
/// return short reads on each newline).
pub struct PtyHandle {
    inner: AsyncFd<OwnedFd>,
}

impl PtyHandle {
    /// Wrap an owned master fd. The fd is switched to non-blocking
    /// mode in-place.
    pub fn new(fd: OwnedFd) -> io::Result<Self> {
        let raw = fd.as_raw_fd();
        // `fcntl(F_GETFL)` → `fcntl(F_SETFL, flags | O_NONBLOCK)`
        // is the standard recipe for flipping a single status flag
        // without trampling the rest.
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            inner: AsyncFd::new(fd)?,
        })
    }

    /// Read up to `buf.len()` bytes from the master. Returns `Ok(0)`
    /// when the slave side has been fully closed (`EIO` on Linux is
    /// mapped to EOF here, since that's the cleanest signal that the
    /// child has exited and dropped its tty fds).
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.inner.readable().await?;
            let result = guard.try_io(|inner| {
                // SAFETY: `inner` borrows the OwnedFd; `buf` is a
                // valid mutable slice; the return is the byte count
                // or -1.
                let n =
                    unsafe { libc::read(inner.as_raw_fd(), buf.as_mut_ptr() as *mut _, buf.len()) };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            });
            match result {
                Ok(Ok(n)) => return Ok(n),
                Ok(Err(e)) if e.raw_os_error() == Some(libc::EIO) => return Ok(0),
                Ok(Err(e)) => return Err(e),
                // `try_io` returns `Err(WouldBlock)` when the readiness
                // guard turned out to be a false positive; retry.
                Err(_would_block) => continue,
            }
        }
    }

    /// Write all of `buf` to the master, looping on partial writes.
    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let mut guard = self.inner.writable().await?;
            let result = guard.try_io(|inner| {
                // SAFETY: see `read`; symmetric.
                let n =
                    unsafe { libc::write(inner.as_raw_fd(), buf.as_ptr() as *const _, buf.len()) };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n as usize)
                }
            });
            match result {
                Ok(Ok(n)) => buf = &buf[n..],
                Ok(Err(e)) => return Err(e),
                Err(_would_block) => continue,
            }
        }
        Ok(())
    }

    /// Issue `TIOCSWINSZ` on the master to resize the slave terminal.
    /// The child receives `SIGWINCH` from the kernel as a side effect.
    pub fn resize(&self, rows: u32, cols: u32) -> io::Result<()> {
        let ws = libc::winsize {
            ws_row: rows as u16,
            ws_col: cols as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: `ws` is a valid pointee for `TIOCSWINSZ`'s
        // expected payload; the fd is owned by `self.inner`.
        let rc = unsafe { libc::ioctl(self.as_raw_fd(), libc::TIOCSWINSZ, &ws as *const _) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Raw master fd, for callers that need to identify the fd in
    /// diagnostics. Do not perform direct I/O on this — use the
    /// async methods above so the readiness state stays consistent.
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.get_ref().as_raw_fd()
    }
}

/// Allocate a PTY pair with the given initial window size.
///
/// Returns `(master, slave)` as owned fds. The caller hands the slave
/// to the child (via `Command::stdin/stdout/stderr` cloning) and keeps
/// the master for I/O.
pub fn open(rows: u32, cols: u32) -> io::Result<(OwnedFd, OwnedFd)> {
    let ws = Winsize {
        ws_row: rows as u16,
        ws_col: cols as u16,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let result = openpty(Some(&ws), None).map_err(|e| io::Error::from_raw_os_error(e as i32))?;
    Ok((result.master, result.slave))
}

/// `pre_exec` callback for the freshly-forked child:
///
/// 1. `setsid()` — detach from the parent's controlling terminal and
///    become the leader of a new session, so the slave can be made the
///    new session's controlling terminal in step 2.
/// 2. `ioctl(0, TIOCSCTTY, 0)` — acquire fd 0 (the slave, which the
///    spawn machinery has already dup2'd into place from
///    `Command::stdin(Stdio::from(slave_clone))`) as the controlling
///    terminal of this session.
///
/// Both syscalls are async-signal-safe, which is the binding contract
/// for `pre_exec` on a multi-threaded parent.
pub fn install_controlling_tty() -> io::Result<()> {
    // SAFETY: `setsid` and `ioctl` are async-signal-safe; the post-fork
    // child runs with only the forking thread, so calling them here
    // does not deadlock the runtime's allocator.
    if unsafe { libc::setsid() } == -1 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::ioctl(0, libc::TIOCSCTTY, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
