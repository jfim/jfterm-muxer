//! Self-spawn support: an `flock` lockfile + atomic socket bind (stale-socket
//! cleanup) resolves spawn races to one daemon; `daemonize()` double-forks.

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixListener;
use std::path::Path;

use nix::fcntl::{Flock, FlockArg};

fn errno_io(e: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(e as i32)
}

/// Outcome of trying to become the daemon.
// `main.rs` (Task 9) consumes `lock`/`listener`; until then only tests read
// them via `matches!`, so allow dead_code here rather than dropping the fields.
#[allow(dead_code)]
pub enum Acquire {
    /// We won: hold the lock (must stay alive) and the bound listener.
    Bound {
        lock: Flock<File>,
        listener: UnixListener,
    },
    /// Another daemon already holds the lock; the caller should exit.
    AlreadyRunning,
}

/// Try to take the exclusive lockfile without blocking.
fn try_flock(lock_path: &Path) -> io::Result<Option<Flock<File>>> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // A lockfile must never be truncated: its content is irrelevant and
        // truncation would needlessly race other openers. Default is no-trunc.
        .truncate(false)
        .mode(0o600)
        // FD_CLOEXEC: the daemon's lock fd must not leak into spawned shells.
        .custom_flags(libc::O_CLOEXEC)
        .open(lock_path)?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(flock) => Ok(Some(flock)),
        // EWOULDBLOCK == EAGAIN on Linux; matching one covers both.
        Err((_f, nix::errno::Errno::EAGAIN)) => Ok(None),
        Err((_f, e)) => Err(errno_io(e)),
    }
}

/// Resolve the spawn race and bind the socket. Holding the lock makes us
/// authoritative, so a leftover socket file is stale -> unlink + rebind.
pub fn acquire_daemon(sock_path: &Path, lock_path: &Path) -> io::Result<Acquire> {
    let lock = match try_flock(lock_path)? {
        Some(l) => l,
        None => return Ok(Acquire::AlreadyRunning),
    };
    let listener = match UnixListener::bind(sock_path) {
        Ok(l) => l,
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            // Only treat the leftover as a stale socket of ours: it must be a
            // socket (S_ISSOCK) owned by the current uid before we unlink it.
            // Refuse to unconditionally remove an arbitrary file at this path.
            unlink_stale_socket(sock_path)?;
            UnixListener::bind(sock_path)?
        }
        Err(e) => return Err(e),
    };
    // FD_CLOEXEC: the listening socket fd must not leak into spawned shells.
    set_cloexec(&listener)?;
    Ok(Acquire::Bound { lock, listener })
}

/// Unlink a leftover socket file, but only if it really is a socket owned by
/// the current uid. Refuses (errors) on anything else so a planted regular
/// file, directory, or another user's node is never silently removed.
fn unlink_stale_socket(sock_path: &Path) -> io::Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    // lstat: do not follow a symlink planted at the socket path.
    let md = std::fs::symlink_metadata(sock_path)?;
    if !md.file_type().is_socket() {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            "refusing to unlink non-socket file at socket path",
        ));
    }
    // SAFETY: getuid is always safe.
    let uid = unsafe { libc::getuid() };
    if md.uid() != uid {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "refusing to unlink socket not owned by current user",
        ));
    }
    std::fs::remove_file(sock_path)
}

/// Set FD_CLOEXEC on a file descriptor so it is not inherited across exec.
fn set_cloexec<F: std::os::fd::AsFd>(fd: &F) -> io::Result<()> {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).map_err(errno_io)?;
    Ok(())
}

/// `setsid` + double-fork into a daemon; `chdir("/")`; std fds -> `/dev/null`.
///
/// MUST be called BEFORE constructing any tokio runtime or spawning threads:
/// `fork()` only carries the calling thread into the child, so forking after a
/// multi-thread runtime exists would leave the child with a broken runtime
/// (locks held by threads that no longer exist).
pub fn daemonize() -> io::Result<()> {
    use nix::sys::stat::{Mode, umask};
    use nix::unistd::{ForkResult, chdir, fork, setsid};

    // Force a restrictive umask so the inherited umask cannot loosen the modes
    // of the socket or any child-created files. This runs before the socket
    // bind path, narrowing the bind->chmod window to 0600-or-tighter.
    umask(Mode::from_bits_truncate(0o077));

    // SAFETY: between fork and the next exec/_exit we touch only
    // async-signal-safe calls; the parent simply exits.
    match unsafe { fork() }.map_err(errno_io)? {
        ForkResult::Parent { .. } => std::process::exit(0),
        ForkResult::Child => {}
    }
    setsid().map_err(errno_io)?;
    match unsafe { fork() }.map_err(errno_io)? {
        ForkResult::Parent { .. } => std::process::exit(0),
        ForkResult::Child => {}
    }
    chdir("/").map_err(errno_io)?;
    redirect_std_to_devnull()?;
    Ok(())
}

fn redirect_std_to_devnull() -> io::Result<()> {
    use std::os::fd::IntoRawFd;
    // Take ownership of the raw fd so its `File` drop can't close it out from
    // under us. If the process started with 0/1/2 already closed, `open` may
    // hand back one of those very fds — in that case we must NOT close the
    // source at the end, or we'd re-close the std fd we just set up.
    let src = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?
        .into_raw_fd();
    // SAFETY: dup2 onto the three std fds; `src` is a valid open /dev/null.
    // dup2(src, src) is a no-op, so skipping it leaves that std fd open.
    for target in 0..=2 {
        if src != target && unsafe { libc::dup2(src, target) } < 0 {
            let e = io::Error::last_os_error();
            if src > 2 {
                unsafe { libc::close(src) };
            }
            return Err(e);
        }
    }
    // Close the source only when it isn't one of the std fds we just wired up.
    if src > 2 {
        unsafe { libc::close(src) };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_reports_already_running() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        let lock = dir.path().join("muxer.lock");
        let first = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(first, Acquire::Bound { .. }));
        let second = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(second, Acquire::AlreadyRunning));
        drop(first);
    }

    #[test]
    fn stale_socket_file_is_unlinked_and_rebound() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        let lock = dir.path().join("muxer.lock");
        // A genuine leftover socket node (no live listener): binding hits
        // AddrInUse, and since it is a socket owned by us it is unlinked + rebound.
        let stale = UnixListener::bind(&sock).unwrap();
        drop(stale);
        std::fs::write(dir.path().join("touch"), b"").unwrap(); // keep dir alive
        let acq = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(acq, Acquire::Bound { .. }));
        assert!(std::os::unix::net::UnixStream::connect(&sock).is_ok());
        drop(acq);
    }

    #[test]
    fn refuses_to_unlink_non_socket_at_socket_path() {
        // A regular file occupying the socket path must NOT be silently removed.
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        std::fs::write(&sock, b"not a socket").unwrap();
        assert!(unlink_stale_socket(&sock).is_err());
        assert!(sock.exists(), "regular file must be left in place");
    }

    #[test]
    fn lock_releases_on_drop_allowing_reacquire() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        let lock = dir.path().join("muxer.lock");
        let first = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(first, Acquire::Bound { .. }));
        drop(first);
        let again = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(again, Acquire::Bound { .. }));
    }
}
