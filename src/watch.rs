//! Wait for a change in a few state directories instead of re-reading files on
//! a short timer.
//!
//! Every sidebar copy used to stat `daemon.paused` and read its host's health
//! record every 100 ms, and check its view claim and the visibility record every
//! 300 ms. Forty streamers made that ~14k file operations per 15 s on the Studio
//! host while nothing changed (OmniDev bug 85). The facts those checks read only
//! change when a file is created, replaced or removed in one of a handful of
//! directories, and every writer of them replaces files (`rename`) or creates and
//! removes markers. So a streamer waits on the directories themselves: kqueue
//! (`EVFILT_VNODE`) on macOS, inotify on Linux. Neither goes through fseventsd.
//!
//! A watch is an optimisation of *when* to look, never of *what* is true: the
//! caller still reads the files after every wake, and keeps a slow safety tick
//! (`FALLBACK_TICK`) for anything a directory event cannot carry (a dead
//! daemon's pid, an idle deadline). Where no watch can be set up the caller
//! falls back to `UNWATCHED_TICK`, which is still ten times slower than the old
//! poll.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

/// Safety re-check while a watch is armed.
pub const FALLBACK_TICK: Duration = Duration::from_secs(10);
/// Re-check interval when no watch could be armed.
pub const UNWATCHED_TICK: Duration = Duration::from_secs(1);

pub struct DirWatch {
    fd: AsyncFd<OwnedFd>,
    // kqueue watches hold one descriptor per directory; inotify needs none
    _dirs: Vec<OwnedFd>,
}

impl DirWatch {
    /// Watch `dirs` for entries created, removed or renamed. Missing directories
    /// are created first so a marker written into them later is seen. `None`
    /// when the platform or the runtime cannot provide a watch.
    pub fn new(dirs: &[&Path]) -> Option<DirWatch> {
        for dir in dirs {
            let _ = std::fs::create_dir_all(dir);
        }
        let (queue, held) = arm(dirs)?;
        // readable only: a kqueue descriptor rejects a write-interest filter
        let fd = AsyncFd::with_interest(queue, Interest::READABLE).ok()?;
        Some(DirWatch { fd, _dirs: held })
    }

    /// Resolve once something changed in a watched directory since the last
    /// call. Pending events are drained, so a burst of writes is one wake.
    pub async fn changed(&self) {
        loop {
            let Ok(mut guard) = self.fd.readable().await else {
                // the reactor dropped the descriptor: behave like no watch
                return std::future::pending().await;
            };
            let drained = drain(guard.get_inner().as_raw_fd());
            guard.clear_ready();
            if drained {
                return;
            }
        }
    }
}

/// Wait for a change, or for `tick` to pass when there is no watch.
pub async fn changed_or_tick(watch: Option<&DirWatch>, tick: Duration) {
    match watch {
        Some(w) => {
            tokio::select! {
                _ = w.changed() => {}
                _ = tokio::time::sleep(tick) => {}
            }
        }
        None => tokio::time::sleep(UNWATCHED_TICK.min(tick)).await,
    }
}

fn owned(fd: RawFd) -> Option<OwnedFd> {
    (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) })
}

fn nonblocking(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd"))]
fn arm(dirs: &[&Path]) -> Option<(OwnedFd, Vec<OwnedFd>)> {
    use std::os::unix::ffi::OsStrExt;
    let queue = owned(unsafe { libc::kqueue() })?;
    nonblocking(queue.as_raw_fd());
    let mut held = Vec::new();
    for dir in dirs {
        let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
        #[cfg(target_os = "macos")]
        let flags = libc::O_EVTONLY;
        #[cfg(not(target_os = "macos"))]
        let flags = libc::O_RDONLY;
        let dir_fd = owned(unsafe { libc::open(path.as_ptr(), flags | libc::O_CLOEXEC) })?;
        let mut change: libc::kevent = unsafe { std::mem::zeroed() };
        change.ident = dir_fd.as_raw_fd() as libc::uintptr_t;
        change.filter = libc::EVFILT_VNODE;
        change.flags = libc::EV_ADD | libc::EV_CLEAR;
        change.fflags = libc::NOTE_WRITE | libc::NOTE_DELETE | libc::NOTE_RENAME | libc::NOTE_EXTEND;
        let done = unsafe {
            libc::kevent(queue.as_raw_fd(), &change, 1, std::ptr::null_mut(), 0, std::ptr::null())
        };
        if done < 0 {
            return None;
        }
        held.push(dir_fd);
    }
    Some((queue, held))
}

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd"))]
fn drain(queue: RawFd) -> bool {
    let zero = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let mut events: [libc::kevent; 16] = unsafe { std::mem::zeroed() };
    let mut any = false;
    loop {
        let n = unsafe {
            libc::kevent(queue, std::ptr::null(), 0, events.as_mut_ptr(), events.len() as i32, &zero)
        };
        if n <= 0 {
            return any;
        }
        any = true;
    }
}

#[cfg(target_os = "linux")]
fn arm(dirs: &[&Path]) -> Option<(OwnedFd, Vec<OwnedFd>)> {
    use std::os::unix::ffi::OsStrExt;
    let queue = owned(unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) })?;
    for dir in dirs {
        let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
        let mask = libc::IN_CREATE
            | libc::IN_DELETE
            | libc::IN_MOVED_TO
            | libc::IN_MOVED_FROM
            | libc::IN_CLOSE_WRITE
            | libc::IN_DELETE_SELF
            | libc::IN_MOVE_SELF;
        if unsafe { libc::inotify_add_watch(queue.as_raw_fd(), path.as_ptr(), mask) } < 0 {
            return None;
        }
    }
    nonblocking(queue.as_raw_fd());
    Some((queue, Vec::new()))
}

#[cfg(target_os = "linux")]
fn drain(queue: RawFd) -> bool {
    let mut buf = [0u8; 4096];
    let mut any = false;
    loop {
        let n = unsafe { libc::read(queue, buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            return any;
        }
        any = true;
    }
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "linux"
)))]
fn arm(_dirs: &[&Path]) -> Option<(OwnedFd, Vec<OwnedFd>)> {
    None
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "linux"
)))]
fn drain(_queue: RawFd) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("hm-watch-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn wakes(watch: &DirWatch) -> bool {
        tokio::time::timeout(Duration::from_millis(500), watch.changed()).await.is_ok()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_marker_created_or_removed_in_a_watched_directory_wakes_the_watcher() {
        let root = scratch("marker");
        let claims = root.join("view-claims");
        let watch = DirWatch::new(&[&root, &claims]).expect("watch available on this platform");

        // quiet directories do not wake anyone: this is the whole point
        assert!(!wakes(&watch).await, "no change, no wake");

        std::fs::write(root.join("daemon.paused"), b"paused\n").unwrap();
        assert!(wakes(&watch).await, "pause marker created");
        std::fs::remove_file(root.join("daemon.paused")).unwrap();
        assert!(wakes(&watch).await, "pause marker removed");

        // a directory the watch created itself still reports its entries
        std::fs::write(claims.join("h--p1.json"), b"{}").unwrap();
        assert!(wakes(&watch).await, "view claim created");

        // an atomic replace (write a temporary, rename over) is one change
        std::fs::write(root.join(".visible.tmp"), b"{}").unwrap();
        std::fs::rename(root.join(".visible.tmp"), root.join("visible-panes.json")).unwrap();
        assert!(wakes(&watch).await, "record replaced");
        assert!(!wakes(&watch).await, "the burst was drained in one wake");

        let _ = std::fs::remove_dir_all(root);
    }
}
