// ssh transport for the DAEMON's own traffic (remote CLI execs + API-socket
// forward) over one ControlMaster per host. Pane streams deliberately use
// their own direct connections instead (see pane.rs).

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at, Instant};

use crate::api::ApiClient;
use crate::config::{ApiTransport, HostConfig};
use crate::util::{err, Logger, Result};

/// Marker in the error text for "the container isn't running". A stopped
/// devcontainer is its resting state, unlike an unreachable ssh host, so the
/// daemon backs off gently instead of treating it as a fault.
pub const DORMANT: &str = "dormant";

/// first build with terminal session observe/control
const MIN_PREVIEW_BUILD: &str = "2026-06-30";

/// Common ssh options, shared by the daemon's master and every pane stream.
pub const SSH_COMMON_OPTS: [&str; 6] = [
    "-o",
    "BatchMode=yes",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=3",
];

#[derive(Debug)]
pub struct RemoteStatus {
    pub socket: String,
    pub supported: bool,
    pub reason: Option<String>,
}

pub(crate) struct SshOutput {
    pub(crate) code: i32,
    pub(crate) out: String,
    pub(crate) err: String,
}

const SSH_TIMEOUT_TERM_GRACE: Duration = Duration::from_secs(2);
const SSH_TIMEOUT_KILL_GRACE: Duration = Duration::from_millis(500);
const SSH_TIMEOUT_REAP_POLL: Duration = Duration::from_millis(10);

async fn ssh(args: &[String], timeout_ms: u64) -> SshOutput {
    ssh_with_program(OsStr::new("ssh"), args, timeout_ms).await
}

pub(crate) async fn ssh_with_program(
    program: &OsStr,
    args: &[String],
    timeout_ms: u64,
) -> SshOutput {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Every short-lived daemon SSH invocation owns its complete native
        // ProxyCommand tree. A timeout may therefore stop exactly this group
        // without finding processes by name or touching a successful master.
        .process_group(0)
        // Last-resort protection if this helper future itself is cancelled.
        // The explicit timeout path below owns group cleanup and reaping.
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return SshOutput { code: 1, out: String::new(), err: error.to_string() };
        }
    };
    let Some(pgid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        let _ = child.kill().await;
        return SshOutput {
            code: 1,
            out: String::new(),
            err: "ssh process has no usable pid".into(),
        };
    };
    let stdout = read_pipe(child.stdout.take());
    let stderr = read_pipe(child.stderr.take());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut stdout = stdout;
    let mut stderr = stderr;

    let status = match timeout_at(deadline, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            let cleanup = terminate_owned_process_group(&mut child, pgid).await;
            abort_pipe_tasks(&stdout, &stderr);
            let detail = match cleanup {
                Ok(()) => error.to_string(),
                Err(cleanup_error) => {
                    format!("{error}; process cleanup failed: {cleanup_error}")
                }
            };
            return SshOutput { code: 1, out: String::new(), err: detail };
        }
        Err(_) => {
            let cleanup = terminate_owned_process_group(&mut child, pgid).await;
            abort_pipe_tasks(&stdout, &stderr);
            return match cleanup {
                Ok(()) => SshOutput { code: 1, out: String::new(), err: "ssh timeout".into() },
                Err(error) => SshOutput {
                    code: 1,
                    out: String::new(),
                    err: format!("ssh timeout; process cleanup failed: {error}"),
                },
            };
        }
    };

    match timeout_at(deadline, finish_pipes(&mut stdout, &mut stderr)).await {
        Ok(Ok((stdout, stderr))) => SshOutput {
            code: status.code().unwrap_or(1),
            out: String::from_utf8_lossy(&stdout).into_owned(),
            err: String::from_utf8_lossy(&stderr).into_owned(),
        },
        Ok(Err(error)) => {
            let cleanup = terminate_owned_process_group(&mut child, pgid).await;
            abort_pipe_tasks(&stdout, &stderr);
            let detail = match cleanup {
                Ok(()) => error.to_string(),
                Err(cleanup_error) => {
                    format!("{error}; process cleanup failed: {cleanup_error}")
                }
            };
            SshOutput { code: 1, out: String::new(), err: detail }
        }
        Err(_) => {
            let cleanup = terminate_owned_process_group(&mut child, pgid).await;
            abort_pipe_tasks(&stdout, &stderr);
            match cleanup {
                Ok(()) => SshOutput { code: 1, out: String::new(), err: "ssh timeout".into() },
                Err(error) => SshOutput {
                    code: 1,
                    out: String::new(),
                    err: format!("ssh timeout; process cleanup failed: {error}"),
                },
            }
        }
    }
}

fn read_pipe<R>(pipe: Option<R>) -> JoinHandle<io::Result<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut bytes = Vec::new();
        if let Some(mut pipe) = pipe {
            pipe.read_to_end(&mut bytes).await?;
        }
        Ok(bytes)
    })
}

async fn finish_pipe(pipe: &mut JoinHandle<io::Result<Vec<u8>>>) -> io::Result<Vec<u8>> {
    pipe.await.map_err(io::Error::other)?
}

async fn finish_pipes(
    stdout: &mut JoinHandle<io::Result<Vec<u8>>>,
    stderr: &mut JoinHandle<io::Result<Vec<u8>>>,
) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let (stdout, stderr) = tokio::join!(finish_pipe(stdout), finish_pipe(stderr));
    match (stdout, stderr) {
        (Ok(stdout), Ok(stderr)) => Ok((stdout, stderr)),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

fn abort_pipe_tasks(
    stdout: &JoinHandle<io::Result<Vec<u8>>>,
    stderr: &JoinHandle<io::Result<Vec<u8>>>,
) {
    stdout.abort();
    stderr.abort();
}

fn process_group_exists(pgid: i32) -> io::Result<bool> {
    if unsafe { libc::kill(-pgid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(error),
    }
}

fn signal_process_group(pgid: i32, signal: i32) -> io::Result<()> {
    if unsafe { libc::kill(-pgid, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

async fn wait_for_process_group_exit(pgid: i32, grace: Duration) -> io::Result<bool> {
    let deadline = Instant::now() + grace;
    while process_group_exists(pgid)? && Instant::now() < deadline {
        sleep(SSH_TIMEOUT_REAP_POLL).await;
    }
    Ok(!process_group_exists(pgid)?)
}

async fn terminate_owned_process_group(child: &mut Child, pgid: i32) -> io::Result<()> {
    signal_process_group(pgid, libc::SIGTERM)?;
    let term_deadline = Instant::now() + SSH_TIMEOUT_TERM_GRACE;
    while process_group_exists(pgid)? && Instant::now() < term_deadline {
        // Reap the SSH leader as soon as it exits, but keep giving its
        // ProxyCommand descendants the same bounded opportunity to handle TERM.
        child.try_wait()?;
        sleep(SSH_TIMEOUT_REAP_POLL).await;
    }
    if process_group_exists(pgid)? {
        // The leader's former PGID still identifies only this invocation.
        signal_process_group(pgid, libc::SIGKILL)?;
    }
    timeout(SSH_TIMEOUT_KILL_GRACE, child.wait())
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "ssh leader was not reaped after process-group cleanup",
            )
        })??;
    if !wait_for_process_group_exit(pgid, SSH_TIMEOUT_KILL_GRACE).await? {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "owned ssh process group survived SIGKILL",
        ));
    }
    Ok(())
}

/// How long a retired ControlMaster may take to handle SIGTERM.
const MASTER_TERM_GRACE: Duration = Duration::from_secs(2);
/// How long it may then take to disappear after SIGKILL.
const MASTER_KILL_GRACE: Duration = Duration::from_millis(500);
/// Bound on the `-O check` that decides whether a master is still usable. Much
/// tighter than the 15s the connect path allows, because this runs on a
/// disconnect: a master that cannot answer promptly is exactly the one being
/// retired, and waiting on it would delay the reconnect it is blocking.
const MASTER_CHECK_TIMEOUT_MS: u64 = 5_000;

fn process_exists(pid: i32) -> bool {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Pick the backgrounded ControlMaster holding `ctl_path` out of a `ps` listing.
///
/// Identity-guarded on three things at once, because this pid is about to be
/// signalled. The program must be `ssh`; `-S` must be followed by exactly this
/// control path (a prefix test would match a neighbouring `<name>.ctl.backup`,
/// and `socket_stem` already gives two hosts a shared prefix by design); and
/// the argv must carry a bare `-M`, which only a master does. The daemon's own
/// exec relays, forwards and `-O` commands all pass the same `-S <ctl_path>`
/// *without* `-M`, and killing one of those would abort a live relay carrying
/// the API connection.
pub(crate) fn master_pid_from_ps(listing: &str, ctl_path: &str) -> Option<i32> {
    for line in listing.lines() {
        let mut fields = line.split_whitespace();
        let Some(pid) = fields.next().and_then(|p| p.parse::<i32>().ok()) else {
            continue;
        };
        let argv: Vec<&str> = fields.collect();
        let Some(program) = argv.first() else { continue };
        let base = program.rsplit('/').next().unwrap_or(program);
        if base != "ssh" {
            continue;
        }
        if !argv.contains(&"-M") {
            continue;
        }
        let holds_path = argv
            .windows(2)
            .any(|w| w[0] == "-S" && w[1] == ctl_path);
        if holds_path {
            return Some(pid);
        }
    }
    None
}

async fn ps_listing() -> String {
    // `pid=,command=` with empty headers: one line per process, pid first,
    // full argv after. `command` is the portable spelling — macOS's own
    // keyword, and an alias of `args` on procps.
    let out = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .stdin(Stdio::null())
        .output()
        .await;
    out.map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default()
}

/// End a ControlMaster that this daemon started and that can no longer serve.
///
/// `ssh -M -f -N` daemonizes: the surviving master is reparented to init, so it
/// is not one of this process's children and can only be found by its
/// ControlPath and ended by a signal. Its `ProxyCommand` — on this fleet
/// `coder ssh --disable-autostart --stdio <workspace>.project` — is reparented
/// the same way and holds the master's stdio pipes, so it reads EOF and exits
/// as soon as the master does. Ending the master is therefore what ends the
/// whole transport, and leaving an unusable one running is what leaves that
/// child behind: `ensure_master` only unlinked the stale socket and started a
/// replacement beside it.
///
/// Deliberately conditional. A dropped event stream does not imply a dead
/// transport, and killing a healthy master would spend a full ssh (and, here, a
/// Coder) handshake on every transient reconnect. So the master is retired only
/// when it fails its own `-O check` — precisely the case `ensure_master`
/// already treats as stale.
///
/// Returns the pid it retired, so the disconnect can name it.
pub async fn retire_unusable_master(ctl_path: &Path, target: &str) -> Option<i32> {
    let ctl = ctl_path.display().to_string();
    let pid = master_pid_from_ps(&ps_listing().await, &ctl)?;
    let check = vec![
        "-S".to_string(),
        ctl,
        "-o".to_string(),
        "BatchMode=yes".to_string(),
        "-O".to_string(),
        "check".to_string(),
        target.to_string(),
    ];
    if ssh(&check, MASTER_CHECK_TIMEOUT_MS).await.code == 0 {
        return None; // still serving: not ours to end
    }
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0
        && io::Error::last_os_error().raw_os_error() != Some(libc::EPERM)
    {
        return None; // already gone
    }
    let deadline = Instant::now() + MASTER_TERM_GRACE;
    while process_exists(pid) && Instant::now() < deadline {
        sleep(SSH_TIMEOUT_REAP_POLL).await;
    }
    if process_exists(pid) {
        unsafe { libc::kill(pid, libc::SIGKILL) };
        let deadline = Instant::now() + MASTER_KILL_GRACE;
        while process_exists(pid) && Instant::now() < deadline {
            sleep(SSH_TIMEOUT_REAP_POLL).await;
        }
    }
    Some(pid)
}

fn remove_stale_control_socket(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(err(format!(
                "cannot inspect ssh control socket {}: {e}",
                path.display()
            )))
        }
    };
    if !metadata.file_type().is_socket() {
        return Err(err(format!(
            "refusing to replace ssh control path {} because it is not a socket",
            path.display()
        )));
    }
    fs::remove_file(path).map_err(|e| {
        err(format!(
            "cannot remove stale ssh control socket {}: {e}",
            path.display()
        ))
    })
}

pub struct RemoteHost {
    pub cfg: HostConfig,
    ctl_path: PathBuf,
    pub fwd_sock: PathBuf,
    forwarded: bool,
    /// docker hosts only: resolved container + chosen stdio bridge
    container: Option<crate::docker::Container>,
    /// docker hosts only: owns the relay listener. Dropping it stops serving
    /// and unlinks the socket, so a reconnect never inherits a dead one.
    relay: Option<crate::docker::RelayHandle>,
    /// ssh hosts only: owns the exec-relay listener when the exec transport
    /// is in use. Same lifecycle reasoning as `relay` above, one level up
    /// the transport stack (ssh exec instead of `docker exec`).
    exec_relay: Option<crate::ssh_relay::RelayHandle>,
    /// ssh hosts only: where the exec relay listens. Deliberately NOT
    /// `fwd_sock`: sharing one path with the streamlocal forward makes the
    /// relay's "is a healthy relay already serving this?" check answerable by
    /// a live `-L` forward, which silently ignores `api_transport = "exec"`
    /// and leaves the daemon believing it is on a relay it never started.
    exec_sock: PathBuf,
    /// ssh hosts only: which transport to try first. Seeded from `cfg` at
    /// construction; `hint_transport` lets the daemon override it with what
    /// last worked, since a fresh `RemoteHost` is built on every reconnect
    /// and would otherwise re-probe streamlocal every time even after it is
    /// known to be dead for this host.
    transport_hint: ApiTransport,
    /// ssh hosts only: which transport this connection actually used, so the
    /// daemon can feed it back into the next `RemoteHost`'s `hint_transport`.
    pub last_api_transport: Option<ApiTransport>,
    log: Logger,
}

/// FNV-1a, truncated to 8 hex chars.
///
/// NOT `DefaultHasher`: this value lands in a path the daemon and every
/// streamer must derive identically, and it has to survive a toolchain
/// upgrade. std's hasher is explicitly unstable across Rust releases, so a
/// bump would silently move every long-named host's socket and orphan its live
/// ControlMaster. FNV-1a is a published algorithm, so the crate's output is
/// fixed by spec rather than by implementation detail. Not a security boundary
/// — it only has to separate a user's own host names, and `socket_stem_hash_is_stable`
/// pins the exact value so a future swap cannot move anyone's sockets unnoticed.
///
/// Hashes the raw bytes rather than going through `Hash for str`, which appends
/// a terminator byte and would give a different (still stable, but arbitrary)
/// value.
fn short_hash(s: &str) -> String {
    use std::hash::Hasher;

    let mut hasher = fnv::FnvHasher::default();
    hasher.write(s.as_bytes());
    format!("{:08x}", hasher.finish() as u32)
}

/// Filename stem shared by a host's three sockets, bounded so the longest one
/// still fits sockaddr_un.
///
/// Truncation alone is not enough: two hosts sharing a long prefix (exactly the
/// naming style that overflows the limit in the first place) would collide, and
/// a collision is silent and dangerous rather than loud. `ensure_master` probes
/// the ControlPath before creating a master, so host B would find host A's live
/// master and run every ssh command — including remote-invoke plugin actions —
/// on the wrong machine; the docker relay would unlink the other host's live
/// socket and take over the path. So anything truncated carries a hash of the
/// FULL name.
///
/// Names that already fit are returned verbatim, which is what keeps existing
/// installs on byte-identical paths across this upgrade (no orphaned masters,
/// no migration). Only names that were already too long to work at all move.
fn socket_stem(state_dir: &std::path::Path, host_name: &str) -> String {
    use std::os::unix::ffi::OsStrExt;

    // macOS reserves one byte of sockaddr_un's 104-byte path for NUL; Linux
    // allows 108, and we deliberately apply the tighter bound on both so a
    // hosts.toml is portable. Overhead is the worst case across the three
    // suffixes (`-api-exec.sock`, 14) plus OpenSSH's mux temp suffix on the
    // ControlPath (a dot and 11 chars); 21 keeps a little slack.
    const MAX_SOCKET_PATH_BYTES: usize = 103;
    const CONTROL_SOCKET_OVERHEAD: usize = 21;

    let directory_bytes = state_dir.as_os_str().as_bytes().len() + 1;
    let budget = MAX_SOCKET_PATH_BYTES.saturating_sub(directory_bytes + CONTROL_SOCKET_OVERHEAD);
    if host_name.len() <= budget {
        return host_name.into();
    }

    // Degrade in defined steps rather than collapsing to a shared stem: prefix
    // plus hash while both fit, then hash alone. Below that even the hash can't
    // fit, which means the state dir path itself is too long for any socket —
    // return the hash anyway so hosts stay distinct and let the bind fail
    // loudly, rather than handing every host one shared path.
    let hash = short_hash(host_name);
    let keep = budget.saturating_sub(hash.len() + 1);
    let mut end = keep.min(host_name.len());
    while !host_name.is_char_boundary(end) {
        end -= 1;
    }
    let prefix = host_name[..end].trim_end_matches('-');
    if prefix.is_empty() {
        return hash;
    }
    format!("{prefix}-{hash}")
}

pub(crate) fn control_path(state_dir: &std::path::Path, host_name: &str) -> PathBuf {
    state_dir.join(format!("{}.ctl", socket_stem(state_dir, host_name)))
}

impl RemoteHost {
    pub fn new(cfg: &HostConfig, state_dir: &std::path::Path) -> RemoteHost {
        let stem = socket_stem(state_dir, &cfg.name);
        RemoteHost {
            ctl_path: state_dir.join(format!("{stem}.ctl")),
            fwd_sock: state_dir.join(format!("{stem}-api.sock")),
            transport_hint: cfg.api_transport,
            cfg: cfg.clone(),
            forwarded: false,
            container: None,
            relay: None,
            exec_relay: None,
            exec_sock: state_dir.join(format!("{stem}-api-exec.sock")),
            last_api_transport: None,
            log: Logger::new(state_dir, false),
        }
    }

    /// Seed the transport hint from a daemon-remembered choice. A no-op
    /// unless the host is configured `api_transport = "auto"` (the default):
    /// an explicit `socket` or `exec` override always pins its own choice and
    /// ignores anything remembered from a previous connection.
    pub fn hint_transport(&mut self, hint: Option<ApiTransport>) {
        if self.cfg.api_transport == ApiTransport::Auto {
            if let Some(h) = hint {
                self.transport_hint = h;
            }
        }
    }

    /// Bring the transport up: an ssh ControlMaster, or a resolved container.
    ///
    /// ssh hosts take the identical path they always did; the docker branch is
    /// additive.
    pub async fn ensure_ready(&mut self) -> Result<()> {
        if !self.cfg.kind.is_docker() {
            return self.ensure_master().await;
        }
        let bin = self.cfg.docker_bin.clone();
        let ids = crate::docker::resolve(&bin, &self.cfg.kind).await?;
        let Some(id) = ids.first().cloned() else {
            // a stopped devcontainer is the resting state, not a fault; the
            // daemon matches this marker to back off gently
            return Err(err(format!("{DORMANT}: no running container for {}", self.cfg.target)));
        };
        if ids.len() > 1 {
            // reachable without an attacker: a compose devcontainer can put the
            // same local_folder label on several services
            self.log.log(&format!(
                "[{}] {} containers match; using {id} — narrow the config if that is wrong",
                self.cfg.name,
                ids.len()
            ));
        }
        // re-probe on every (re)connect: a rebuilt container may differ
        crate::docker::probe_socat(&bin, &id).await?;
        self.container = Some(crate::docker::Container { id, docker_bin: bin });
        Ok(())
    }

    fn base_args(&self) -> Vec<String> {
        vec![
            "-S".into(),
            self.ctl_path.display().to_string(),
            "-o".into(),
            "BatchMode=yes".into(),
        ]
    }

    pub async fn ensure_master(&mut self) -> Result<()> {
        let mut check = self.base_args();
        check.extend(["-O".into(), "check".into(), self.cfg.target.clone()]);
        if ssh(&check, 15000).await.code == 0 {
            return Ok(());
        }
        self.forwarded = false;
        // OpenSSH falls back to a standalone connection when ControlPath exists
        // but no master is listening. With -f -N that silently leaks one process
        // per retry, while every later -O command keeps targeting the dead socket.
        remove_stale_control_socket(&self.ctl_path)?;
        let mut start: Vec<String> = vec![
            "-M".into(),
            "-S".into(),
            self.ctl_path.display().to_string(),
        ];
        start.extend(SSH_COMMON_OPTS.iter().map(|s| s.to_string()));
        start.extend([
            "-o".into(),
            "ControlPersist=yes".into(),
            "-f".into(),
            "-N".into(),
            self.cfg.target.clone(),
        ]);
        let res = ssh(&start, 20000).await;
        if res.code != 0 {
            return Err(err(format!(
                "ssh master to {} failed: {}",
                self.cfg.target,
                nonempty(&res.err, res.code)
            )));
        }
        let verified = ssh(&check, 15000).await;
        if verified.code != 0 {
            return Err(err(format!(
                "ssh master to {} did not create a usable control socket: {}",
                self.cfg.target,
                nonempty(&verified.err, verified.code)
            )));
        }
        Ok(())
    }

    pub async fn exec(&self, command: &str, timeout_ms: u64) -> Result<String> {
        if let Some(c) = &self.container {
            return c.exec(command, timeout_ms).await;
        }
        let mut args = self.base_args();
        args.extend([self.cfg.target.clone(), command.to_string()]);
        let res = ssh(&args, timeout_ms).await;
        if res.code != 0 {
            return Err(err(format!(
                "ssh exec failed ({command}): {}",
                nonempty(&res.err, res.code)
            )));
        }
        Ok(res.out)
    }

    pub async fn status(&self) -> Result<RemoteStatus> {
        let bin = crate::config::remote_herdr_expr(
            self.cfg.remote_bin.as_deref(),
            self.cfg.session.as_deref(),
        );
        let out = self.exec(&format!("exec {} status --json", bin), 15000).await?;
        #[derive(Deserialize)]
        struct Client {
            version: Option<String>,
        }
        #[derive(Deserialize)]
        struct Server {
            running: Option<bool>,
            socket: Option<String>,
            version: Option<String>,
        }
        #[derive(Deserialize)]
        struct StatusJson {
            client: Option<Client>,
            server: Option<Server>,
        }
        let parsed: StatusJson = serde_json::from_str(&out)?;
        let version = parsed
            .server
            .as_ref()
            .and_then(|s| s.version.clone())
            .or(parsed.client.and_then(|c| c.version))
            .unwrap_or_else(|| "unknown".into());
        let running = parsed.server.as_ref().and_then(|s| s.running) == Some(true);
        let socket = parsed.server.and_then(|s| s.socket).unwrap_or_default();
        let mut status = RemoteStatus { socket, supported: false, reason: None };
        if !running {
            // Name the session, or this reads as "that machine's herdr is
            // down" while the default session is running perfectly and only
            // the configured one is stopped — which is the common way to get
            // here once `session` is in play (a typo, or `herdr session stop`).
            status.reason = Some(match &self.cfg.session {
                Some(name) => format!("remote herdr session {name:?} is not running"),
                None => "remote herdr server is not running".into(),
            });
            return Ok(status);
        }
        match version_supported(&version) {
            Some(true) => status.supported = true,
            Some(false) => {
                status.reason = Some(format!(
                    "remote herdr {version} lacks terminal session streams (need >= 0.7.2 or preview {MIN_PREVIEW_BUILD})"
                ))
            }
            None => status.reason = Some(format!("cannot parse remote version {version}")),
        }
        Ok(status)
    }

    pub async fn forward_api(&mut self, remote_socket: &str) -> Result<PathBuf> {
        if self.forwarded && self.fwd_sock.exists() {
            return Ok(self.fwd_sock.clone());
        }
        // NEVER cancel a healthy forward — other processes may be using it
        if self.fwd_sock.exists() && ApiClient::connect(&self.fwd_sock).await.is_ok() {
            self.forwarded = true;
            return Ok(self.fwd_sock.clone());
        }
        let spec = format!("{}:{}", self.fwd_sock.display(), remote_socket);
        // a dead process can leave the forward registered on the master with
        // its socket file unlinked — cancel before re-adding
        let mut cancel = self.base_args();
        cancel.extend(["-O".into(), "cancel".into(), "-L".into(), spec.clone(), self.cfg.target.clone()]);
        let _ = ssh(&cancel, 15000).await;
        let _ = std::fs::remove_file(&self.fwd_sock);
        let mut fwd = self.base_args();
        fwd.extend(["-O".into(), "forward".into(), "-L".into(), spec, self.cfg.target.clone()]);
        let res = ssh(&fwd, 15000).await;
        if res.code != 0 {
            return Err(err(format!("ssh socket forward failed: {}", nonempty(&res.err, res.code))));
        }
        self.forwarded = true;
        Ok(self.fwd_sock.clone())
    }

    /// Try the streamlocal `-L` forward, verified with a real ping — not just
    /// that `ssh -O forward` reported success.
    ///
    /// The forward registering successfully is not proof the transport works:
    /// some sshds (embedded Go sshds fronting container/VM workspaces are the
    /// case this was written against) accept a direct-streamlocal channel
    /// open and then never service it. Every byte written just sits there, so
    /// the first sign of trouble is the API layer's own connect/ping timing
    /// out or the channel closing with zero bytes read — which is exactly
    /// what `ApiClient::connect`'s ping round-trip surfaces.
    ///
    /// That ping is the client `connect_api` returns, not an extra probe on
    /// top of it: a working host must not pay a round trip for a fallback it
    /// never needs.
    async fn try_socket_transport(&mut self, remote_socket: &str) -> Result<ApiClient> {
        let sock = self.forward_api(remote_socket).await?;
        ApiClient::connect(&sock).await
    }

    /// Drop a forward that just proved itself dead, so it doesn't sit
    /// registered on the ControlMaster for the connection's life with its
    /// socket file unlinked. Unlike `forward_api`'s guard this cannot steal a
    /// healthy forward: it only runs after a real ping failed.
    async fn cancel_forward(&mut self, remote_socket: &str) {
        let spec = format!("{}:{}", self.fwd_sock.display(), remote_socket);
        let mut args = self.base_args();
        args.extend(["-O".into(), "cancel".into(), "-L".into(), spec, self.cfg.target.clone()]);
        let _ = ssh(&args, 15000).await;
        let _ = std::fs::remove_file(&self.fwd_sock);
        self.forwarded = false;
    }

    /// Bridge the remote socket over a plain ssh exec channel instead of a
    /// streamlocal forward. See `ssh_relay` for the transport itself; this
    /// only resolves the relay command once and (re)starts the listener,
    /// mirroring the docker branch below one function down.
    async fn exec_relay_transport(&mut self, remote_socket: &str) -> Result<PathBuf> {
        // NEVER steal a healthy relay — same reasoning as the docker guard:
        // the socket path is per-host but shared across processes (daemon,
        // `remote-*` actions, `once`), and state_dir is a single fixed path.
        // `exec_sock` is the relay's OWN path, so a live streamlocal forward
        // can't answer for it and quietly cancel the exec transport.
        if self.exec_relay.is_none() && ApiClient::connect(&self.exec_sock).await.is_ok() {
            return Ok(self.exec_sock.clone());
        }
        self.exec_relay = None;
        let relay_cmd = crate::ssh_relay::detect_relay_command(self, remote_socket).await?;
        self.log.log(&format!(
            "[{}] exec relay via {} → {remote_socket}",
            self.cfg.name,
            relay_cmd.tool()
        ));
        let handle = crate::ssh_relay::serve_relay(
            self.ctl_path.clone(),
            self.cfg.target.clone(),
            relay_cmd,
            self.exec_sock.clone(),
            self.log.clone(),
        )?;
        let path = handle.path.clone();
        self.exec_relay = Some(handle);
        Ok(path)
    }

    /// Choose and reach the ssh API transport: streamlocal socket forward, or
    /// an exec relay. `api_transport = "socket"` / `"exec"` pin one and never
    /// try the other; the default `"auto"` tries the socket transport first
    /// (unless a prior connection in this daemon's lifetime already learned
    /// it doesn't work here — see `hint_transport`) and falls back to the
    /// exec relay on failure, logging the switch exactly once per fallback.
    ///
    /// Returns a CONNECTED client rather than a path: the connect is the
    /// probe, so the socket transport costs a working host exactly what it
    /// cost before this fallback existed.
    async fn connect_ssh_api(&mut self, remote_socket: &str) -> Result<ApiClient> {
        let configured = self.cfg.api_transport;
        let start_with_socket =
            (if configured == ApiTransport::Auto { self.transport_hint } else { configured })
                != ApiTransport::Exec;

        if start_with_socket {
            match self.try_socket_transport(remote_socket).await {
                Ok(api) => {
                    self.last_api_transport = Some(ApiTransport::Socket);
                    return Ok(api);
                }
                // only auto may fall back; an explicit `socket` pin means the
                // caller wants the real failure, not a silent transport swap
                Err(e) if configured != ApiTransport::Auto => return Err(e),
                Err(e) => {
                    self.log.log(&format!(
                        "[{}] streamlocal forward unavailable ({e}) — using exec relay",
                        self.cfg.name
                    ));
                    self.transport_hint = ApiTransport::Exec;
                    // it answered nothing; don't leave it registered
                    self.cancel_forward(remote_socket).await;
                }
            }
        }

        let sock = self.exec_relay_transport(remote_socket).await?;
        let api = ApiClient::connect(&sock).await?;
        self.last_api_transport = Some(ApiTransport::Exec);
        Ok(api)
    }

    pub async fn connect_api(&mut self) -> Result<(ApiClient, RemoteStatus)> {
        self.ensure_ready().await?;
        let status = match self.status().await {
            Ok(s) => s,
            Err(_) => {
                // transient mux hiccup (e.g. concurrent -O forward churn) — retry once
                tokio::time::sleep(Duration::from_secs(1)).await;
                self.status().await?
            }
        };
        if !status.supported {
            return Err(err(status.reason.clone().unwrap_or_else(|| "remote unsupported".into())));
        }
        // ssh hosts hand back a connected client (its ping doubles as the
        // transport probe); the docker branch resolves a path and connects below
        let container = self.container.clone();
        let sock = match &container {
            None => return Ok((self.connect_ssh_api(&status.socket).await?, status)),
            Some(c) => {
                // NEVER steal a healthy relay — the socket path is per-HOST but
                // shared across processes (daemon, `remote-*` actions, `once`),
                // and state_dir is deliberately a single fixed path. Binding on
                // top of a live one orphans the owner's listener and then
                // unlinks the path from under it, bouncing the daemon's whole
                // host connection on every remote action. Same reasoning as the
                // ssh forward guard above.
                if self.relay.is_none() && ApiClient::connect(&self.fwd_sock).await.is_ok() {
                    self.fwd_sock.clone()
                } else {
                    self.relay = None;
                    let handle = crate::docker::serve_relay(
                        c.clone(),
                        status.socket.clone(),
                        self.fwd_sock.clone(),
                        self.log.clone(),
                    )?;
                    let path = handle.path.clone();
                    self.relay = Some(handle);
                    path
                }
            }
        };
        let api = ApiClient::connect(&sock).await?;
        Ok((api, status))
    }

}

fn nonempty(e: &str, code: i32) -> String {
    let t = e.trim();
    if t.is_empty() {
        format!("exit {code}")
    } else {
        t.to_string()
    }
}

/// `Some(true)` = supported, `Some(false)` = too old, `None` = unparseable.
fn version_supported(version: &str) -> Option<bool> {
    let core = version.split(['-', '+']).next()?;
    let mut it = core.split('.');
    let maj: u64 = it.next()?.parse().ok()?;
    let min: u64 = it.next()?.parse().ok()?;
    let pat: u64 = it.next()?.parse().ok()?;
    let newer_than_base = maj > 0 || min > 7 || (min == 7 && pat > 1);
    // preview builds look like 0.7.1-preview.2026-06-30-<hash>
    let preview_ok = version
        .split_once("-preview.")
        .map(|(_, rest)| rest.get(0..10).map(|d| d >= MIN_PREVIEW_BUILD).unwrap_or(false))
        .unwrap_or(false);
    Some(newer_than_base || preview_ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// The fake-SSH regressions run one at a time.
    ///
    /// They are process tests, not unit tests: each one forks a busy-looping
    /// descendant, blocks TERM, and asserts on exact pids and sub-second
    /// deadlines. Run beside the other 220 tests on `cargo test`'s thread pool
    /// they contend for the same cores and miss those deadlines — measured on
    /// 2026-09-07, where a serial run was green and parallel runs failed
    /// intermittently on `recorded_pid`. One at a time they are deterministic,
    /// and they cost about 12 seconds together either way, because their own
    /// timeouts dominate.
    static FAKE_SSH: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn fake_ssh_guard() -> std::sync::MutexGuard<'static, ()> {
        // A panicking regression must not make every later one fail to acquire.
        FAKE_SSH.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A path no other test in this process can produce.
    ///
    /// The clock alone was not enough: `cargo test` runs this module's tests on
    /// several threads at once, `SystemTime::now()` is not guaranteed to
    /// advance between two of them, and every test asks for a program called
    /// `fake-ssh`. Two colliding calls shared one file, and the first test to
    /// finish deleted it out from under the other's `spawn`. The counter makes
    /// the name unique by construction.
    fn test_path(name: &str) -> PathBuf {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "herdr-mirror-{name}-{}-{nonce}-{sequence}",
            std::process::id()
        ))
    }

    fn ssh_host(name: &str) -> HostConfig {
        HostConfig {
            name: name.into(),
            target: format!("{name}.example.com"),
            kind: crate::config::HostKind::Ssh,
            docker_bin: "docker".into(),
            prefix: name.into(),
            remote_bin: None,
            session: None,
            max_cols: None,
            max_rows: None,
            api_transport: ApiTransport::Auto,
            always_control: true,
        }
    }

    fn fake_ssh() -> PathBuf {
        let path = test_path("fake-ssh");
        fs::write(
            &path,
            r#"#!/bin/sh
case "$1" in
  timeout)
    printf '%s\n' "$$" >"$2"
    sh -c 'trap "" TERM; printf "%s\n" "$$" >"$1"; while :; do :; done' sh "$3" &
    wait
    ;;
  pipe-hold)
    printf '%s\n' "$$" >"$2"
    sh -c 'trap "" TERM; printf "%s\n" "$$" >"$1"; while :; do :; done' sh "$3" &
    exit 0
    ;;
  term-cleanup)
    printf '%s\n' "$$" >"$2"
    sh -c 'trap "/bin/sleep 0.2; printf cleaned >\"$2\"; exit 0" TERM; printf "%s\n" "$$" >"$1"; while :; do :; done' sh "$3" "$4" &
    exit 0
    ;;
  output)
    printf 'ordinary stdout'
    printf 'ordinary stderr' >&2
    exit 7
    ;;
  master)
    [ "$3" = -M ] && [ "$4" = -f ] && [ "$5" = -N ] || exit 9
    /bin/sleep 30 </dev/null >/dev/null 2>&1 &
    printf '%s\n' "$!" >"$2"
    exit 0
    ;;
  *) exit 8 ;;
esac
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    async fn recorded_pid(path: &Path) -> i32 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            if let Ok(value) = fs::read_to_string(path) {
                if let Ok(pid) = value.trim().parse() {
                    return pid;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "fake ssh did not record its pid in {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn pid_alive(pid: i32) -> bool {
        unsafe { libc::kill(pid, 0) == 0 }
    }

    fn kill_pid(pid: i32) {
        if pid_alive(pid) {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }

    async fn wait_for_pid_exit(pid: i32) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while pid_alive(pid) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        !pid_alive(pid)
    }

    /// The three identity guards that stand between this scan and a SIGTERM.
    ///
    /// Every one of them was a real way to kill the wrong process: the daemon's
    /// exec relay carries the same `-S <ctl_path>` without `-M`, `socket_stem`
    /// deliberately gives two long-named hosts a shared path prefix, and the
    /// same host runs plenty of processes that are not ssh at all.
    #[test]
    fn only_a_master_holding_exactly_this_control_path_is_signallable() {
        let ctl = "/state/greenroom-studio.ctl";
        let master = "111 ssh -M -S /state/greenroom-studio.ctl -o BatchMode=yes -f -N omnidev-greenroom-studio";
        assert_eq!(master_pid_from_ps(master, ctl), Some(111));
        let absolute = "222 /usr/bin/ssh -M -S /state/greenroom-studio.ctl -f -N host";
        assert_eq!(master_pid_from_ps(absolute, ctl), Some(222), "an absolute ssh is still ssh");

        // the exec relay: same control path, no -M. Killing it aborts the live
        // API connection this reconnect is trying to replace.
        let relay = "333 ssh -S /state/greenroom-studio.ctl -o BatchMode=yes omnidev-greenroom-studio python3 -c ...";
        assert_eq!(master_pid_from_ps(relay, ctl), None);

        // a neighbouring path that merely starts the same way
        let neighbour = "444 ssh -M -S /state/greenroom-studio.ctl.backup -o BatchMode=yes -f -N x";
        assert_eq!(master_pid_from_ps(neighbour, ctl), None);

        // not ssh, however convincing its argv
        let impostor = "555 herdr-mirror pane -M -S /state/greenroom-studio.ctl";
        assert_eq!(master_pid_from_ps(impostor, ctl), None);

        // and the real listing is many lines of other people's processes
        let listing = format!("{impostor}\n{relay}\n{neighbour}\n{master}\n666 sleep 30");
        assert_eq!(master_pid_from_ps(&listing, ctl), Some(111));
    }

    /// A reconnect must not leave the previous transport running beside its
    /// replacement.
    ///
    /// The fake is shaped like the real thing: a master that survives on its
    /// own, plus a `ProxyCommand` child in a different process group that is
    /// never signalled and exits only when the master's stdio pipe closes —
    /// which is exactly how `coder ssh --disable-autostart --stdio
    /// <workspace>.project` behaves on this fleet. So asserting the child is
    /// gone asserts the whole transport is gone, not just the pid we signalled.
    // Holding `FAKE_SSH` across awaits is the point of the serial guard; the
    // five fake-ssh regressions beside this one trip the same lint at the pin.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn retiring_an_unusable_master_leaves_no_transport_child_behind() {
        let _serial = fake_ssh_guard();
        let dir = test_path("retire-master");
        fs::create_dir_all(&dir).unwrap();
        let script = dir.join("master.sh");
        let leader_file = dir.join("leader");
        let child_file = dir.join("child");
        let fifo = dir.join("stdio");
        // A control path that exists nowhere, so the real `ssh -O check` this
        // function runs fails immediately and locally: no socket, no DNS, no
        // network. "Unusable" is precisely the state under test.
        let ctl = dir.join("host.ctl");
        fs::write(
            &script,
            r#"#!/bin/sh
mkfifo "$HM_TEST_FIFO"
sh -c 'printf "%s\n" "$$" >"$1"; exec cat >/dev/null' sh "$HM_TEST_CHILD" <"$HM_TEST_FIFO" &
exec 9>"$HM_TEST_FIFO"
printf '%s\n' "$$" >"$HM_TEST_LEADER"
while :; do /bin/sleep 1; done
"#,
        )
        .unwrap();

        // argv[0] must be `ssh`, because that is what the scan matches on and
        // what a real `-f` master shows; a `#!` script would show its
        // interpreter instead.
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("/bin/sh");
        command
            .arg0("ssh")
            .arg(&script)
            .args(["-M", "-S", &ctl.display().to_string(), "-f", "-N", "fake.example.com"])
            .env("HM_TEST_LEADER", &leader_file)
            .env("HM_TEST_CHILD", &child_file)
            .env("HM_TEST_FIFO", &fifo)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut spawned = command.spawn().unwrap();

        let leader = recorded_pid(&leader_file).await;
        let child = recorded_pid(&child_file).await;
        assert!(pid_alive(leader) && pid_alive(child));

        let retired = retire_unusable_master(&ctl, "fake.example.com").await;
        assert_eq!(retired, Some(leader), "the disconnect must be able to name it");

        // Reap it here: this test is the master's direct parent, which the
        // daemon never is (`-f` reparents the real one to init), and an
        // unreaped zombie still answers `kill(pid, 0)`.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut leader_gone = false;
        while tokio::time::Instant::now() < deadline {
            if matches!(spawned.try_wait(), Ok(Some(_))) {
                leader_gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let child_gone = wait_for_pid_exit(child).await;
        if !leader_gone {
            kill_pid(leader);
        }
        if !child_gone {
            kill_pid(child);
        }
        // Reaped on every path: a failing regression must leave nothing behind.
        let _ = spawned.wait();
        let _ = fs::remove_dir_all(&dir);
        assert!(leader_gone, "the master this daemon started survived its retirement");
        assert!(child_gone, "the transport child outlived its parent — the orphan this fixes");
    }

    /// Nothing to retire is the ordinary case: a host whose master never came
    /// up, or one whose reconnect follows a clean exit. It must cost no signal
    /// and no claim.
    #[tokio::test]
    async fn retiring_reports_nothing_when_no_master_holds_the_path() {
        let ctl = test_path("absent-master").join("host.ctl");
        assert_eq!(retire_unusable_master(&ctl, "fake.example.com").await, None);
    }

    #[tokio::test]
    async fn ssh_timeout_reaps_its_process_tree_across_retries() {
        let _serial = fake_ssh_guard();
        let program = fake_ssh();
        let mut survivors = Vec::new();

        for attempt in 0..3 {
            let leader_file = test_path(&format!("timeout-leader-{attempt}"));
            let child_file = test_path(&format!("timeout-child-{attempt}"));
            let args = vec![
                "timeout".into(),
                leader_file.display().to_string(),
                child_file.display().to_string(),
            ];

            let output = ssh_with_program(program.as_os_str(), &args, 2000).await;
            assert_eq!(output.err, "ssh timeout");
            let leader = recorded_pid(&leader_file).await;
            let child = recorded_pid(&child_file).await;
            if !wait_for_pid_exit(leader).await || !wait_for_pid_exit(child).await {
                survivors.push((leader, child));
                // A failing regression must not leave its disposable processes
                // behind. Only a survivor is signalled: the kernel recycles the
                // pid of a process that already exited, and the rest of this
                // suite runs in parallel, so killing a reaped pid can land on
                // another test's child instead.
                kill_pid(child);
                kill_pid(leader);
            }
            let _ = fs::remove_file(leader_file);
            let _ = fs::remove_file(child_file);
        }

        let _ = fs::remove_file(program);
        assert!(survivors.is_empty(), "timed-out ssh process trees survived: {survivors:?}");
    }

    #[tokio::test]
    async fn ssh_preserves_ordinary_output_and_status() {
        let _serial = fake_ssh_guard();
        let program = fake_ssh();
        let output = ssh_with_program(program.as_os_str(), &["output".into()], 1000).await;
        let _ = fs::remove_file(program);

        assert_eq!(output.code, 7);
        assert_eq!(output.out, "ordinary stdout");
        assert_eq!(output.err, "ordinary stderr");
    }

    #[tokio::test]
    async fn ssh_timeout_includes_pipes_held_by_a_descendant() {
        let _serial = fake_ssh_guard();
        let program = fake_ssh();
        let leader_file = test_path("pipe-leader");
        let child_file = test_path("pipe-child");
        let args = vec![
            "pipe-hold".into(),
            leader_file.display().to_string(),
            child_file.display().to_string(),
        ];

        let bounded = tokio::time::timeout(
            Duration::from_secs(4),
            ssh_with_program(program.as_os_str(), &args, 500),
        )
        .await;
        let leader = recorded_pid(&leader_file).await;
        let child = recorded_pid(&child_file).await;
        if bounded.is_err() {
            kill_pid(child);
            kill_pid(leader);
        }
        let output = bounded.expect("ssh helper hung after its direct child exited");
        assert_eq!(output.err, "ssh timeout");
        // Both assertions above already proved these exited, and their pids are
        // free for the kernel to reuse, so there is nothing left to signal.
        assert!(wait_for_pid_exit(leader).await, "direct ssh child survived timeout");
        assert!(wait_for_pid_exit(child).await, "pipe-holding descendant survived timeout");

        let _ = fs::remove_file(leader_file);
        let _ = fs::remove_file(child_file);
        let _ = fs::remove_file(program);
    }

    #[tokio::test]
    async fn ssh_timeout_gives_the_whole_group_term_grace() {
        let _serial = fake_ssh_guard();
        let program = fake_ssh();
        let leader_file = test_path("term-leader");
        let child_file = test_path("term-child");
        let cleanup_file = test_path("term-cleanup");
        let args = vec![
            "term-cleanup".into(),
            leader_file.display().to_string(),
            child_file.display().to_string(),
            cleanup_file.display().to_string(),
        ];

        let output = ssh_with_program(program.as_os_str(), &args, 2000).await;
        let leader = recorded_pid(&leader_file).await;
        let child = recorded_pid(&child_file).await;
        assert_eq!(output.err, "ssh timeout");
        assert_eq!(fs::read_to_string(&cleanup_file).unwrap(), "cleaned");
        assert!(wait_for_pid_exit(leader).await, "direct ssh child survived timeout");
        assert!(wait_for_pid_exit(child).await, "graceful descendant survived timeout");

        let _ = fs::remove_file(leader_file);
        let _ = fs::remove_file(child_file);
        let _ = fs::remove_file(cleanup_file);
        let _ = fs::remove_file(program);
    }

    #[tokio::test]
    async fn successful_master_start_is_not_treated_as_a_timeout() {
        let _serial = fake_ssh_guard();
        let program = fake_ssh();
        let master_file = test_path("successful-master");
        let args = vec![
            "master".into(),
            master_file.display().to_string(),
            "-M".into(),
            "-f".into(),
            "-N".into(),
        ];

        let output = ssh_with_program(program.as_os_str(), &args, 1000).await;
        let master = recorded_pid(&master_file).await;
        assert_eq!(output.code, 0);
        assert!(pid_alive(master), "successful background master was terminated");

        kill_pid(master);
        let _ = fs::remove_file(master_file);
        let _ = fs::remove_file(program);
    }

    #[test]
    fn long_host_names_use_truncated_socket_paths() {
        let state_dir = PathBuf::from("/Users/example/.local/state/herdr-mirror");
        let name = "remote-development-environment-with-a-very-long-name";
        let remote = RemoteHost::new(&ssh_host(name), &state_dir);
        let stem = socket_stem(&state_dir, name);

        assert!(stem.len() < name.len(), "long name should have been shortened");
        assert!(name.starts_with(stem.split('-').next().unwrap()), "readable prefix kept");
        assert_eq!(
            remote.ctl_path.file_name().unwrap().to_string_lossy(),
            format!("{stem}.ctl")
        );
        // the ControlPath must survive OpenSSH's mux temp suffix on top
        assert!(remote.ctl_path.as_os_str().len() + 17 <= 103);
        assert!(remote.fwd_sock.as_os_str().len() <= 103);
        assert!(remote.exec_sock.as_os_str().len() <= 103);
    }

    #[test]
    fn truncated_stems_never_collide_across_hosts() {
        // The regression this guards: pure truncation gave these one shared
        // stem, so `-O check` found the other host's live master and every ssh
        // command — remote-invoke included — ran on the wrong machine, while
        // the docker relay unlinked the other host's live socket.
        let state_dir = PathBuf::from("/Users/example/.local/state/herdr-mirror");
        let a = "prod-us-east-1-application-server-cluster-node-alpha";
        let b = "prod-us-east-1-application-server-cluster-node-beta";

        // these differ only *past* the cut, so plain truncation collided
        let budget = 103 - state_dir.as_os_str().len() - 1 - 21;
        assert_eq!(a[..budget], b[..budget], "names must be identical up to the cut");

        assert_ne!(
            socket_stem(&state_dir, a),
            socket_stem(&state_dir, b),
            "distinct hosts must not share a socket stem"
        );
        assert_ne!(
            RemoteHost::new(&ssh_host(a), &state_dir).ctl_path,
            RemoteHost::new(&ssh_host(b), &state_dir).ctl_path
        );
    }

    #[test]
    fn socket_stem_hash_is_stable() {
        // Golden values. These bytes are baked into live socket paths, so a
        // change here silently relocates every long-named host's ControlMaster.
        // If a hashing swap ever moves them, this must fail first.
        assert_eq!(short_hash("vps"), "4f02d738");
        assert_eq!(short_hash("prod-us-east-1-application-server-cluster-node-alpha"), "cbd62d65");
    }

    #[test]
    fn short_host_names_keep_their_exact_path() {
        // existing installs must not move to a new socket on upgrade, which
        // would orphan a live ControlMaster
        let state_dir = PathBuf::from("/Users/example/.local/state/herdr-mirror");
        assert_eq!(socket_stem(&state_dir, "vps"), "vps");
        assert_eq!(socket_stem(&state_dir, "work"), "work");
        assert_eq!(
            control_path(&state_dir, "vps"),
            state_dir.join("vps.ctl"),
            "path derivation must match what pre-upgrade daemons used"
        );
    }

    #[test]
    fn socket_stem_survives_multibyte_names_and_a_tiny_budget() {
        let state_dir = PathBuf::from("/Users/example/.local/state/herdr-mirror");
        // truncation must land on a char boundary, never split a code point
        let name = "höst-nàme-with-ünicode-and-a-very-long-tail-that-overflows";
        let stem = socket_stem(&state_dir, name);
        assert!(stem.is_char_boundary(stem.len()));

        // a state dir long enough to eat the whole budget still yields distinct
        // stems rather than one shared path
        let deep = PathBuf::from("/Users/example/".to_string() + &"d".repeat(80));
        assert_ne!(socket_stem(&deep, "alpha-host-name"), socket_stem(&deep, "beta-host-name"));
        assert!(!socket_stem(&deep, "alpha-host-name").is_empty());
    }

    #[test]
    fn removes_stale_control_socket() {
        let path = test_path("stale-control");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        drop(listener);

        remove_stale_control_socket(&path).unwrap();

        assert!(!path.exists());
    }

    #[test]
    fn refuses_to_replace_non_socket_control_path() {
        let path = test_path("non-socket-control");
        fs::write(&path, "do not delete").unwrap();

        let error = remove_stale_control_socket(&path).unwrap_err().to_string();

        assert!(error.contains("is not a socket"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "do not delete");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn version_gate() {
        assert_eq!(version_supported("0.7.1"), Some(false));
        assert_eq!(version_supported("0.7.2"), Some(true));
        assert_eq!(version_supported("0.8.0"), Some(true));
        assert_eq!(version_supported("1.0.0"), Some(true));
        assert_eq!(version_supported("0.7.1-preview.2026-06-30-3459798b606d"), Some(true));
        assert_eq!(version_supported("0.7.1-preview.2026-07-04-aaaa"), Some(true));
        assert_eq!(version_supported("0.7.1-preview.2026-06-29-aaaa"), Some(false));
        assert_eq!(version_supported("garbage"), None);
    }
}
