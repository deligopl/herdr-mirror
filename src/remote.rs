// Workspace commands and API relays use the connection owner through odx-connect.

use std::ffi::OsStr;
#[cfg(test)]
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, timeout_at, Instant};

use crate::api::ApiClient;
use crate::config::HostConfig;
use crate::util::{err, Logger, Result};

/// Marker in the error text for "the container isn't running". A stopped
/// devcontainer is its resting state, unlike an unreachable ssh host, so the
/// daemon backs off gently instead of treating it as a fault.
pub const DORMANT: &str = "dormant";

/// first build with terminal session observe/control
const MIN_PREVIEW_BUILD: &str = "2026-06-30";

#[derive(Debug)]
pub struct RemoteStatus {
    pub socket: String,
    pub supported: bool,
    pub reason: Option<String>,
}

pub(crate) struct CommandOutput {
    pub(crate) code: i32,
    pub(crate) out: String,
    pub(crate) err: String,
}

const COMMAND_TIMEOUT_TERM_GRACE: Duration = Duration::from_secs(2);
const COMMAND_TIMEOUT_KILL_GRACE: Duration = Duration::from_millis(500);
const COMMAND_TIMEOUT_REAP_POLL: Duration = Duration::from_millis(10);

async fn owner_command(args: &[String], timeout_ms: u64) -> CommandOutput {
    run_with_program(OsStr::new(crate::owner_transport::PROGRAM), args, timeout_ms).await
}

pub(crate) async fn run_with_program(
    program: &OsStr,
    args: &[String],
    timeout_ms: u64,
) -> CommandOutput {
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
            return CommandOutput { code: 1, out: String::new(), err: error.to_string() };
        }
    };
    let Some(pgid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        let _ = child.kill().await;
        return CommandOutput {
            code: 1,
            out: String::new(),
            err: "odx-connect process has no usable pid".into(),
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
            return CommandOutput { code: 1, out: String::new(), err: detail };
        }
        Err(_) => {
            let cleanup = terminate_owned_process_group(&mut child, pgid).await;
            abort_pipe_tasks(&stdout, &stderr);
            return match cleanup {
                Ok(()) => CommandOutput { code: 1, out: String::new(), err: "odx-connect timeout".into() },
                Err(error) => CommandOutput {
                    code: 1,
                    out: String::new(),
                    err: format!("odx-connect timeout; process cleanup failed: {error}"),
                },
            };
        }
    };

    match timeout_at(deadline, finish_pipes(&mut stdout, &mut stderr)).await {
        Ok(Ok((stdout, stderr))) => CommandOutput {
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
            CommandOutput { code: 1, out: String::new(), err: detail }
        }
        Err(_) => {
            let cleanup = terminate_owned_process_group(&mut child, pgid).await;
            abort_pipe_tasks(&stdout, &stderr);
            match cleanup {
                Ok(()) => CommandOutput { code: 1, out: String::new(), err: "odx-connect timeout".into() },
                Err(error) => CommandOutput {
                    code: 1,
                    out: String::new(),
                    err: format!("odx-connect timeout; process cleanup failed: {error}"),
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
        sleep(COMMAND_TIMEOUT_REAP_POLL).await;
    }
    Ok(!process_group_exists(pgid)?)
}

async fn terminate_owned_process_group(child: &mut Child, pgid: i32) -> io::Result<()> {
    signal_process_group(pgid, libc::SIGTERM)?;
    let term_deadline = Instant::now() + COMMAND_TIMEOUT_TERM_GRACE;
    while process_group_exists(pgid)? && Instant::now() < term_deadline {
        // Reap the SSH leader as soon as it exits, but keep giving its
        // ProxyCommand descendants the same bounded opportunity to handle TERM.
        child.try_wait()?;
        sleep(COMMAND_TIMEOUT_REAP_POLL).await;
    }
    if process_group_exists(pgid)? {
        // The leader's former PGID still identifies only this invocation.
        signal_process_group(pgid, libc::SIGKILL)?;
    }
    timeout(COMMAND_TIMEOUT_KILL_GRACE, child.wait())
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "ssh leader was not reaped after process-group cleanup",
            )
        })??;
    if !wait_for_process_group_exit(pgid, COMMAND_TIMEOUT_KILL_GRACE).await? {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "owned ssh process group survived SIGKILL",
        ));
    }
    Ok(())
}

pub struct RemoteHost {
    pub cfg: HostConfig,
    pub fwd_sock: PathBuf,
    /// docker hosts only: resolved container + chosen stdio bridge
    container: Option<crate::docker::Container>,
    /// docker hosts only: owns the relay listener. Dropping it stops serving
    /// and unlinks the socket, so a reconnect never inherits a dead one.
    relay: Option<crate::docker::RelayHandle>,
    /// Owns the local API relay into the workspace owner.
    exec_relay: Option<crate::owner_relay::RelayHandle>,
    log: Logger,
}

/// Stable FNV-1a suffix keeps bounded local socket names distinct.
fn short_hash(s: &str) -> String {
    use std::hash::Hasher;

    let mut hasher = fnv::FnvHasher::default();
    hasher.write(s.as_bytes());
    format!("{:08x}", hasher.finish() as u32)
}

/// Bound local relay paths to fit sockaddr_un, preserving distinct hosts.
fn socket_stem(state_dir: &std::path::Path, host_name: &str) -> String {
    use std::os::unix::ffi::OsStrExt;

    // macOS reserves one byte of sockaddr_un's 104-byte path for NUL; Linux
    // allows 108, and we deliberately apply the tighter bound on both so a
    // hosts.toml is portable. Preserve the existing conservative name budget
    // so cooperating Mirror processes derive identical relay socket paths.
    const MAX_SOCKET_PATH_BYTES: usize = 103;
    const SOCKET_OVERHEAD: usize = 21;

    let directory_bytes = state_dir.as_os_str().as_bytes().len() + 1;
    let budget = MAX_SOCKET_PATH_BYTES.saturating_sub(directory_bytes + SOCKET_OVERHEAD);
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

pub(crate) fn forward_socket_path(
    state_dir: &std::path::Path,
    host_name: &str,
) -> PathBuf {
    state_dir.join(format!("{}-api.sock", socket_stem(state_dir, host_name)))
}

impl RemoteHost {
    pub fn new(cfg: &HostConfig, state_dir: &std::path::Path) -> RemoteHost {
        RemoteHost {
            fwd_sock: forward_socket_path(state_dir, &cfg.name),
            cfg: cfg.clone(),
            container: None,
            relay: None,
            exec_relay: None,
            log: Logger::new(state_dir, false),
        }
    }

    pub async fn ensure_ready(&mut self) -> Result<()> {
        if !self.cfg.kind.is_docker() {
            return Ok(());
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

    pub async fn exec(&self, command: &str, timeout_ms: u64) -> Result<String> {
        if let Some(c) = &self.container {
            return c.exec(command, timeout_ms).await;
        }
        let args = vec![self.cfg.target.clone(), command.to_string()];
        let res = owner_command(&args, timeout_ms).await;
        if res.code != 0 {
            return Err(err(format!(
                "odx-connect failed ({command}): {}",
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

    /// Bridge the remote Herdr socket through odx-connect.
    async fn exec_relay_transport(&mut self, remote_socket: &str) -> Result<PathBuf> {
        // NEVER steal a healthy relay — same reasoning as the docker guard:
        // the socket path is per-host but shared across processes (daemon,
        // `remote-*` actions, `once`), and state_dir is a single fixed path.
        if self.exec_relay.is_none() && ApiClient::connect(&self.fwd_sock).await.is_ok() {
            return Ok(self.fwd_sock.clone());
        }
        self.exec_relay = None;
        let relay_cmd = crate::owner_relay::detect_relay_command(self, remote_socket).await?;
        self.log.log(&format!(
            "[{}] exec relay via {} → {remote_socket}",
            self.cfg.name,
            relay_cmd.tool()
        ));
        let handle = crate::owner_relay::serve_relay(
            self.cfg.target.clone(),
            relay_cmd,
            self.fwd_sock.clone(),
            self.log.clone(),
        )?;
        let path = handle.path.clone();
        self.exec_relay = Some(handle);
        Ok(path)
    }

    pub async fn connect_api(&mut self) -> Result<(ApiClient, RemoteStatus)> {
        self.ensure_ready().await?;
        let status = match self.status().await {
            Ok(s) => s,
            Err(e) if !crate::owner_transport::stopped_refusal(&e.to_string()) => {
                // A transient owner handover can briefly refuse its socket; retry once.
                tokio::time::sleep(Duration::from_secs(1)).await;
                self.status().await?
            }
            Err(e) => return Err(e),
        };
        if !status.supported {
            return Err(err(status.reason.clone().unwrap_or_else(|| "remote unsupported".into())));
        }
        // The ping verifies the owner relay; Docker resolves its relay below.
        let container = self.container.clone();
        let sock = match &container {
            None => return Ok((ApiClient::connect(&self.exec_relay_transport(&status.socket).await?).await?, status)),
            Some(c) => {
                // NEVER steal a healthy relay — the socket path is per-HOST but
                // shared across processes (daemon, `remote-*` actions, `once`),
                // and state_dir is deliberately a single fixed path. Binding on
                // top of a live one orphans the owner's listener and then
                // unlinks the path from under it, bouncing the daemon's whole
                // host connection on every remote action.
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
pub(crate) fn version_supported(version: &str) -> Option<bool> {
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
    use std::path::Path;
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

    fn fake_transport_guard() -> std::sync::MutexGuard<'static, ()> {
        // A panicking regression must not make every later one fail to acquire.
        FAKE_SSH.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A path no other test in this process can produce.
    ///
    /// The clock alone was not enough: `cargo test` runs this module's tests on
    /// several threads at once, `SystemTime::now()` is not guaranteed to
    /// advance between two of them, and every test asks for a program called
    /// `fake-transport`. Two colliding calls shared one file, and the first test to
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
            always_control: true,
        }
    }

    fn fake_transport() -> PathBuf {
        let path = test_path("fake-transport");
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

    #[tokio::test]
    async fn command_timeout_reaps_its_process_tree_across_retries() {
        let _serial = fake_transport_guard();
        let program = fake_transport();
        let mut survivors = Vec::new();

        for attempt in 0..3 {
            let leader_file = test_path(&format!("timeout-leader-{attempt}"));
            let child_file = test_path(&format!("timeout-child-{attempt}"));
            let args = vec![
                "timeout".into(),
                leader_file.display().to_string(),
                child_file.display().to_string(),
            ];

            let output = run_with_program(program.as_os_str(), &args, 2000).await;
            assert_eq!(output.err, "odx-connect timeout");
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
    async fn command_preserves_ordinary_output_and_status() {
        let _serial = fake_transport_guard();
        let program = fake_transport();
        let output = run_with_program(program.as_os_str(), &["output".into()], 1000).await;
        let _ = fs::remove_file(program);

        assert_eq!(output.code, 7);
        assert_eq!(output.out, "ordinary stdout");
        assert_eq!(output.err, "ordinary stderr");
    }

    #[tokio::test]
    async fn command_timeout_includes_pipes_held_by_a_descendant() {
        let _serial = fake_transport_guard();
        let program = fake_transport();
        let leader_file = test_path("pipe-leader");
        let child_file = test_path("pipe-child");
        let args = vec![
            "pipe-hold".into(),
            leader_file.display().to_string(),
            child_file.display().to_string(),
        ];

        let bounded = tokio::time::timeout(
            Duration::from_secs(4),
            run_with_program(program.as_os_str(), &args, 500),
        )
        .await;
        let leader = recorded_pid(&leader_file).await;
        let child = recorded_pid(&child_file).await;
        if bounded.is_err() {
            kill_pid(child);
            kill_pid(leader);
        }
        let output = bounded.expect("ssh helper hung after its direct child exited");
        assert_eq!(output.err, "odx-connect timeout");
        // Both assertions above already proved these exited, and their pids are
        // free for the kernel to reuse, so there is nothing left to signal.
        assert!(wait_for_pid_exit(leader).await, "direct ssh child survived timeout");
        assert!(wait_for_pid_exit(child).await, "pipe-holding descendant survived timeout");

        let _ = fs::remove_file(leader_file);
        let _ = fs::remove_file(child_file);
        let _ = fs::remove_file(program);
    }

    #[tokio::test]
    async fn command_timeout_gives_the_whole_group_term_grace() {
        let _serial = fake_transport_guard();
        let program = fake_transport();
        let leader_file = test_path("term-leader");
        let child_file = test_path("term-child");
        let cleanup_file = test_path("term-cleanup");
        let args = vec![
            "term-cleanup".into(),
            leader_file.display().to_string(),
            child_file.display().to_string(),
            cleanup_file.display().to_string(),
        ];

        let output = run_with_program(program.as_os_str(), &args, 2000).await;
        let leader = recorded_pid(&leader_file).await;
        let child = recorded_pid(&child_file).await;
        assert_eq!(output.err, "odx-connect timeout");
        assert_eq!(fs::read_to_string(&cleanup_file).unwrap(), "cleaned");
        assert!(wait_for_pid_exit(leader).await, "direct ssh child survived timeout");
        assert!(wait_for_pid_exit(child).await, "graceful descendant survived timeout");

        let _ = fs::remove_file(leader_file);
        let _ = fs::remove_file(child_file);
        let _ = fs::remove_file(cleanup_file);
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
        assert!(remote.fwd_sock.as_os_str().len() <= 103);
    }

    #[test]
    fn truncated_stems_never_collide_across_hosts() {
        // A shared truncated stem would reach or replace the wrong host's relay.
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
            RemoteHost::new(&ssh_host(a), &state_dir).fwd_sock,
            RemoteHost::new(&ssh_host(b), &state_dir).fwd_sock
        );
    }

    #[test]
    fn socket_stem_hash_is_stable() {
        // Golden values. These bytes are baked into live socket paths, so a
        // change here silently relocates every long-named host's relay socket.
        // If a hashing swap ever moves them, this must fail first.
        assert_eq!(short_hash("vps"), "4f02d738");
        assert_eq!(short_hash("prod-us-east-1-application-server-cluster-node-alpha"), "cbd62d65");
    }

    #[test]
    fn short_host_names_keep_their_exact_path() {
        // Cooperating processes must agree on local relay socket names.
        let state_dir = PathBuf::from("/Users/example/.local/state/herdr-mirror");
        assert_eq!(socket_stem(&state_dir, "vps"), "vps");
        assert_eq!(socket_stem(&state_dir, "work"), "work");
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
