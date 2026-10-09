// herdr-mirror pane wrapper (data plane).
//
// Runs inside a local herdr pane and shows a remote herdr pane's terminal,
// live, over ssh. Read-only observe by default; escalates to a writable
// control session when the user types and releases back to observe.
//
//   herdr-mirror pane <ssh-target> <pane-target> [options]
//
// options:
//   --remote-bin PATH   remote herdr binary (default: PATH, then ~/.local/bin/herdr)
//   --cols N --rows N   observe request size (default 240x72; must be >= the
//                       remote PTY size or the server clips bottom rows away)
//   --dump              headless mode: print plain-text screen per frame
//   --session NAME      remote named session (passed as --session to herdr)
//   --control-idle N    auto-release control after N seconds idle (default 3600)
//   --always-control    start and stay in control: writable, no idle release,
//                       and sized to the local pane so it fills
//   --max-cols N        cap the size control asks the remote for (default:
//   --max-rows N        uncapped — control fills the local pane). Set for a
//                       remote with its own display: the remote keeps its own
//                       geometry and the rest of the local pane stays blank.
//
// Every stream gets its own direct ssh connection (no shared ControlMaster):
// isolated, and nothing persists to go stale on a flaky network.
//
// The streamer OWNS the remote client it attaches. The remote
// `herdr … terminal session …` process is not a child of the ssh (or docker
// exec) that carries its stdio and does not die with it, so every path out of
// this process — and every reconnect — terminates it explicitly through the
// same transport. See `remote_session_script` and `kill_remote_client`.
//
// One owner of all state, message-driven: frames, keystrokes, timers, and
// ssh-child exits arrive on one channel; a session generation number tags
// every message so stale ones are dropped.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::util::{err, Result};
use crate::grid::{Grid, Renderer};
use crate::foreground::Fg;
use crate::predict::Predictor;
use crate::select::{Released, Select};

// ---------------------------------------------------------------------------
// args

#[derive(Debug, Clone)]
pub struct Args {
    pub ssh_target: String,
    pub pane_target: String,
    /// Configured remote herdr path. `None` = auto-resolve on the remote
    /// (PATH, then `~/.local/bin/herdr`). See `config::remote_herdr_expr`.
    pub remote_bin: Option<String>,
    pub cols: usize,
    pub rows: usize,
    pub dump: bool,
    pub session: Option<String>,
    /// auto-release control after this much input idle; 0 disables
    pub control_idle_secs: u64,
    /// start and stay in control: writable, no idle release, and sized to the
    /// local pane so it fills. Set by the daemon from per-host config.
    pub always_control: bool,
    /// upper bound on the size control asks the remote for. `None` = uncapped
    /// (fill the local pane). Set by the daemon from per-host config; observe
    /// is never capped, since it doesn't resize anything.
    pub max_cols: Option<usize>,
    pub max_rows: Option<usize>,
    /// daemon's ssh ControlMaster socket for this host; foreground polls reuse it
    /// (`ssh -S <path>`) to skip a handshake. None → polls connect directly.
    ///
    pub ctl_path: Option<String>,
    /// Daemon-owned API forward used for pane metadata.  It is a local Unix
    /// socket even when its remote side is direct-tcpip.
    pub api_socket: Option<String>,
    /// Mirror host key for the daemon's shared HostHealth admission gate.
    pub host_name: Option<String>,
    /// container to exec into instead of ssh. `None` = ssh host.
    pub container: Option<ContainerArg>,
}

/// How the pane process should reach its container. The daemon passes a *ref*,
/// not a resolved id: the pane may outlive a rebuild, and ids change while the
/// folder label does not.
#[derive(Debug, Clone)]
pub struct ContainerArg {
    pub kind: crate::config::HostKind,
    pub docker_bin: String,
}

pub fn parse_args(argv: &[String]) -> Result<Args> {
    let mut args = Args {
        ssh_target: String::new(),
        pane_target: String::new(),
        remote_bin: None,
        cols: 240,
        rows: 72,
        dump: false,
        session: None,
        control_idle_secs: 3600,
        always_control: false,
        max_cols: None,
        max_rows: None,
        ctl_path: None,
        api_socket: None,
        host_name: None,
        container: None,
    };
    let mut container_name: Option<String> = None;
    let mut container_folder: Option<String> = None;
    let mut docker_bin = "docker".to_string();
    let mut positional: Vec<String> = Vec::new();
    let mut it = argv.iter();
    while let Some(a) = it.next() {
        let mut next = |flag: &str| -> Result<String> {
            it.next().cloned().ok_or_else(|| err(format!("{flag} needs a value")))
        };
        match a.as_str() {
            "--remote-bin" => args.remote_bin = Some(next("--remote-bin")?),
            "--cols" => {
                args.cols = next("--cols")?.parse().map_err(|_| err("--cols must be a number"))?;
            }
            "--rows" => {
                args.rows = next("--rows")?.parse().map_err(|_| err("--rows must be a number"))?;
            }
            "--session" => args.session = Some(next("--session")?),
            "--control-idle" => {
                args.control_idle_secs =
                    next("--control-idle")?.parse().map_err(|_| err("--control-idle must be a number"))?
            }
            "--always-control" => args.always_control = true,
            // 0 is unset here for the same reason config treats it that way:
            // a zero cap would ask the remote for a zero-column terminal, which
            // herdr rejects outright, killing the session twice over and
            // stranding the pane in "control unavailable" over a typo.
            "--max-cols" => {
                args.max_cols = Some(next("--max-cols")?.parse().map_err(|_| err("--max-cols must be a number"))?)
                    .filter(|&n| n > 0)
            }
            "--max-rows" => {
                args.max_rows = Some(next("--max-rows")?.parse().map_err(|_| err("--max-rows must be a number"))?)
                    .filter(|&n| n > 0)
            }
            "--ctl-path" => args.ctl_path = Some(next("--ctl-path")?),
            "--api-socket" => args.api_socket = Some(next("--api-socket")?),
            "--host-name" => args.host_name = Some(next("--host-name")?),
            "--container" => container_name = Some(next("--container")?),
            "--container-folder" => container_folder = Some(next("--container-folder")?),
            "--docker-bin" => docker_bin = next("--docker-bin")?,
            "--dump" => args.dump = true,
            other if other.starts_with('-') => return Err(err(format!("unknown option: {other}"))),
            other => positional.push(other.to_string()),
        }
    }
    if positional.len() != 2 {
        return Err(err(
            "usage: herdr-mirror pane <ssh-target> <pane-target> [--remote-bin PATH] [--session NAME] [--cols N --rows N] [--max-cols N --max-rows N] [--dump]",
        ));
    }
    args.container = match (container_name, container_folder) {
        (Some(_), Some(_)) => return Err(err("--container and --container-folder are exclusive")),
        (Some(n), None) => {
            Some(ContainerArg { kind: crate::config::HostKind::DockerContainer(n), docker_bin })
        }
        (None, Some(f)) => {
            Some(ContainerArg { kind: crate::config::HostKind::DockerFolder(f), docker_bin })
        }
        (None, None) => None,
    };
    args.ssh_target = positional.remove(0);
    args.pane_target = positional.remove(0);
    Ok(args)
}

// ---------------------------------------------------------------------------
// remote session: one ssh child running observe or control

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Observe,
    Control,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Observe => "observe",
            Mode::Control => "control",
        }
    }
}

#[derive(Debug, Deserialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    seq: Option<u64>,
    full: Option<bool>,
    width: Option<usize>,
    height: Option<usize>,
    bytes: Option<String>,
    reason: Option<String>,
}

enum Msg {
    Frame { gen: u64, frame: Frame },
    /// pid of the remote client this session attached (its first stdout line)
    RemotePid { gen: u64, pid: i32 },
    SessionExit { gen: u64, mode: Mode, reason: String, uptime: Duration },
    Stdin(Vec<u8>),
    /// result of a background foreground poll; None=poll failed (keep the last
    /// value)
    Foreground(Option<Fg>),
    /// the remote pane's own content revision from that same poll; None=unknown
    RemoteRevision(Option<u64>),
    Paste(crate::paste::Outcome),
    Drop(crate::paste::DropResult),
    /// the pane's stdin closed (a view's tile went away)
    StdinEof,
}

struct Session {
    gen: u64,
    mode: Mode,
    /// local transport child (ssh, or `docker exec`), started as its own
    /// process-group leader so its pid is also the group id
    process_group: i32,
    /// remote `herdr … terminal session …` client, once it has announced
    /// itself. `None` until that line arrives — the attach may fail before the
    /// wrapper ever runs — so every cleanup treats it as optional.
    remote_pid: Option<i32>,
    stdin: ChildStdin,
}

async fn write_terminal_input(
    stdin: &mut (impl tokio::io::AsyncWrite + Unpin),
    bytes: &[u8],
) -> std::io::Result<()> {
    let line = json!({ "type": "terminal.input", "bytes": B64.encode(bytes) }).to_string()
        + "\n";
    stdin.write_all(line.as_bytes()).await
}

/// Stop the transport and every local helper it spawned (notably an ssh
/// ProxyCommand such as `aws ssm start-session`). Each transport is started as
/// its own process-group leader, so this cannot signal the pane wrapper itself.
fn kill_session_process_group(process_group: i32) {
    if process_group > 0 {
        unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }
}

fn isolate_session_process_group(command: &mut tokio::process::Command) {
    command.process_group(0);
}

/// POSIX single-quote: an embedded ' can't break the remote shell parse.
pub(crate) fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn ssh_stream_args(ssh_target: &str, cmd: &str) -> Vec<String> {
    let mut argv: Vec<String> = crate::remote::SSH_COMMON_OPTS
        .iter()
        .map(|arg| (*arg).to_string())
        .collect();
    // A pane stream is long-lived and interactive. It must not inherit a
    // ControlPath from ~/.ssh/config: a stale shared master can hang
    // the stream before the remote command starts. `-S none` disables control
    // socket use without changing the daemon's intentional multiplexing.
    argv.extend(["-S".into(), "none".into(), ssh_target.into(), cmd.into()]);
    argv
}

/// Prefix the remote wrapper prints, on its own line, before it becomes the
/// client. Chosen to be inert to the frame reader (not JSON) and unmistakable
/// in a transcript.
const REMOTE_PID_MARKER: &str = "herdr-mirror-remote-pid";

/// Read the marker line the remote wrapper prints. `None` for every other line,
/// so an unpatched remote, a login banner, or a herdr frame passes through.
fn remote_pid_from_line(line: &str) -> Option<i32> {
    let mut parts = line.split_whitespace();
    (parts.next()? == REMOTE_PID_MARKER)
        .then(|| parts.next())
        .flatten()
        .filter(|_| parts.next().is_none())
        .and_then(|pid| pid.parse::<i32>().ok())
        .filter(|pid| *pid > 1)
}

/// Run a script under a POSIX `sh` we name, whatever the remote login shell is.
///
/// `ssh host <string>` hands the string to the account's login shell, which may
/// be fish or csh. Both keep single quotes literal and both have `exec`, so the
/// only thing they parse is `exec sh -c '<literal>'`; everything inside — `$$`,
/// `$(…)`, `case` — is then read by `sh`. This is the same reasoning that put
/// `config::remote_herdr_expr`'s resolver behind `sh -c`. `exec` keeps it to
/// one process, so the pid `$$` reports is the pid that survives.
fn sh_wrapped(script: &str) -> String {
    format!("exec sh -c {}", sh_quote(script))
}

/// The remote script for one attach: announce the pid, then become the client.
///
/// `$$` is the wrapper shell's pid and `exec` gives that same pid to herdr, so
/// the number printed is the pid of the process that ends up attached — not of
/// a parent that exits a moment later. Knowing it is what lets this streamer
/// terminate its own client; without it the client outlives every transport
/// failure and every local pane close, and the leftovers hold the terminal
/// against the next streamer.
fn remote_session_script(args: &Args, mode: Mode, cols: usize, rows: usize) -> String {
    // Configured paths stay unquoted so remote-shell ~ expands; auto mode is an
    // `sh -c` resolver that takes the trailing words as "$@" (see
    // config::remote_herdr_expr).
    let bin = crate::config::remote_herdr_expr(
        args.remote_bin.as_deref(),
        args.session.as_deref(),
    );
    format!(
        "echo {marker} $$; exec {bin} terminal session {mode} {target} --cols {cols} --rows {rows}",
        marker = REMOTE_PID_MARKER,
        mode = mode.as_str(),
        target = sh_quote(&args.pane_target),
    )
}

/// Terminate one remote attach client, identified before it is signalled.
///
/// `ps` is asked what the pid actually is and only a process whose argv still
/// carries `terminal session` is signalled. A recorded pid can be minutes or
/// hours old, remote pids get reused, and the same host runs the herdr *server*
/// — so an unguarded `kill` is how a cleanup turns into an outage. A remote
/// without `ps` (a stripped container) falls back to the plain kill: there the
/// pid was recorded by this process and the reuse window is the seconds since.
fn remote_kill_script(pid: i32) -> String {
    format!(
        "p={pid}; if command -v ps >/dev/null 2>&1; then \
case \"$(ps -o args= -p \"$p\" 2>/dev/null)\" in *\"terminal session\"*) kill -TERM \"$p\" ;; esac; \
else kill -TERM \"$p\" 2>/dev/null || true; fi"
    )
}

/// How long one remote-client kill may take. Short on purpose: this runs on
/// exit paths (SIGTERM from the supervisor, a closing pane) and before each
/// reconnect, and a cleanup that delays either is worse than one that misses.
const REMOTE_KILL_TIMEOUT: Duration = Duration::from_secs(2);

/// How long one remote-client kill may take during an explicit host
/// suspension (`reap_host_remote_clients`). That pass is the last identity-
/// guarded cleanup before the host transport is closed, nobody is waiting on
/// a pane, and it may have to open a fresh ssh connection: the daemon's control
/// master can already be unusable then. On VM hosts (ProxyCommand to the VM
/// host, then the sandbox proxy) such a round trip measures 4–6 s, so the 2 s
/// exit-path bound turned every suspension there into a refusal with the
/// remote client still alive.
const SUSPEND_REMOTE_KILL_TIMEOUT: Duration = Duration::from_secs(12);

/// Kill a remote client through the same transport that started it. Best
/// effort: `true` only when the command ran and reported success, so a caller
/// can keep an unresolved pid for the next attempt instead of leaking it.
pub(crate) async fn kill_remote_client(args: &Args, pid: i32) -> bool {
    kill_remote_client_within(args, pid, REMOTE_KILL_TIMEOUT).await
}

/// `kill_remote_client` with an explicit bound on the whole command.
async fn kill_remote_client_within(args: &Args, pid: i32, limit: Duration) -> bool {
    if pid <= 1 {
        return true;
    }
    let script = remote_kill_script(pid);
    let mut builder = match &args.container {
        Some(ct) => {
            // async resolve, like the foreground poll: this can run while the
            // streamer is still drawing, and a wedged Docker daemon must not
            // freeze rendering or the exit path
            let Some(id) = crate::docker::resolve(&ct.docker_bin, &ct.kind)
                .await
                .ok()
                .and_then(|ids| ids.into_iter().next())
            else {
                return false;
            };
            let mut c = tokio::process::Command::new(&ct.docker_bin);
            c.args(["exec", &id, "sh", "-c", &script]);
            c
        }
        None => {
            let mut c = tokio::process::Command::new("ssh");
            // reuse the daemon's ControlMaster when we were given one: this is
            // a sub-second command and a fresh handshake would dominate it
            if let Some(path) = &args.ctl_path {
                c.arg("-S").arg(path);
            }
            c.args(crate::remote::SSH_COMMON_OPTS).arg(&args.ssh_target).arg(sh_wrapped(&script));
            c
        }
    };
    // never onto the pane's tty: this runs while the streamer is drawing
    let child = builder
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .status();
    matches!(tokio::time::timeout(limit, child).await, Ok(Ok(status)) if status.success())
}

/// Clear a remote client recorded by a streamer that is no longer running.
/// Used by the supervisor, whose child can be SIGKILLed before it cleans up.
async fn reap_recorded_remote_client(args: &Args, state_dir: &std::path::Path) {
    let Some(pid) =
        crate::util::take_remote_client(state_dir, &args.ssh_target, &args.pane_target)
    else {
        return;
    };
    preserve_unresolved_remote_client(args, state_dir, pid, kill_remote_client(args, pid).await);
}

fn preserve_unresolved_remote_client(
    args: &Args,
    state_dir: &std::path::Path,
    pid: i32,
    reaped: bool,
) {
    if !reaped {
        // A hidden/stopped host has no replacement streamer to adopt this
        // record later. Keep the only safe handle on the remote client so an
        // explicit suspension can retry it before the transport is closed.
        crate::util::record_remote_client(
            state_dir,
            &args.ssh_target,
            &args.pane_target,
            pid,
        );
    }
}

/// Finish the remote-client half of suspending one host.
///
/// Local pane closure terminates the streamer supervisor first. Each
/// supervisor normally clears its own remote attach client; this bounded pass
/// handles a client whose earlier cleanup could not complete. A surviving
/// record is a refusal, because ending the host transport at that point would
/// discard the only identity-guarded cleanup path.
pub(crate) async fn reap_host_remote_clients(
    host: &crate::config::HostConfig,
    state_dir: &std::path::Path,
    pane_targets: &[String],
) -> Result<()> {
    let sizes = std::collections::HashMap::new();
    let command_for = crate::mirror::cmd_for_pane(host, state_dir, &sizes);
    let mut unresolved = Vec::new();
    for pane_target in pane_targets {
        let argv = command_for(pane_target);
        let args = parse_args(&argv[2..])?;
        let Some(pid) = crate::util::take_remote_client(
            state_dir,
            &args.ssh_target,
            &args.pane_target,
        ) else {
            continue;
        };
        if !kill_remote_client_within(&args, pid, SUSPEND_REMOTE_KILL_TIMEOUT).await {
            crate::util::record_remote_client(
                state_dir,
                &args.ssh_target,
                &args.pane_target,
                pid,
            );
            unresolved.push(pane_target.clone());
        }
    }
    if unresolved.is_empty() {
        Ok(())
    } else {
        Err(err(format!(
            "remote stream clients still owned by {}: {}",
            host.name,
            unresolved.join(", ")
        )))
    }
}

fn spawn_session(args: &Args, mode: Mode, cols: usize, rows: usize, gen: u64, tx: mpsc::Sender<Msg>) -> Result<Session> {
    let cmd = remote_session_script(args, mode, cols, rows);
    // ssh and docker differ only in how the command is carried; the streaming
    // contract (piped stdio, herdr's frames on stdout) is identical
    let mut builder = match &args.container {
        None => {
            let mut c = tokio::process::Command::new("ssh");
            c.args(ssh_stream_args(&args.ssh_target, &sh_wrapped(&cmd)));
            c
        }
        Some(ct) => {
            // resolve per spawn so a rebuilt container is picked up on
            // reconnect. Bounded: this runs on the pane's single-threaded
            // runtime, so a wedged Docker daemon must not be able to freeze
            // input, rendering or signal handling.
            let id = crate::docker::resolve_blocking(
                &ct.docker_bin,
                &ct.kind,
                Duration::from_secs(5),
            )?;
            let mut c = tokio::process::Command::new(&ct.docker_bin);
            // `sh -c` not `-lc`: match ssh's non-login remote shell
            c.args(["exec", "-i", &id, "sh", "-c", &cmd]);
            c
        }
    };
    // Keep the transport and all of its local descendants in one independently
    // killable group. Killing only ssh can orphan a ProxyCommand and leave its
    // remote terminal controller attached after the mirror pane closes.
    isolate_session_process_group(&mut builder);
    let mut child = builder
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let process_group = child.id().map(|p| p as i32).unwrap_or(0);
    let stdin = child.stdin.take().ok_or_else(|| err("no child stdin"))?;
    let stdout = child.stdout.take().ok_or_else(|| err("no child stdout"))?;
    let stderr = child.stderr.take().ok_or_else(|| err("no child stderr"))?;
    let started = Instant::now();

    tokio::spawn(async move {
        // ssh errors arrive on stderr; the server's failure reason arrives as
        // a terminal.closed frame on STDOUT — capture both
        let err_tail: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let err_tail2 = err_tail.clone();
        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(l)) = lines.next_line().await {
                let mut buf = err_tail2.lock().unwrap();
                buf.push_str(&l);
                buf.push('\n');
                if buf.len() > 400 {
                    let tail: String = buf.chars().rev().take(400).collect::<Vec<_>>().into_iter().rev().collect();
                    *buf = tail;
                }
            }
        });
        let mut close_reason = String::new();
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(remote_pid) = remote_pid_from_line(&line) {
                if tx.send(Msg::RemotePid { gen, pid: remote_pid }).await.is_err() {
                    break;
                }
                continue;
            }
            let Ok(frame) = serde_json::from_str::<Frame>(&line) else { continue };
            if frame.kind == "terminal.closed" {
                if let Some(r) = &frame.reason {
                    close_reason = r.clone();
                }
            }
            if tx.send(Msg::Frame { gen, frame }).await.is_err() {
                break;
            }
        }
        let _ = child.wait().await;
        stderr_task.abort();
        let tail = err_tail.lock().unwrap().trim().to_string();
        let reason = if close_reason.is_empty() { tail } else { close_reason };
        let _ = tx.send(Msg::SessionExit { gen, mode, reason, uptime: started.elapsed() }).await;
    });

    Ok(Session { gen, mode, process_group, remote_pid: None, stdin })
}

// ---------------------------------------------------------------------------
// terminal plumbing

/// The layout herdr renders when a server has NO client attached: MIN_COLS x
/// MIN_ROWS from its headless server. Not a minimum — an attached client
/// smaller than this gets its real size — it is specifically the placeholder
/// used when nobody is watching.
///
/// Every derivation from it subtracts: sidebar, tab bar, splits, gaps, the
/// scrollbar gutter. So a pane born under that layout cannot EXCEED it on
/// either axis, which is what makes this a sound upper bound rather than a
/// shape to match. Shape matching is the trap: a phone lays out to the same
/// rectangle as the placeholder.
const HERDR_NO_CLIENT_LAYOUT: (usize, usize) = (80, 24);

/// Could this pane's size have come from a layout nobody is watching?
///
/// Strictly larger on either axis is provably a real viewport. `>` not `>=`:
/// herdr spawns restored panes at exactly 24x80, so `>=` would trust a
/// placeholder.
///
/// Consulted at birth to pick the initial mode. Deliberately NOT consulted on
/// the promotion path: a resize is taken as evidence of a client, which holds
/// in practice but is empirical rather than structural — herdr has one
/// clientless resize path (its first virtual render), so a pane created in the
/// instant before that render could in principle promote on a placeholder
/// resize. It self-heals the moment a client attaches.
fn size_is_trusted((cols, rows): (usize, usize)) -> bool {
    cols > HERDR_NO_CLIENT_LAYOUT.0 || rows > HERDR_NO_CLIENT_LAYOUT.1
}

/// Clamp a local terminal size to the per-host control caps. Split out from
/// `control_size` so the arithmetic is testable without an `App`.
fn cap_size(
    (cols, rows): (usize, usize),
    max_cols: Option<usize>,
    max_rows: Option<usize>,
) -> (usize, usize) {
    (
        max_cols.map_or(cols, |cap| cols.min(cap)),
        max_rows.map_or(rows, |cap| rows.min(cap)),
    )
}

/// Size to request for an observe stream. Split out from `App::observe_size` so
/// the floor is testable.
///
/// `--cols/--rows` are a floor, never an exact request. As a floor they still do
/// their original job: the request must be >= the remote PTY size or the server
/// clips its bottom rows away, and the daemon's numbers already carry a margin.
/// As an exact request they are wrong — the daemon samples the *remote* pane's
/// rect when it spawns the streamer, and a headless remote reports the no-client
/// placeholder, so the numbers are small. Control then resizes the remote pty to
/// this pane and nothing shrinks it back on release, so asking for the daemon's
/// numbers again would stream a crop of a screen that has since grown, painted
/// into the corner of a much larger pane.
fn observe_size_for(args: &Args, term: (usize, usize)) -> (usize, usize) {
    (args.cols.max(term.0), args.rows.max(term.1))
}

/// Mode to open with. Split out from `run` so the composition is testable.
fn initial_mode(always_control: bool, size: (usize, usize)) -> Mode {
    if always_control && size_is_trusted(size) {
        Mode::Control
    } else {
        Mode::Observe
    }
}

fn term_size() -> (usize, usize) {
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 && ws.ws_row > 0 {
            return (ws.ws_col as usize, ws.ws_row as usize);
        }
    }
    (80, 24)
}

struct RawMode {
    orig: libc::termios,
}

impl RawMode {
    fn enable() -> Option<RawMode> {
        unsafe {
            let mut orig: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(libc::STDIN_FILENO, &mut orig) != 0 {
                return None;
            }
            let mut raw = orig;
            libc::cfmakeraw(&mut raw);
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw) != 0 {
                return None;
            }
            Some(RawMode { orig })
        }
    }

    fn restore(&self) {
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &self.orig);
        }
    }
}

fn write_stdout(s: &str) {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(s.as_bytes());
    let _ = out.flush();
}

/// One SGR mouse event: ESC [ < btn ; col ; row (M|m). Returns (btn, col, row,
/// press, total len) for a sequence starting at `bytes[at]`.
fn parse_mouse(bytes: &[u8], at: usize) -> Option<(u32, u32, u32, bool, usize)> {
    let rest = &bytes[at..];
    if rest.len() < 6 || rest[0] != 0x1b || rest[1] != b'[' || rest[2] != b'<' {
        return None;
    }
    let mut nums = [0u32; 3];
    let mut n = 0usize;
    let mut i = 3usize;
    let mut have_digit = false;
    while i < rest.len() && n < 3 {
        match rest[i] {
            b'0'..=b'9' => {
                // saturate: garbage digit runs on stdin must not overflow-panic
                nums[n] = nums[n].saturating_mul(10).saturating_add((rest[i] - b'0') as u32);
                have_digit = true;
                i += 1;
            }
            b';' if n < 2 && have_digit => {
                n += 1;
                have_digit = false;
                i += 1;
            }
            b'M' | b'm' if n == 2 && have_digit => {
                return Some((nums[0], nums[1], nums[2], rest[i] == b'M', i + 1));
            }
            _ => return None,
        }
    }
    None
}

const MOUSE_INPUT_TIMEOUT: Duration = Duration::from_millis(150);
const MAX_INCOMPLETE_MOUSE_BYTES: usize = 32;

#[derive(Debug, PartialEq)]
pub(crate) enum MouseSplit {
    Pending,
    Passthrough(Vec<u8>),
}

/// Find a possible SGR mouse sequence that ends at the current read boundary.
/// Only a short, syntactically valid prefix is held; ordinary input keeps
/// flowing to the existing routing code.
fn trailing_incomplete_mouse(bytes: &[u8]) -> Option<usize> {
    for (at, byte) in bytes.iter().enumerate() {
        if *byte != 0x1b {
            continue;
        }
        let rest = &bytes[at..];
        if rest == b"\x1b" || rest == b"\x1b[" {
            return Some(at);
        }
        if rest.starts_with(b"\x1b[<")
            && rest.len() <= MAX_INCOMPLETE_MOUSE_BYTES
            && rest[3..].iter().all(|b| b.is_ascii_digit() || *b == b';')
        {
            return Some(at);
        }
    }
    None
}

/// Hold only a trailing incomplete mouse sequence across stdin reads.
pub(crate) fn split_mouse(buf: &mut Vec<u8>, chunk: Vec<u8>) -> MouseSplit {
    let mut all = std::mem::take(buf);
    all.extend_from_slice(&chunk);
    let Some(at) = trailing_incomplete_mouse(&all) else {
        return MouseSplit::Passthrough(all);
    };
    let tail = all.split_off(at);
    *buf = tail;
    if all.is_empty() {
        MouseSplit::Pending
    } else {
        MouseSplit::Passthrough(all)
    }
}

/// A timed-out lone ESC is still a key; an incomplete mouse prefix is dropped
/// so it cannot leak as literal input into a remote shell.
fn flush_mouse(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    let pending = std::mem::take(buf);
    (pending == b"\x1b").then_some(pending)
}

/// How a parsed mouse event should be routed while in control mode.
#[derive(Debug, PartialEq, Eq)]
enum MouseAction {
    /// wheel: send as a semantic terminal.scroll (server decides app vs scrollback)
    Scroll { up: bool },
    /// click/drag on a remote TUI: forward the raw SGR sequence
    ForwardRaw,
    /// left press/drag/release: hand it to the plugin selector, which defers
    /// the press and (on a TUI) replays it if the gesture was a click rather
    /// than a drag (see src/select.rs)
    Select,
    /// unclassified, or a non-left button at a shell: drop so SGR never
    /// reaches a prompt that never enabled mouse reporting
    Drop,
}

/// Which physical button an SGR code names, with the modifier and motion flags
/// removed: 0/1/2 = left/middle/right, 4/5 = wheel up/down, 6/7 = wheel
/// left/right, 8.. = extra buttons.
///
/// The button number is *not* contiguous in the wire encoding: it is the low two
/// bits plus bit 6 (64) for the wheel set and bit 7 (128) for buttons 8-11,
/// while bits 2-4 carry shift/alt/ctrl and bit 5 carries motion. Masking only
/// the low two bits reads shift+wheel-up (68) as a left press, which is how the
/// wheel ends up driving the selection.
fn button_number(btn: u32) -> u32 {
    (btn & 0b11) + if btn & 64 != 0 { 4 } else { 0 } + if btn & 128 != 0 { 8 } else { 0 }
}

/// Wheel always scrolls semantically, regardless of the foreground
/// classification — the remote herdr server knows the real app's mouse mode
/// and is a better judge than this side's process-name heuristic (e.g. a TUI
/// that doesn't consume wheel events, like an agent CLI). Non-wheel
/// clicks/drags keep the existing foreground-based routing.
/// The left button goes to the plugin selector in every classified foreground
/// (TUI, agent, or shell), because a drag is the gesture with no substitute:
/// an app's click can be replayed after the fact, a selection cannot be
/// recovered. The grab is always held, so these events actually arrive — even
/// at a shell, where releasing it used to hand selection to herdr and starve
/// the wheel (#75).
///
/// A TUI gesture that never leaves its cell is replayed to the app as a real
/// click, so htop still sorts on a header click and lazygit still stages on a
/// file click. Agent CLIs get it too: they never enabled mouse reporting, but
/// claude and codex both discard the bytes cleanly, so withholding it only
/// stood to swallow clicks the day one of them grows mouse support. A shell
/// click is not replayed: the prompt never enabled mouse reporting, and the
/// bytes would dump into it.
///
/// The cost is an in-app *drag*: vim's mouse visual-select, a resize handle.
fn mouse_action(fg: Option<Fg>, btn: u32, press: bool) -> MouseAction {
    match button_number(btn) {
        // wheel up/down. Matched on the button number, not on `btn == 64`, so a
        // modified scroll still scrolls instead of falling through as a click.
        b @ (4 | 5) if press => MouseAction::Scroll { up: b == 4 },
        6 | 7 => MouseAction::Drop,
        0 if matches!(fg, Some(Fg::Agent) | Some(Fg::Mouse) | Some(Fg::Shell)) => {
            MouseAction::Select
        }
        // middle, right, wheel release and the extra buttons — TUI/agent only.
        // A shell never asked for these; forwarding them garbage the prompt.
        _ if matches!(fg, Some(Fg::Agent) | Some(Fg::Mouse)) => MouseAction::ForwardRaw,
        _ => MouseAction::Drop,
    }
}

fn contains_wheel_press(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i < bytes.len() {
        if let Some((btn, _, _, press, len)) = parse_mouse(bytes, i) {
            if press && (btn == 64 || btn == 65) {
                return true;
            }
            i += len;
        } else {
            i += 1;
        }
    }
    false
}

/// Cap on a partially-received bracketed paste. Past this we stop waiting for
/// a terminator and flush what we have: a huge paste is still forwarded, it
/// just loses the drop treatment rather than buffering without bound.
const MAX_PASTE_BYTES: usize = 1024 * 1024;

const PASTE_START: &[u8] = b"\x1b[200~";
const PASTE_END: &[u8] = b"\x1b[201~";

/// Input held back while an upload is in flight (see `route_input`).
enum Queued {
    Raw(Vec<u8>),
    Body(Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub(crate) enum PasteSplit {
    /// a paste has begun but not finished; nothing to do yet
    Pending,
    /// no paste involved: forward as-is
    Passthrough(Vec<u8>),
    Complete { before: Vec<u8>, body: Vec<u8>, after: Vec<u8> },
}

fn find_seq(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Longest suffix of `hay` that is a *proper* prefix of `needle`, so a marker
/// straddling two reads is held rather than forwarded in pieces.
fn trailing_partial(hay: &[u8], needle: &[u8]) -> usize {
    (1..needle.len().min(hay.len()) + 1)
        .rev()
        .find(|&n| n < needle.len() && hay[hay.len() - n..] == needle[..n])
        .unwrap_or(0)
}

/// Pull the next complete bracketed paste out of `buf` + `chunk`.
///
/// Pure so the framing can be table-tested without a pane: every interesting
/// case (split reads, a START with no END, bytes either side, several pastes
/// in one read, the oversize flush) lives here rather than in the event loop.
pub(crate) fn split_paste(buf: &mut Vec<u8>, chunk: Vec<u8>) -> PasteSplit {
    // fast path for ordinary typing: nothing buffered and no marker starting
    if buf.is_empty()
        && find_seq(&chunk, PASTE_START).is_none()
        && trailing_partial(&chunk, PASTE_START) == 0
    {
        return PasteSplit::Passthrough(chunk);
    }
    buf.extend_from_slice(&chunk);

    // a paste that never terminates must not buffer without bound
    if buf.len() > MAX_PASTE_BYTES {
        return PasteSplit::Passthrough(std::mem::take(buf));
    }

    let Some(start) = find_seq(buf, PASTE_START) else {
        // hold only a possible partial marker; release everything before it
        let keep = trailing_partial(buf, PASTE_START);
        let cut = buf.len() - keep;
        if cut == 0 {
            return PasteSplit::Pending;
        }
        let tail = buf.split_off(cut);
        let head = std::mem::replace(buf, tail);
        return PasteSplit::Passthrough(head);
    };
    let Some(end) = find_seq(&buf[start..], PASTE_END).map(|i| start + i) else {
        return PasteSplit::Pending;
    };

    let all = std::mem::take(buf);
    PasteSplit::Complete {
        before: all[..start].to_vec(),
        body: all[start + PASTE_START.len()..end].to_vec(),
        after: all[end + PASTE_END.len()..].to_vec(),
    }
}

fn has_mouse_seq(bytes: &[u8]) -> bool {
    bytes.windows(3).any(|w| w == [0x1b, b'[', b'<'])
}


// ---------------------------------------------------------------------------
// the wrapper state machine

const BACKOFF: [u64; 4] = [1000, 2000, 5000, 10000];

/// Rung used once the remote pane is known to be gone. Deliberately still a
/// retry and not a stop: the daemon owns mirror lifecycle and reaps a pane
/// whose remote has been absent for two converge polls, so the streamer's job
/// here is to wait quietly for that rather than to decide it is finished. It
/// also keeps "gone" recoverable, which it sometimes is — a remote herdr
/// restarting renumbers pane ids while session restore runs.
const GONE_BACKOFF_MS: u64 = 60_000;

/// herdr's refusal when something else already holds the terminal
/// ("terminal … already has an attached client; retry").
const ATTACH_CONFLICT: &str = "already has an attached client";

/// How many unconfirmed remote clients to carry. One per failed cleanup; a
/// transport that has been down long enough to make eight of them will not be
/// helped by a ninth, and the record on disk still names the last one.
const MAX_PENDING_REMOTE_KILLS: usize = 8;

/// Which of our own orphaned clients to clear before retrying a refused
/// control attach — `None` for "handle this like any other failure".
///
/// Deliberately narrow. Only a client THIS streamer created is ever a
/// candidate: another Herdr client attached to the same remote pane is a
/// person, and the answer to a person holding the terminal is to stay in
/// observe, not to evict them. One attempt only, so a genuine second client
/// cannot be fought over in a loop.
fn attach_conflict_pid(reason: &str, ours: Option<i32>, already_retried: bool) -> Option<i32> {
    if already_retried || !reason.to_ascii_lowercase().contains(ATTACH_CONFLICT) {
        return None;
    }
    ours.filter(|pid| *pid > 1)
}

const SWITCH_GAP: Duration = Duration::from_millis(200);
const QUICK_CONTROL_FAILURE: Duration = Duration::from_secs(4);

/// Is this failure "the pane we stream is gone", as opposed to any other
/// failure that happens to mention something missing?
///
/// Matches herdr's whole sentence (`headless.rs`: "terminal target {t} not
/// found") with OUR target in it, rather than loose substrings. That keeps it
/// off the remote-bin resolver's `exec: …/herdr: not found`, which means herdr
/// is absent on that host — a different problem with a different fix — and
/// stops a target being confused with one that merely shares its prefix
/// (`w1:p1` vs `w1:p10`, both real ids in herdr's base-32 alphabet).
///
/// Note the pane dying *underneath* a live stream reports differently
/// ("terminal attach ended: terminal {term_id} not found", carrying herdr's
/// internal terminal id, not ours). That deliberately does not match: the next
/// attempt asks for the pane target and gets the canonical sentence a second
/// later, so this fires one cycle behind rather than being loosened.
fn target_gone(reason: &str, pane_target: &str) -> bool {
    if pane_target.is_empty() {
        return false;
    }
    reason
        .to_ascii_lowercase()
        .contains(&format!("terminal target {} not found", pane_target.to_ascii_lowercase()))
}

/// Delay before the next attempt, and the ladder position to keep.
///
/// Pure so the rung and the ladder-resume are testable without a live pane.
/// A gone target does NOT consume a rung: if the pane comes back and later
/// fails transiently, that failure should start where the fast ladder left
/// off rather than at the top.
fn reconnect_delay(gone: bool, idx: usize) -> (u64, usize) {
    if gone {
        return (GONE_BACKOFF_MS, idx);
    }
    (BACKOFF[idx.min(BACKOFF.len() - 1)], idx + 1)
}

struct App {
    args: Args,
    /// where the remote-client record for this mirror pane lives
    state_dir: std::path::PathBuf,
    tty: bool,
    grid: Grid,
    renderer: Renderer,
    tx: mpsc::Sender<Msg>,

    mode: Mode,
    /// in-flight mode switch (guards fast re-entry)
    switching_to: Option<Mode>,
    switch_at: Option<Instant>,
    session: Option<Session>,
    next_gen: u64,

    backoff_idx: usize,
    reconnect_at: Option<(Instant, Mode)>,
    /// remote attach clients we started and have not confirmed dead. Cleared
    /// before every attach, so a retrying streamer replaces its own client
    /// instead of stacking a new one beside it every rung of the ladder.
    pending_remote_kills: Vec<i32>,
    /// one-shot recovery from a control attach our own orphan is holding
    attach_conflict_retried: bool,
    /// consecutive quick control failures → fall back to observe
    control_failures: u32,
    control_sticky: bool,
    pending_input: Vec<Vec<u8>>,
    last_input: Instant,
    hint_clear_at: Option<Instant>,
    /// predictive local echo — draws keystrokes optimistically, frame-verified
    predict: Predictor,
    /// remote pane foreground classification, None=unknown (fail safe to local).
    /// Refreshed lazily on mouse activity; see `foreground::classify`.
    remote_fg: Option<Fg>,
    /// local drag-selection, driven by the left button the remote app would
    /// otherwise receive
    select: Select,
    /// screen rows the selection overlay covered on the last paint, so they can
    /// be repainted when it moves away
    last_select_rows: Option<(usize, usize)>,
    /// last time a foreground poll was kicked off (throttles the polls)
    fg_poll_at: Option<Instant>,
    /// at most one metadata poll per pane is ever in the air; see `PollGate`
    fg_poll_gate: crate::foreground::PollGate,
    /// this pane's one metadata channel, shared with the tasks that poll on it.
    /// Unused by a pane whose host has an API forward: see `foreground::poll`.
    fg_poller: crate::foreground::Shared,
    /// when a frame last reached us
    last_frame_at: Instant,
    /// the remote pane's content revision as of the last successful poll, and
    /// when it was last seen to INCREASE. The increase is the fact that
    /// matters: it is the only cheap proof that the remote produced output
    /// this stream was supposed to carry.
    remote_revision: Option<u64>,
    remote_advanced_at: Option<Instant>,
    /// next output-health tick (see `OUTPUT_HEALTH_INTERVAL`)
    health_at: Option<Instant>,
    /// scheduled delayed re-poll to catch a foreground change the last input just
    /// caused (e.g. quitting a TUI back to a shell); bypasses the throttle
    settle_at: Option<Instant>,
    /// whether the local mouse grab (?1002h) is currently on. Always held so
    /// wheel events reach us as terminal.scroll even at a shell; the pane is
    /// on the alt screen and has no local scrollback, so a released grab
    /// cannot scroll (#75).
    mouse_grabbed: bool,
    /// whether the local pane is currently in application cursor mode (?1h), held
    /// to match the remote's so forwarded arrows arrive in the form it expects
    app_cursor_keys: bool,
    paste_inflight: bool,
    /// partially-received bracketed paste (see `intercept_paste`)
    paste_buf: Vec<u8>,
    /// partially-received SGR mouse sequence
    mouse_buf: Vec<u8>,
    mouse_flush_at: Option<Instant>,
    /// input held back while an upload is in flight, flushed in order after
    paste_queue: Vec<Queued>,
    /// the payload that started the in-flight upload, so it can be forwarded
    /// unchanged when every path turns out to exist on the remote already
    paste_original: Option<Vec<u8>>,
    /// this process is a `herdr-mirror view` in a tile, not a sidebar copy
    is_view: bool,
    /// view only: the remote pane is gone, so the view leaves this streamer to
    /// re-resolve its agent instead of retrying a dead pane id
    view_target_gone: bool,
    /// host key for this streamer's remote-client record (a view keeps its
    /// own, see `view::view_record_target`)
    record_target: String,
    /// the local pane this streamer draws, when it knows it
    local_pane_id: Option<String>,
    /// sidebar copy only: the live view claim it is standing aside for
    claim: Option<crate::view::ViewClaim>,
    /// sidebar copy only: where its input goes while claimed
    forward: crate::view::Forwarder,
    /// sidebar copy only: stream released because nobody views the pane
    /// (idle release, see `visibility.rs`)
    idle_released: bool,
    /// sidebar copy only: last moment the pane was on screen or got input
    last_seen: Instant,
    /// sidebar copy only: the daemon's published view of what is on screen
    visibility: crate::visibility::Reader,
}

/// minimum spacing between foreground polls — each is an ssh handshake, so we
/// poll lazily (only around mouse activity) and no faster than this
const FG_POLL_INTERVAL: Duration = Duration::from_millis(1500);

/// Hard floor between foreground polls on a host whose pane metadata rides the
/// daemon's API forward. Forced polls included — every caller, no exceptions.
///
/// The Herdr API answers one request per connection, so a poll there is at
/// least one fresh connection, and on a sandbox guest each connection is one
/// direct-tcpip channel and one guest `exec` — against the very exec service
/// these guests wedged on twice in a day. The cost is therefore the poll RATE,
/// and the rate has to be bounded by something that cannot be argued around.
///
/// The number comes from the measurement: before the rolled-back `.26`, an
/// idle four-pane Studio guest showed 26 forwards/min, which is each pane
/// polled on its 20 s output-health tick at two connections a poll — 6 per
/// minute per pane. A poll now costs one connection, so a 10 s floor caps a
/// pane at 6 per minute whatever asks for a poll, forced or not, busy or idle.
/// That makes the pre-`.26` idle cost a ceiling instead of an average; at rest
/// the health tick remains the only caller and the real figure is 3 per minute.
///
/// What it costs. On these hosts a shell→TUI change can take up to 10 s to
/// reach the local pane's mouse and cursor-key handling, where elsewhere it is
/// immediate. Cheap where it applies: a host is on this transport because it is
/// a sandbox guest being mirrored for its agent pane, and an agent pane is
/// classified from `pane.get`'s own `agent` field, which does not move.
const API_FG_POLL_FLOOR: Duration = Duration::from_secs(10);

/// How long after the last poll the next one may run.
///
/// Free-standing so the policy can be asserted without an `App`: it is the
/// whole rate limit on the API forward, and the number that decides how much
/// `exec` load an idle mirrored guest carries.
fn fg_poll_spacing(force: bool, over_api_forward: bool) -> Duration {
    // One rule on the API forward, and it does not care who is asking: a
    // ceiling that a forced caller could step over would not be a ceiling.
    if over_api_forward {
        return API_FG_POLL_FLOOR;
    }
    if force {
        Duration::ZERO
    } else {
        FG_POLL_INTERVAL
    }
}

/// after input settles, re-poll once this much later to catch a foreground
/// change the input caused (e.g. a TUI just exited); bypasses FG_POLL_INTERVAL
const SETTLE_DELAY: Duration = Duration::from_millis(350);

/// How often a streamer asks the remote whether it produced output, and
/// publishes what it knows about its own output direction.
///
/// The existing foreground poll cannot answer this: it fires around input and
/// mode changes, and the pane this exists for is precisely the one nobody is
/// typing into any more. One extra multiplexed ssh exec every 20 seconds per
/// mirrored pane is the whole cost, on the ControlMaster the daemon already
/// holds, and it is bounded by the sweep it feeds: reporting faster than the
/// daemon can act would buy nothing.
const OUTPUT_HEALTH_INTERVAL: Duration = Duration::from_secs(20);

/// Explicit Mirror pause is a traffic boundary, not only a daemon switch.
/// Stream children notice it at once (they wait on the state directory, see
/// `crate::watch`), leave through their normal attach-client cleanup, and let
/// the stable supervisor keep the local pane alive.
const HOST_HEALTH_WAIT: Duration = Duration::from_secs(1);

/// Spread an explicit all-pane resume over eight seconds. This is deliberately
/// a stable local calculation rather than a scheduler: every supervisor can
/// make the same decision after the shared pause marker disappears, and no
/// control-plane service has to admit the resulting connections.
const STREAM_RESUME_SLOTS: u64 = 80;
const STREAM_RESUME_SLOT: Duration = Duration::from_millis(100);

fn stream_may_connect(state_dir: &std::path::Path) -> bool {
    !crate::daemon::streams_paused(state_dir)
}

/// Pane processes are consumers of daemon health, never additional health
/// trial owners.  While the daemon publishes a pending retry, every streamer
/// waits on local state instead of adding session, cleanup or metadata work to
/// the failed host.  Missing health remains permissive for standalone pane
/// mode and the instant before a daemon's first successful sync.
fn host_admission_open(
    state_dir: &std::path::Path,
    host_name: Option<&str>,
) -> bool {
    host_name
        .and_then(|host| crate::state::read_host_health(state_dir, host))
        .is_none_or(|health| health.next_retry_unix.is_none())
}

fn stream_resume_delay(local_pane_id: &str) -> Duration {
    // FNV-1a: deterministic across processes and releases, unlike DefaultHasher.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in local_pane_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    STREAM_RESUME_SLOT * (hash % STREAM_RESUME_SLOTS) as u32
}

fn stream_pause_claim_path(state_dir: &std::path::Path, local_pane_id: &str) -> std::path::PathBuf {
    state_dir
        .join("stream-paused")
        .join(format!("{}.paused", crate::util::sane_component(local_pane_id)))
}

fn mark_stream_pause(state_dir: &std::path::Path, local_pane_id: &str) {
    let path = stream_pause_claim_path(state_dir, local_pane_id);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(path, b"paused\n");
}

fn take_stream_pause(state_dir: &std::path::Path, local_pane_id: &str) -> bool {
    let path = stream_pause_claim_path(state_dir, local_pane_id);
    let existed = path.exists();
    let _ = std::fs::remove_file(path);
    existed
}

async fn wait_for_stream_resume_with_delay(
    state_dir: &std::path::Path,
    pause_observed: bool,
    delay: Duration,
) -> bool {
    if !pause_observed && stream_may_connect(state_dir) {
        return false;
    }
    // The marker lives in the state directory itself: wait for that directory
    // to change rather than stat the marker ten times a second (bug 85).
    let watch = crate::watch::DirWatch::new(&[state_dir]);
    while !stream_may_connect(state_dir) {
        crate::watch::changed_or_tick(watch.as_ref(), crate::watch::FALLBACK_TICK).await;
    }
    tokio::time::sleep(delay).await;
    true
}

/// A change in the watched state directories, or the slow safety tick.
async fn state_changed(watch: Option<&crate::watch::DirWatch>, tick: &mut tokio::time::Interval) {
    match watch {
        Some(w) => {
            tokio::select! {
                _ = w.changed() => {}
                _ = tick.tick() => {}
            }
        }
        None => {
            tick.tick().await;
        }
    }
}

async fn wait_for_stream_resume(state_dir: &std::path::Path, local_pane_id: &str) -> bool {
    wait_for_stream_resume_with_delay(
        state_dir,
        take_stream_pause(state_dir, local_pane_id),
        stream_resume_delay(local_pane_id),
    )
    .await
}

impl App {
    fn paint(&mut self) {
        // a sidebar copy standing aside shows only its notice
        if !self.tty || self.aside() {
            return;
        }
        let (cols, rows) = term_size();
        // Overlays paint outside the renderer's per-row cache, so the rows they
        // covered must be repainted or the old drawing survives underneath.
        // Predictions are scattered, so they take the whole pane; a selection is
        // a contiguous band, and during a drag it moves on every mouse motion,
        // so it repaints only the rows it just left and the ones it now covers.
        let reserved = self.renderer.status_rows();
        if self.predict.take_dirty() {
            self.renderer.invalidate();
        }
        if self.select.take_dirty() {
            let now = self.select.painted_rows(&self.grid, rows, reserved);
            for (top, bottom) in self.last_select_rows.into_iter().chain(now) {
                for r in top..=bottom {
                    self.renderer.invalidate_row(r);
                }
            }
        }
        self.last_select_rows = self.select.painted_rows(&self.grid, rows, reserved);
        let mut out = self.renderer.paint(&self.grid, cols, rows);
        // inject the overlays inside the synchronized-update block. Selection
        // goes last: it is the one the user is actively pointing at.
        let mut overlay = self.predict.overlay(&self.grid, cols, rows);
        let sel = self.select.overlay(&self.grid, cols, rows, reserved);
        overlay.push_str(&sel);
        if !overlay.is_empty() {
            // `paint` parks the cursor last; the overlays land after that, so
            // re-park or the cursor is left wherever the highlight ended and is
            // re-asserted there on every frame
            if !sel.is_empty() {
                overlay.push_str(&self.renderer.cursor_park(&self.grid, cols, rows));
            }
            const SYNC_END: &str = "\x1b[?2026l";
            if let Some(pos) = out.rfind(SYNC_END) {
                out.insert_str(pos, &overlay);
            } else {
                out.push_str(&overlay);
            }
        }
        write_stdout(&out);
    }

    fn hint(&mut self, text: &str) {
        self.hint_for(text, Duration::from_millis(1500));
    }

    /// A hint that stays up longer than the usual flash, for one the user did
    /// not cause by typing and so is not already watching for.
    fn hint_for(&mut self, text: &str, ttl: Duration) {
        self.renderer.status(text);
        self.paint();
        self.hint_clear_at = Some(Instant::now() + ttl);
    }

    /// A hint with no expiry, for work whose duration we don't know: an upload
    /// can outlast the usual 1.5s and the pane would otherwise look idle while
    /// it is busy. Whoever set it replaces it when the work resolves.
    fn hint_sticky(&mut self, text: &str) {
        self.renderer.status(text);
        self.paint();
        self.hint_clear_at = None;
    }

    /// How long after the last poll this one may run. Zero for a forced poll on
    /// a transport where an extra one is cheap, `API_FG_POLL_FLOOR` for a
    /// forced poll on the API forward, `FG_POLL_INTERVAL` otherwise.
    ///
    /// This is the whole rate limit, and it is deliberately the only one: with
    /// the health tick forcing a poll every `OUTPUT_HEALTH_INTERVAL`, an idle
    /// pane on the API forward costs one poll per 20 s whatever this returns,
    /// and the floor is what stops an input burst turning that into one poll
    /// per settle.
    fn fg_poll_spacing(&self, force: bool) -> Duration {
        fg_poll_spacing(force, self.args.api_socket.is_some())
    }

    /// Kick a background poll of the remote pane's foreground process, throttled
    /// so a mouse burst doesn't spawn an ssh per event. The result arrives as
    /// Msg::Foreground and updates `remote_is_shell`.
    fn spawn_foreground_poll(&mut self, force: bool) {
        if !host_admission_open(&self.state_dir, self.args.host_name.as_deref()) {
            return;
        }
        let now = Instant::now();
        if self.fg_poll_at.is_some_and(|t| now.duration_since(t) < self.fg_poll_spacing(force)) {
            return;
        }
        self.fg_poll_at = Some(now);
        // A forced poll bypasses the throttle above, so the throttle cannot be
        // what bounds concurrency. This can: one in flight per pane, and a
        // request arriving while one runs is coalesced into a single extra
        // pass rather than starting or queueing another.
        let Some(permit) = self.fg_poll_gate.begin() else { return };
        let tx = self.tx.clone();
        let bin = self.args.remote_bin.clone();
        let session = self.args.session.clone();
        let pane = self.args.pane_target.clone();
        let api_socket = self.args.api_socket.clone();
        let poller = self.fg_poller.clone();
        tokio::spawn(async move {
            // dropped on completion or cancellation, so the gate never stays
            // shut on a poll that will not finish
            let permit = permit;
            loop {
                let (fg, revision) = crate::foreground::poll(
                    &poller,
                    bin.as_deref(),
                    session.as_deref(),
                    &pane,
                    api_socket.as_deref(),
                )
                .await;
                let _ = tx.send(Msg::Foreground(fg)).await;
                let _ = tx.send(Msg::RemoteRevision(revision)).await;
                if !permit.another_pass_wanted() {
                    break;
                }
            }
        });
    }

    /// Record what a poll said about the remote pane's content revision.
    ///
    /// Only an INCREASE is stamped. A poll that failed, or one that saw the
    /// same revision, says nothing new about whether the remote produced
    /// anything, and stamping it would let an idle pane look like a stall the
    /// moment its first frame is older than the grace.
    fn note_remote_revision(&mut self, revision: Option<u64>) {
        let Some(revision) = revision else { return };
        if self.remote_revision.is_some_and(|seen| revision > seen) {
            self.remote_advanced_at = Some(Instant::now());
        }
        self.remote_revision = Some(revision);
    }

    /// Publish this streamer's view of its own output direction for the
    /// daemon's sweep. Wall-clock, because the reader is another process.
    fn publish_stream_health(&self, local_pane_id: Option<&str>) {
        let Some(id) = local_pane_id else { return };
        if self.args.dump || self.is_view || self.aside() {
            return;
        }
        let now_instant = Instant::now();
        let now = crate::state::unix_now();
        let to_unix = |t: Instant| now - now_instant.saturating_duration_since(t).as_secs_f64();
        crate::state::publish_stream_health(
            &self.state_dir,
            id,
            &crate::state::StreamHealth {
                last_frame_unix: to_unix(self.last_frame_at),
                remote_advanced_unix: self.remote_advanced_at.map(to_unix),
            },
        );
    }

    /// Hold the local mouse grab for the streamer's whole lifetime. The pane
    /// is always on the alt screen (no scrollback), so herdr's released-grab
    /// wheel routing has nothing to move; ?1007l already prevents the
    /// arrow-key hijacking that motivated the original shell-side release
    /// (#69). Text selection stays in the plugin selector.
    fn sync_mouse_grab(&mut self) {
        if !self.tty {
            return;
        }
        if self.mouse_grabbed {
            return;
        }
        self.mouse_grabbed = true;
        write_stdout("\x1b[?1002h\x1b[?1006h");
    }

    /// Match the local pane's cursor-key mode to the remote's, so the arrow bytes
    /// herdr hands us are already the ones the remote app expects.
    ///
    /// Frames carry no DEC modes (see grid.rs), so a remote app in application
    /// cursor mode (DECCKM, what terminfo `smkx` sets) never moves the local
    /// pane out of normal mode: herdr encodes Up as CSI A, we forward it
    /// verbatim, and the remote app is listening for SS3 A. Rather than rewrite
    /// the bytes in flight, put the LOCAL pane in the same mode and let herdr's
    /// own encoder produce the right form: it also covers Home/End and anything
    /// else whose encoding turns on this mode.
    ///
    /// The classification is the same shell/TUI proxy the mouse grab uses, for
    /// the same reason: the API exposes no input modes to ask for directly.
    fn sync_cursor_key_mode(&mut self) {
        if !self.tty {
            return;
        }
        // a shell prompt reads arrows in normal mode; a TUI is the case that
        // sets smkx, so mirror application mode unless we've confirmed a shell
        let want = matches!(self.remote_fg, Some(Fg::Agent) | Some(Fg::Mouse));
        if want == self.app_cursor_keys {
            return;
        }
        self.app_cursor_keys = want;
        write_stdout(if want { "\x1b[?1h" } else { "\x1b[?1l" });
    }

    fn observe_size(&self) -> (usize, usize) {
        observe_size_for(&self.args, if self.tty { term_size() } else { (0, 0) })
    }

    /// Size to enter (and stay in) control at. Control is authoritative on the
    /// remote — the server resizes the remote pty to whatever we ask for — so a
    /// host whose remote has its own display caps this and renders the remote at
    /// its own geometry, leaving the rest of the local pane blank rather than
    /// reflowing a screen someone over there is reading. Uncapped by default,
    /// which is the pre-existing fill-the-pane behaviour.
    fn control_size(&self) -> (usize, usize) {
        cap_size(term_size(), self.args.max_cols, self.args.max_rows)
    }

    /// Stop the child (clean release first for control) — never leave an
    /// orphan holding the remote attach lock.
    /// Remember a remote client we started so the next attach clears it.
    /// Capped and de-duplicated: this list is retried, not accumulated.
    fn queue_remote_kill(&mut self, pid: Option<i32>) {
        let Some(pid) = pid.filter(|p| *p > 1) else { return };
        if self.pending_remote_kills.contains(&pid) {
            return;
        }
        if self.pending_remote_kills.len() >= MAX_PENDING_REMOTE_KILLS {
            self.pending_remote_kills.remove(0);
        }
        self.pending_remote_kills.push(pid);
    }

    /// Take the current session, queueing the remote client it owns. Every
    /// path that drops a session goes through here, which is what makes "the
    /// streamer owns its remote client" true rather than aspirational.
    fn retire_session(&mut self) -> Option<Session> {
        let session = self.session.take()?;
        self.queue_remote_kill(session.remote_pid);
        Some(session)
    }

    /// Terminate the remote clients we have queued, oldest first, under one
    /// deadline for the whole batch. Anything not confirmed dead stays queued
    /// for the next attempt rather than being dropped or retried forever here.
    async fn reap_remote_clients(&mut self) {
        if self.pending_remote_kills.is_empty() {
            return;
        }
        let deadline = Instant::now() + REMOTE_KILL_TIMEOUT;
        let mut unresolved: Vec<i32> = Vec::new();
        for (i, pid) in std::mem::take(&mut self.pending_remote_kills).into_iter().enumerate() {
            // the first one always gets its try; later ones only while there
            // is time left, so a dead transport costs one timeout, not eight
            if i > 0 && Instant::now() >= deadline {
                unresolved.push(pid);
                continue;
            }
            if !kill_remote_client(&self.args, pid).await {
                unresolved.push(pid);
            }
        }
        if unresolved.is_empty() {
            crate::util::clear_remote_client(
                &self.state_dir,
                &self.record_target,
                &self.args.pane_target,
            );
        }
        self.pending_remote_kills = unresolved;
    }

    fn stop_session(&mut self) {
        if let Some(mut s) = self.retire_session() {
            tokio::spawn(async move {
                if s.mode == Mode::Control {
                    let _ = s.stdin.write_all(b"{\"type\":\"terminal.release\"}\n").await;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
                kill_session_process_group(s.process_group);
            });
        }
    }

    async fn connect(&mut self, m: Mode) {
        self.mode = m;
        if self.aside() {
            // standing aside (for a view, or idle): no stream now
            return;
        }
        // re-earn prediction confidence against the new session's frames
        self.predict = Predictor::new();
        // the new session repaints from scratch, so a span from the old one
        // points at text that no longer exists
        self.select.clear();
        if !host_admission_open(&self.state_dir, self.args.host_name.as_deref()) {
            self.renderer.status("waiting for Mirror host health trial");
            self.paint();
            self.reconnect_at = Some((Instant::now() + HOST_HEALTH_WAIT, m));
            return;
        }
        let (cols, rows) = match m {
            Mode::Observe => self.observe_size(),
            Mode::Control => self.control_size(),
        };
        if let Some(s) = self.retire_session() {
            kill_session_process_group(s.process_group);
        }
        // Before attaching, not after: the client we are replacing still holds
        // the remote terminal, and a control attach beside it is refused
        // outright ("already has an attached client"). This is also what stops
        // a reconnect ladder against a half-broken transport from leaving one
        // more remote client per rung.
        self.reap_remote_clients().await;
        self.next_gen += 1;
        match spawn_session(&self.args, m, cols, rows, self.next_gen, self.tx.clone()) {
            Ok(mut s) => {
                if m == Mode::Control {
                    self.last_input = Instant::now();
                    // keystrokes typed while the control session was spinning up
                    for buf in std::mem::take(&mut self.pending_input) {
                        let _ = write_terminal_input(&mut s.stdin, &buf).await;
                    }
                } else {
                    self.pending_input.clear();
                }
                self.session = Some(s);
                // warm the foreground classification before the user mouses
                self.spawn_foreground_poll(false);
                // always-control has no release, so no "ctrl+\ to release" hint
                self.renderer.status(
                    if m == Mode::Control && !self.args.always_control {
                        "CONTROL — ctrl+\\ to release"
                    } else {
                        ""
                    },
                );
            }
            Err(e) => self.schedule_reconnect(m, &e.to_string()),
        }
    }

    /// A view whose remote pane is gone stops here: `cmd_view` re-resolves its
    /// agent (the remote layout may have moved it to another pane) rather than
    /// retrying a pane id that no longer exists. Any mode: a view usually
    /// runs in control, where a sidebar copy would keep its fast retries.
    fn leave_gone_view(&mut self, reason: &str) -> bool {
        if !self.is_view || !target_gone(reason, &self.args.pane_target) {
            return false;
        }
        self.view_target_gone = true;
        self.reconnect_at = None;
        true
    }

    fn schedule_reconnect(&mut self, m: Mode, reason: &str) {
        if self.leave_gone_view(reason) {
            return;
        }
        // Only slow down once we are back in observe. In control the existing
        // quick-failure fallback needs its fast retries to reach two failures
        // and drop the pane to observe within seconds; a 60s rung there would
        // leave an always_control pane stuck in control for a minute.
        let gone = m == Mode::Observe && target_gone(reason, &self.args.pane_target);
        let (delay, idx) = reconnect_delay(gone, self.backoff_idx);
        self.backoff_idx = idx;

        if gone {
            // Repainted every cycle on purpose: handle_frame paints herdr's
            // raw close reason before us on each attempt, so saying this once
            // would leave the misleading "terminal closed" line on screen from
            // the second cycle onward. The renderer diffs rows, so an
            // unchanged line costs one row write a minute.
            self.renderer.status(&format!("remote pane {} is gone", self.args.pane_target));
            // and nothing may expire it out from under us: the control→observe
            // fallback sets a 1.5s hint just before this path runs
            self.hint_clear_at = None;
        } else {
            let suffix = if reason.is_empty() { String::new() } else { format!(" — {reason}") };
            self.renderer
                .status(&format!("reconnecting in {}s ({}){suffix}", delay / 1000, m.as_str()));
        }
        self.paint();
        self.reconnect_at = Some((Instant::now() + Duration::from_millis(delay), m));
    }


    fn switch_mode(&mut self, m: Mode) {
        // already settled or scheduled — don't restart. Without this guard,
        // fast typing during the 200ms connect gap would spawn one control
        // ssh per keystroke, all racing to attach the same terminal.
        if self.switching_to == Some(m) || (self.switching_to.is_none() && self.mode == m) {
            return;
        }
        self.reconnect_at = None;
        self.switching_to = Some(m);
        self.stop_session();
        // covers every route into a mode change that returns before the mouse
        // loop: ctrl+\, the idle release, and taking control from Observe
        self.select.clear();
        self.renderer.invalidate();
        // immediate feedback for the mode-switch gap (stop + 200ms + reconnect)
        self.renderer.status(if m == Mode::Control { "taking control…" } else { "releasing…" });
        self.paint();
        self.switch_at = Some(Instant::now() + SWITCH_GAP);
    }

    fn handle_frame(&mut self, gen: u64, frame: Frame) {
        if self.session.as_ref().map(|s| s.gen) != Some(gen) {
            return; // stale frame from a replaced session
        }
        if frame.kind == "terminal.closed" {
            let suffix = frame.reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default();
            self.renderer.status(&format!("remote terminal closed{suffix}"));
            self.paint();
            return;
        }
        if frame.kind != "terminal.frame" {
            return;
        }
        let Some(bytes) = &frame.bytes else { return };
        // The output direction is alive exactly when this line runs.
        self.last_frame_at = Instant::now();
        self.backoff_idx = 0;
        // frames are flowing, so whatever was holding the terminal is gone
        self.attach_conflict_retried = false;
        self.renderer.status("");
        let (fw, fh) = (
            frame.width.unwrap_or(self.grid.width),
            frame.height.unwrap_or(self.grid.height),
        );
        // A reflow or a full redraw replaces every cell, so a selection anchored
        // to grid coordinates would survive pointing at different text — and a
        // release afterwards would copy whatever landed there, or nothing, while
        // still showing a highlight that implies it worked.
        if (fw, fh) != (self.grid.width, self.grid.height) || frame.full == Some(true) {
            self.select.clear();
        }
        self.grid.resize(fw, fh);
        if frame.full == Some(true) {
            self.grid.clear();
        }
        if let Ok(decoded) = B64.decode(bytes) {
            self.grid.apply(&String::from_utf8_lossy(&decoded));
            // reconcile predictive echo against the authoritative frame
            self.predict.on_frame(&self.grid);
        }
        if self.args.dump {
            let lines: Vec<String> = self.grid.text_lines().into_iter().filter(|l| !l.is_empty()).collect();
            println!(
                "--- frame seq={:?} full={:?} {}x{} ---\n{}",
                frame.seq,
                frame.full,
                frame.width.unwrap_or(0),
                frame.height.unwrap_or(0),
                lines.join("\n")
            );
        } else {
            self.paint();
        }
    }

    /// Adopt the pid the remote wrapper announced for this session, and publish
    /// it so a streamer that replaces us can clean up if we are killed before
    /// we can. A pid from a session we already replaced is still ours — queue
    /// it for the kill rather than discarding it.
    fn handle_remote_pid(&mut self, gen: u64, pid: i32) {
        match self.session.as_mut() {
            Some(session) if session.gen == gen => {
                session.remote_pid = Some(pid);
                crate::util::record_remote_client(
                    &self.state_dir,
                    &self.record_target,
                    &self.args.pane_target,
                    pid,
                );
            }
            _ => self.queue_remote_kill(Some(pid)),
        }
    }

    fn handle_exit(&mut self, gen: u64, exited_mode: Mode, reason: String, uptime: Duration) {
        if self.session.as_ref().map(|s| s.gen) != Some(gen) {
            return; // an old child we already replaced/killed
        }
        // The transport died; the remote client did not. It ignores the EOF
        // (SIGPIPE is ignored by default in Rust), so it sits attached until
        // this streamer clears it.
        self.retire_session();
        let reason_line =
            reason.lines().map(str::trim).rfind(|l| !l.is_empty()).unwrap_or("").to_string();
        if self.leave_gone_view(&reason) {
            return;
        }
        // A view attaching while the sidebar copy is still letting go: wait
        // for it, without spending the control-failure budget on it.
        if crate::view::retry_attach_conflict(self.is_view, &reason_line) {
            self.control_failures = 0;
            self.renderer.status("waiting for the sidebar copy to release the terminal");
            self.paint();
            self.reconnect_at = Some((Instant::now() + crate::view::VIEW_CONFLICT_RETRY, exited_mode));
            return;
        }
        // A control attach refused because OUR previous client still holds the
        // terminal is not a failing host: clear that client and try once more,
        // rather than dropping the pane to read-only over our own leftovers.
        if exited_mode == Mode::Control {
            if let Some(pid) = attach_conflict_pid(
                &reason_line,
                self.pending_remote_kills.last().copied(),
                self.attach_conflict_retried,
            ) {
                self.attach_conflict_retried = true;
                self.control_failures = 0;
                self.schedule_reconnect(Mode::Control, &reason_line);
                // longer than the usual flash: the user did not cause this
                self.hint_for(
                    &format!(
                        "control was held by our own stale remote client ({pid}) — clearing it"
                    ),
                    Duration::from_secs(3),
                );
                return;
            }
        }
        // control that dies quickly twice is failing (refused/dropped): fall
        // back to observe so the pane stays viewable; a keystroke retries
        if exited_mode == Mode::Control {
            self.control_failures = if uptime < QUICK_CONTROL_FAILURE { self.control_failures + 1 } else { 0 };
            if self.control_failures >= 2 {
                self.control_failures = 0;
                self.control_sticky = true;
                self.switch_mode(Mode::Observe);
                let suffix = if reason_line.is_empty() { String::new() } else { format!(" ({reason_line})") };
                self.hint(&format!("control unavailable — viewing only{suffix}; type to retry"));
                return;
            }
        }
        self.schedule_reconnect(exited_mode, &reason_line);
    }

    async fn send(&mut self, msg: serde_json::Value) {
        if let Some(s) = self.session.as_mut() {
            let line = msg.to_string() + "\n";
            let _ = s.stdin.write_all(line.as_bytes()).await;
        }
    }

    /// Route ordinary input after preserving a partial SGR mouse sequence.
    async fn route_mouse_input(&mut self, bytes: Vec<u8>) {
        match split_mouse(&mut self.mouse_buf, bytes) {
            MouseSplit::Pending => {}
            MouseSplit::Passthrough(bytes) => self.route_input(bytes).await,
        }
        self.mouse_flush_at = (!self.mouse_buf.is_empty())
            .then(|| Instant::now() + MOUSE_INPUT_TIMEOUT);
    }

    /// Flush an unfinished mouse prefix after its short completion window.
    async fn flush_mouse_input(&mut self) {
        self.mouse_flush_at = None;
        if let Some(bytes) = flush_mouse(&mut self.mouse_buf) {
            self.route_input(bytes).await;
        }
    }

    /// Drain every complete paste in this chunk, in order.
    ///
    /// Deliberately a loop, not a one-shot: two drops land in a single read
    /// with nothing between them (a drop carries no terminator at all — which
    /// is precisely why `run` asks for DECSET 2004), so handling only the
    /// first would silently swallow the second, and leave its markers in the
    /// tail to be forwarded raw at the remote.
    async fn handle_stdin(&mut self, chunk: Vec<u8>) {
        let mut chunk = chunk;
        loop {
            match split_paste(&mut self.paste_buf, chunk) {
                PasteSplit::Pending => return,
                PasteSplit::Passthrough(bytes) => return self.route_mouse_input(bytes).await,
                PasteSplit::Complete { before, body, after } => {
                    self.route_mouse_input(before).await;
                    // A partial mouse sequence cannot continue into a paste;
                    // discard it before delivering the paste body directly.
                    if !self.mouse_buf.is_empty() {
                        self.mouse_buf.clear();
                        self.mouse_flush_at = None;
                    }
                    self.route_paste_body(body).await;
                    if after.is_empty() {
                        return;
                    }
                    chunk = after;
                }
            }
        }
    }

    /// Ordinary input, held back while an upload is in flight so the pasted
    /// remote paths cannot be overtaken by whatever was typed after them.
    async fn route_input(&mut self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        if self.paste_inflight {
            self.paste_queue.push(Queued::Raw(bytes));
            return;
        }
        self.handle_stdin_inner(bytes).await;
    }

    /// A complete paste body, markers already stripped. A file drop is
    /// uploaded; anything else is re-framed and forwarded as a paste.
    ///
    /// Re-framing is the whole point of getting here: the markers were only
    /// stripped so a drop could be recognised, and an ordinary paste that
    /// reaches the remote pty *without* them is not a paste any more — the
    /// remote app reads every newline in it as Enter, so a multi-line paste
    /// submits itself a line at a time (an agent's composer takes the first
    /// line and runs it). `deliver_input`, not `handle_stdin_inner`: a paste
    /// body is data, so its bytes must not be re-read as a Ctrl+V clipboard
    /// request, as mouse sequences, or as locally predicted keystrokes.
    async fn route_paste_body(&mut self, body: Vec<u8>) {
        if self.paste_inflight {
            self.paste_queue.push(Queued::Body(body));
            return;
        }
        // lossy only for the probe; the forward path keeps the original bytes
        let Some(paths) = crate::paste::dropped_paths(&String::from_utf8_lossy(&body)) else {
            self.deliver_input(crate::paste::bracketed_bytes(&body)).await;
            return;
        };

        self.paste_inflight = true;
        self.paste_original = Some(body);
        self.hint_sticky(&format!("uploading {} file(s)…", paths.len()));
        let tx = self.tx.clone();
        let ssh = self.args.ssh_target.clone();
        let ctl = self.args.ctl_path.clone();
        let container = self.args.container.clone();
        tokio::spawn(async move {
            let result =
                crate::paste::files_to_remote(&paths, &ssh, ctl.as_deref(), container.as_ref())
                    .await;
            let _ = tx.send(Msg::Drop(result)).await;
        });
    }

    async fn handle_drop(&mut self, result: crate::paste::DropResult) {
        self.paste_inflight = false;
        let original = self.paste_original.take();
        if let Some(text) = &result.text {
            self.deliver_input(crate::paste::bracketed(text)).await;
            self.hint(&format!("→ {text}"));
        } else if result.unchanged {
            // every path already exists over there, so the user meant those
            // files: forward what they actually dropped
            if let Some(body) = original {
                self.deliver_input(crate::paste::bracketed_bytes(&body)).await;
            }
        }
        if let Some(e) = result.error {
            self.hint(&format!("drop failed: {e}"));
        }
        self.drain_paste_queue().await;
    }

    /// Flush input held during an upload. Stops if a queued drop starts a new
    /// upload, leaving the remainder queued behind it so order is preserved.
    async fn drain_paste_queue(&mut self) {
        let mut items = std::mem::take(&mut self.paste_queue).into_iter();
        while let Some(item) = items.next() {
            match item {
                Queued::Raw(b) => self.handle_stdin_inner(b).await,
                Queued::Body(b) => {
                    self.route_paste_body(b).await;
                    if self.paste_inflight {
                        self.paste_queue.extend(items);
                        return;
                    }
                }
            }
        }
    }

    async fn handle_stdin_inner(&mut self, buf: Vec<u8>) {
        if buf.len() == 1 && buf[0] == 0x16 && !self.paste_inflight {
            self.paste_inflight = true;
            let tx = self.tx.clone();
            let ssh = self.args.ssh_target.clone();
            let ctl = self.args.ctl_path.clone();
            let container = self.args.container.clone();
            tokio::spawn(async move {
                let outcome =
                    crate::paste::clipboard_to_remote(&ssh, ctl.as_deref(), container.as_ref())
                        .await;
                let _ = tx.send(Msg::Paste(outcome)).await;
            });
            return;
        }
        if self.mode == Mode::Observe || self.switching_to == Some(Mode::Observe) {
            // no quit key: the wrapper's lifecycle belongs to the hosting pane
            if has_mouse_seq(&buf) {
                // wheel escalates only after a soft release; a stray wheel
                // while glancing shouldn't grab the remote's lock
                if contains_wheel_press(&buf) {
                    if self.control_sticky {
                        self.control_sticky = false;
                        self.switch_mode(Mode::Control);
                    } else {
                        self.hint("read-only — type to take control");
                    }
                }
                return;
            }
            // any keystroke takes control and is delivered once the session is up
            self.control_sticky = false;
            self.pending_input.push(buf);
            self.switch_mode(Mode::Control);
            return;
        }

        // control mode
        self.last_input = Instant::now();
        if buf.len() == 1 && buf[0] == 0x1c {
            // ctrl+\ — manual release. In always-control there's nothing to
            // release to, so swallow it (never forward it: ctrl+\ is SIGQUIT).
            if !self.args.always_control {
                self.control_sticky = false;
                self.switch_mode(Mode::Observe);
            }
            return;
        }
        if self.switching_to == Some(Mode::Control) || self.session.is_none() {
            // spinning up or awaiting reconnect: queue the keystroke (flushed
            // on connect) and, if in backoff, reconnect now
            self.pending_input.push(buf);
            if let Some((_, m)) = self.reconnect_at {
                self.reconnect_at = Some((Instant::now(), m));
            }
            return;
        }
        // refresh the foreground classification on any input while active.
        // the grab is always held, so mouse events also trigger this — that's
        // what catches a shell→TUI switch from a click, and the reverse from
        // `:q`. keyboard still does too, for people who never touch the mouse.
        self.spawn_foreground_poll(false);
        // and re-check shortly after input settles, to catch a change the input
        // just caused (e.g. `:q` quitting vim — the poll above still sees vim)
        self.settle_at = Some(Instant::now() + SETTLE_DELAY);
        // wheel becomes a semantic scroll (server decides app vs scrollback);
        // left-button drags go to the plugin selector; other TUI clicks/drags
        // forward to the remote pty. a shell never sees raw SGR.
        let mut rest: Vec<u8> = Vec::with_capacity(buf.len());
        let mut i = 0usize;
        let mut scrolls: Vec<serde_json::Value> = Vec::new();
        let mut sel_changed = false;
        let mut copy_span: Option<(crate::select::Pos, crate::select::Pos)> = None;
        while i < buf.len() {
            if let Some((btn, x, y, press, len)) = parse_mouse(&buf, i) {
                match mouse_action(self.remote_fg, btn, press) {
                    // Without a tty there is no grab, so this is unreachable in
                    // normal operation — but stdin could still be a pipe, and a
                    // selection there would be sized against a phantom viewport
                    // and would write OSC 52 into something that is not a
                    // terminal. Forward instead, which is what `main` did.
                    MouseAction::Select if !self.tty => {
                        rest.extend_from_slice(&buf[i..i + len]);
                    }
                    MouseAction::Select => {
                        let at = Select::locate(&self.grid, term_size().1, x, y);
                        let raw = &buf[i..i + len];
                        // motion flag (32) distinguishes a drag from the press
                        // that started it; `press` is the M/m final byte
                        match (press, btn & 32 != 0) {
                            (true, false) => self.select.press(at, raw),
                            (true, true) => self.select.drag(at),
                            (false, _) => match self.select.release(at, raw) {
                                // the clipboard holds one thing, so a second
                                // gesture in the same read legitimately wins
                                Released::Selection(span) => {
                                    copy_span = Some(span);
                                    // Finish this gesture before a later press in
                                    // the same read starts the next selection.
                                    sel_changed |= self.select.clear();
                                },
                                // It was a click, not a drag. TUI/agent get it
                                // (claude and codex discard the bytes cleanly).
                                // A shell does not: the prompt never enabled
                                // mouse reporting, and the bytes would dump
                                // into it.
                                Released::Click(bytes) => {
                                    if self.remote_fg != Some(Fg::Shell) {
                                        rest.extend_from_slice(&bytes);
                                    }
                                }
                                Released::Nothing => {}
                            },
                        }
                        sel_changed |= self.select.is_dirty();
                    }
                    MouseAction::Scroll { up } => {
                        // the viewport is about to move under the highlight,
                        // which is anchored to grid rows: leaving it up would
                        // paint reverse video over whatever scrolls into place
                        sel_changed |= self.select.clear();
                        scrolls.push(json!({
                            "type": "terminal.scroll",
                            "direction": if up { "up" } else { "down" },
                            "lines": 3,
                            "source": "wheel",
                            "column": x.saturating_sub(1),
                            "row": y.saturating_sub(1),
                            "modifiers": 0,
                        }));
                    }
                    MouseAction::ForwardRaw => rest.extend_from_slice(&buf[i..i + len]),
                    MouseAction::Drop => {}
                }
                i += len;
            } else {
                rest.push(buf[i]);
                i += 1;
            }
        }
        for s in scrolls {
            self.send(s).await;
        }
        if let Some((start, end)) = copy_span {
            let text = self.grid.selection_text(start, end);
            match crate::select::osc52(&text) {
                // no hint on success: herdr shows its own "copied to clipboard"
                // toast when it takes the OSC 52, so ours would be a duplicate
                Some(seq) => write_stdout(&seq),
                // too big for herdr to accept. Worth saying, because this is the
                // one path where nothing else reports: we never emit, so there
                // is no clipboard write for herdr to toast about.
                None if text.len() > 1024 => self.hint("selection too large to copy"),
                // an all-blank drag: leave the clipboard alone rather than
                // clearing it, and say nothing
                None => {}
            }
        }
        if !rest.is_empty() {
            // typing anywhere dismisses the highlight, the same as it would in a
            // local pane — otherwise it hangs over text the agent has redrawn.
            // `dismiss`, not `clear`: a press may have been buffered earlier in
            // this very read, and cancelling it would eat the click.
            sel_changed |= self.select.dismiss();
            if let Some(session) = self.session.as_mut() {
                let _ = write_terminal_input(&mut session.stdin, &rest).await;
            }
            // optimistic local echo: draw the keystroke now, verify on frame
            if self.predict.on_input(&rest, &self.grid) {
                self.paint();
                sel_changed = false;
            }
        }
        if sel_changed {
            self.paint();
        }
    }

    async fn deliver_input(&mut self, buf: Vec<u8>) {
        if self.mode == Mode::Observe || self.switching_to == Some(Mode::Observe) {
            self.control_sticky = false;
            self.pending_input.push(buf);
            self.switch_mode(Mode::Control);
            return;
        }
        self.last_input = Instant::now();
        if self.switching_to == Some(Mode::Control) || self.session.is_none() {
            self.pending_input.push(buf);
            if let Some((_, m)) = self.reconnect_at {
                self.reconnect_at = Some((Instant::now(), m));
            }
            return;
        }
        if let Some(session) = self.session.as_mut() {
            let _ = write_terminal_input(&mut session.stdin, &buf).await;
        }
    }

    async fn handle_paste(&mut self, outcome: crate::paste::Outcome) {
        self.paste_inflight = false;
        match outcome {
            crate::paste::Outcome::NoImage => self.deliver_input(vec![0x16]).await,
            crate::paste::Outcome::Pasted(path) => {
                self.deliver_input(crate::paste::bracketed(&path)).await;
                self.hint(&format!("→ {path}"));
            }
            crate::paste::Outcome::Failed(e) => {
                self.hint(&format!("image paste failed: {e}"));
            }
        }
    }

    /// Sidebar copy: look for a view claim on this pane, and act on a change.
    async fn poll_view_claim(&mut self) {
        if self.local_pane_id.is_none() {
            return;
        }
        // keyed by host + remote pane, not by our local pane id: a view
        // outlives this sidebar copy being recreated under a new one
        let id = crate::view::claim_key_for(&self.args);
        let now = crate::view::live_claim(&self.state_dir, &id);
        match crate::view::sidebar_step(self.claim.is_some(), now.is_some()) {
            crate::view::SidebarStep::Release => {
                if let Some(claim) = now {
                    self.enter_view_claim(claim).await;
                }
            }
            crate::view::SidebarStep::Reconnect => self.leave_view_claim().await,
            crate::view::SidebarStep::Stay => {
                // one view replaced by another between two polls: the new one
                // waits for our release marker, which its claim just cleared
                if let Some(new) = now.filter(|n| Some(n) != self.claim.as_ref()) {
                    crate::view::mark_released(&self.state_dir, &id);
                    self.forward.reset();
                    self.claim = Some(new);
                    self.draw_view_notice();
                }
            }
        }
    }

    /// Standing aside: a view holds the terminal, or the pane is idle-released.
    fn aside(&self) -> bool {
        self.claim.is_some() || self.idle_released
    }

    /// Release the remote terminal the way every other retire path does
    /// (graceful `terminal.release` for control, then the transport and the
    /// remote client). This process — and the agent identity Herdr ties to
    /// it — stays. Shared by view claims and idle release.
    async fn release_stream(&mut self) {
        self.reconnect_at = None;
        self.switching_to = None;
        self.switch_at = None;
        self.settle_at = None;
        self.hint_clear_at = None;
        // standing aside: no remote traffic and no output health to report
        self.health_at = None;
        self.pending_input.clear();
        self.select.clear();
        if let Some(mut s) = self.retire_session() {
            if s.mode == Mode::Control {
                let _ = s.stdin.write_all(b"{\"type\":\"terminal.release\"}\n").await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            kill_session_process_group(s.process_group);
        }
        if host_admission_open(&self.state_dir, self.args.host_name.as_deref()) {
            self.reap_remote_clients().await;
        }
        if let Some(id) = &self.local_pane_id {
            crate::state::clear_stream_health(&self.state_dir, id);
        }
    }

    /// Stop standing aside and attach again.
    async fn reattach(&mut self) {
        if self.tty {
            write_stdout(crate::view::NOTICE_END);
            self.sync_mouse_grab();
        }
        self.last_seen = Instant::now();
        self.renderer.status("");
        self.renderer.invalidate();
        self.backoff_idx = 0;
        self.control_failures = 0;
        self.control_sticky = false;
        self.attach_conflict_retried = false;
        self.health_at = Some(Instant::now() + OUTPUT_HEALTH_INTERVAL);
        self.connect(initial_mode(self.args.always_control, term_size())).await;
    }

    /// Stand aside for a view: release the remote terminal, tell the view,
    /// and show a notice. A view supersedes an idle release.
    async fn enter_view_claim(&mut self, claim: crate::view::ViewClaim) {
        self.claim = Some(claim);
        self.set_idle_released(false);
        self.release_stream().await;
        if self.local_pane_id.is_some() {
            crate::view::mark_released(&self.state_dir, &crate::view::claim_key_for(&self.args));
        }
        self.draw_view_notice();
    }

    fn draw_view_notice(&mut self) {
        let Some(claim) = &self.claim else { return };
        if self.tty {
            write_stdout(&crate::view::notice(&claim.tile_pane_id));
            self.mouse_grabbed = false;
        }
    }

    /// The view went away: take the terminal back.
    async fn leave_view_claim(&mut self) {
        self.claim = None;
        self.forward.reset();
        if self.local_pane_id.is_some() {
            crate::view::clear_released(&self.state_dir, &crate::view::claim_key_for(&self.args));
        }
        // a view that died without cleaning up leaves its client attached;
        // clear it before asking for the terminal it is still holding
        let orphan = crate::util::take_remote_client(
            &self.state_dir,
            &crate::view::view_record_target(&self.args.ssh_target),
            &self.args.pane_target,
        );
        self.queue_remote_kill(orphan);
        self.reattach().await;
    }

    fn set_idle_released(&mut self, on: bool) {
        self.idle_released = on;
        if let Some(id) = &self.local_pane_id {
            if on {
                crate::visibility::mark_idle_released(&self.state_dir, id);
            } else {
                crate::visibility::clear_idle_released(&self.state_dir, id);
            }
        }
    }

    /// Sidebar copy, unclaimed: release the stream when nobody has viewed the
    /// pane for the grace period, and take it back when it is on screen again.
    async fn poll_idle_release(&mut self) {
        if self.claim.is_some() {
            return;
        }
        let Some(id) = self.local_pane_id.clone() else { return };
        let now = Instant::now();
        let visible = self.visibility.read(&self.state_dir).cloned();
        if visible.as_ref().is_some_and(|v| v.shows(&id)) {
            self.last_seen = now;
        }
        match crate::visibility::idle_step(
            self.idle_released,
            visible.as_ref(),
            &id,
            self.last_seen,
            now,
        ) {
            crate::visibility::IdleStep::Stay => {}
            crate::visibility::IdleStep::Release => self.enter_idle_release().await,
            crate::visibility::IdleStep::Resume => self.leave_idle_release().await,
        }
    }

    async fn enter_idle_release(&mut self) {
        self.set_idle_released(true);
        self.release_stream().await;
        self.draw_idle_notice();
    }

    fn draw_idle_notice(&mut self) {
        if self.tty && self.idle_released && self.claim.is_none() {
            write_stdout(&crate::visibility::idle_notice());
            self.mouse_grabbed = false;
        }
    }

    async fn leave_idle_release(&mut self) {
        self.set_idle_released(false);
        self.reattach().await;
    }

    /// Input while idle-released: a focus-in or anything typed (or sent by
    /// `herdr agent prompt`) resumes at once, no daemon involved; the input
    /// is then handled exactly as if the stream had been up, which queues it
    /// until the new session is there to take it.
    async fn resume_from_input(&mut self, buf: Vec<u8>) {
        let (rest, focus_in) = crate::view::strip_focus_reports(&buf);
        // a lone focus-out is the pane being left, not looked at
        if rest.is_empty() && !focus_in {
            return;
        }
        self.leave_idle_release().await;
        if !rest.is_empty() {
            self.handle_stdin(rest).await;
        }
    }

    /// Sidebar copy input while a view holds the terminal: focus reports turn
    /// into a focus redirect, everything else goes to the view.
    async fn forward_to_view(&mut self, buf: Vec<u8>) {
        let Some(claim) = self.claim.clone() else { return };
        let (rest, focus_in) = crate::view::strip_focus_reports(&buf);
        if focus_in {
            if let Some(socket) = claim.herdr_socket.clone() {
                let tile = claim.tile_pane_id.clone();
                tokio::spawn(async move { crate::view::focus_tile(&socket, &tile).await });
            }
        }
        if !rest.is_empty() {
            self.forward.send(&claim.socket, &rest).await;
        }
    }
}

// ---------------------------------------------------------------------------
// main

/// Removes the streamer pidfile on any exit path out of `run` (stale files
/// from a hard kill are harmless — the daemon checks the pid is alive).
struct PidfileGuard(std::path::PathBuf);
impl Drop for PidfileGuard {
    fn drop(&mut self) {
        // A replacement streamer can publish its pid before this process has
        // finished unwinding. Remove only our own claim; never unlink the new
        // owner's pidfile.
        let ours = std::fs::read_to_string(&self.0)
            .ok()
            .is_some_and(|pid| pid.trim() == std::process::id().to_string());
        if ours {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

/// Consecutive crash respawns the supervisor spends before it gives up and
/// lets the pane close. Bounded because a streamer that cannot survive its own
/// first second — a bad remote binary, a pane target that no longer exists —
/// must not become an endless spawn loop; a tombstone the owner can `restore`
/// is the better failure.
const STREAMER_RESPAWN_LIMIT: u32 = 5;

/// Ceiling on the doubling ladder (1s, 2s, 4s, 8s, 8s). Kept short: the whole
/// budget is then under half a minute, so a transport blip costs a visible
/// pause rather than a pane, and a genuinely broken streamer still fails fast.
const STREAMER_RESPAWN_BACKOFF_CAP: Duration = Duration::from_secs(8);

/// How long a child must have run for its predecessors' crashes to stop
/// counting against it. Longer than the streamer's own connect ladder, so a
/// child that only ever reconnects has not "run healthily"; short enough that
/// a stream alive for a working session gets a fresh budget when it dies.
const STREAMER_HEALTHY_UPTIME: Duration = Duration::from_secs(30);

/// A respawn the supervisor has decided to make.
#[derive(Debug, PartialEq, Eq)]
struct Respawn {
    /// wait this long before starting the replacement
    delay: Duration,
    /// which attempt this is, to carry into the next decision
    attempt: u32,
}

/// Whether to replace a streamer child that just exited, and after how long.
///
/// Pure so the ladder, the reset, and the give-up point are testable without a
/// live pane. Three rules, in order:
///
/// - a child that exited *successfully* is never replaced. Zero means the
///   stream ended deliberately — the remote pane closed, the user quit — and
///   respawning would resurrect a pane the owner just closed.
/// - a child that ran for `STREAMER_HEALTHY_UPTIME` before dying starts from a
///   clean budget. The budget exists to catch a streamer that cannot come up
///   at all; a stream that worked for an hour and then lost its transport is a
///   different event and should not inherit an old crash's rung.
/// - otherwise the ladder doubles and the budget is finite.
fn respawn_decision(success: bool, ran_for: Duration, attempts: u32) -> Option<Respawn> {
    if success {
        return None;
    }
    let spent = if ran_for >= STREAMER_HEALTHY_UPTIME { 0 } else { attempts };
    let attempt = spent + 1;
    if attempt > STREAMER_RESPAWN_LIMIT {
        return None;
    }
    let delay = Duration::from_secs(1u64 << (attempt - 1)).min(STREAMER_RESPAWN_BACKOFF_CAP);
    Some(Respawn { delay, attempt })
}

fn spawn_supervised_streamer(agent: Option<&str>) -> Result<tokio::process::Child> {
    // Same resolution as every other self-respawn: after an in-place rebuild
    // current_exe() names "<path> (deleted)" on Linux, which cannot run.
    let exe = crate::util::self_exe_path()
        .ok_or_else(|| err("cannot locate the herdr-mirror binary to respawn the streamer"))?;
    let mut command = tokio::process::Command::new(exe);
    command.arg("pane-stream").args(std::env::args_os().skip(2));
    command.env("HERDR_MIRROR_SUPERVISED", "1");
    match agent {
        Some(agent) => {
            command.env("HERDR_AGENT", agent);
        }
        None => {
            command.env_remove("HERDR_AGENT");
        }
    }
    Ok(command.spawn()?)
}

/// How long a stopped child may take to exit before it is SIGKILLed. Wider
/// than the streamer's own remote-client cleanup deadline
/// (`REMOTE_KILL_TIMEOUT`) plus its control release, so a child that is doing
/// exactly what it should is not killed halfway through.
const STREAMER_STOP_GRACE: Duration = Duration::from_secs(5);

async fn stop_supervised_streamer(child: &mut tokio::process::Child) {
    if let Some(pid) = child.id() {
        unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    }
    if tokio::time::timeout(STREAMER_STOP_GRACE, child.wait()).await.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

/// Keep the local Herdr pane alive while swapping the child that actually
/// streams the remote terminal. Herdr 0.8.0 caches wrapper detection for a
/// foreground pid across exec(2), while exiting the pane's root process closes
/// the pane. A stable unmarked supervisor plus replaceable marked child gives
/// Herdr a new pid to inspect without touching the remote pane.
pub async fn supervise(args: Args) -> Result<()> {
    if args.dump {
        return run(args).await;
    }
    let Ok(local_pane_id) = std::env::var("HERDR_PANE_ID") else {
        return run(args).await;
    };
    let state_dir = crate::util::state_dir();
    let pid_path = crate::util::pane_pid_path(&state_dir, &local_pane_id);
    if let Some(dir) = pid_path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&pid_path, std::process::id().to_string())?;
    let _pane_pidfile = PidfileGuard(pid_path);
    let _ = crate::state::take_pane_hint(&state_dir, &local_pane_id);
    // A dead supervisor cannot leave a pause acknowledgement that makes its
    // replacement stagger an ordinary, unpaused start.
    let _ = take_stream_pause(&state_dir, &local_pane_id);

    // Register shutdown before waiting on an explicit pause. A paused pane is
    // still an ordinary local Herdr pane: closing it or suspending its host
    // must be able to end the supervisor immediately, even if start never
    // clears the global marker.
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sighup = signal(SignalKind::hangup())?;
    let mut sigusr1 = signal(SignalKind::user_defined1())?;

    let mut desired = crate::state::pane_agent_hint(&state_dir, &local_pane_id).flatten();
    // A restored local Herdr session can start its pane roots while Mirror is
    // explicitly paused. Publish the stable supervisor pid above, but do not
    // create a pane-stream child (and therefore no SSH) until start clears the
    // marker. Resumption uses the same stagger as an already-running child.
    tokio::select! {
        _ = wait_for_stream_resume(&state_dir, &local_pane_id) => {}
        _ = sigterm.recv() => return Ok(()),
        _ = sigint.recv() => return Ok(()),
        _ = sighup.recv() => return Ok(()),
    }
    let mut child = spawn_supervised_streamer(desired.as_deref())?;
    // when the child now running was started, and how much of the crash budget
    // its predecessors have already spent (see `respawn_decision`)
    let mut started = Instant::now();
    let mut attempts = 0u32;
    // Every exit from here is also an exit for the child's remote attach
    // client. The child normally clears its own record on the way out, so
    // these sweeps are no-ops; they matter exactly when it could not — a
    // SIGKILL after the stop grace, or a panic.
    loop {
        tokio::select! {
            status = child.wait() => {
                let status = status?;
                // Before anything else, and before any replacement: the dead
                // child's remote `terminal session` client outlives it, and it
                // would refuse the replacement's control attach on the same
                // remote pane. Identity-guarded, like every other kill here.
                reap_recorded_remote_client(&args, &state_dir).await;
                // pane-stream exits cleanly when it observes an explicit pause.
                // Keep this supervisor — the pane's root process — alive, and
                // resume only after the shared marker clears. A daemon crash
                // has no marker and therefore follows the old exit policy.
                let resume = tokio::select! {
                    resumed = wait_for_stream_resume(&state_dir, &local_pane_id) => resumed,
                    _ = sigterm.recv() => return Ok(()),
                    _ = sigint.recv() => return Ok(()),
                    _ = sighup.recv() => return Ok(()),
                };
                if resume {
                    desired = crate::state::pane_agent_hint(&state_dir, &local_pane_id).flatten();
                    child = spawn_supervised_streamer(desired.as_deref())?;
                    started = Instant::now();
                    attempts = 0;
                    continue;
                }
                let Some(respawn) = respawn_decision(status.success(), started.elapsed(), attempts)
                else {
                    // Budget spent, or a deliberate end of the stream. Leaving
                    // is what closes the pane, which is the point: Mirror
                    // tombstones it and `herdr-mirror restore` brings it back.
                    return if status.success() {
                        Ok(())
                    } else {
                        Err(err(format!("streamer child exited: {status}")))
                    };
                };
                attempts = respawn.attempt;
                // One notice per respawn, addressed to this pane, collected by
                // the replacement on its next poke — the same channel `pick`
                // uses to say "closing the local tab". The log line is the
                // durable half: a respawn that succeeds leaves no other trace.
                let notice = format!(
                    "stream died ({status}); restarting in {}s ({}/{})",
                    respawn.delay.as_secs(),
                    respawn.attempt,
                    STREAMER_RESPAWN_LIMIT,
                );
                crate::state::set_pane_hint(&state_dir, &local_pane_id, &notice);
                crate::util::Logger::new(&state_dir, false)
                    .log(&format!("pane {local_pane_id}: {notice}"));
                // Wait out the rung, but stay killable while doing it: the
                // child is already gone and its client already reaped, so a
                // shutdown signal here is simply an exit.
                tokio::select! {
                    _ = tokio::time::sleep(respawn.delay) => {}
                    _ = sigterm.recv() => return Ok(()),
                    _ = sigint.recv() => return Ok(()),
                    _ = sighup.recv() => return Ok(()),
                }
                started = Instant::now();
                child = spawn_supervised_streamer(desired.as_deref())?;
            }
            _ = sigusr1.recv() => {
                // An explicit restart request outranks everything else this
                // signal can mean. The child is streaming, and by every local
                // check healthy — that is exactly the state the daemon's sweep
                // saw before deciding one direction of it was dead — so nothing
                // the supervisor can ask the child would replace it.
                //
                // Replacing the CHILD is what keeps the pane: the supervisor is
                // the pane's root process, so the owner sees a repaint where
                // the frozen screen was, not a closed pane and a tombstone.
                if crate::state::take_stream_restart(&state_dir, &local_pane_id) {
                    crate::state::set_pane_hint(
                        &state_dir,
                        &local_pane_id,
                        "output stalled — restarting the stream",
                    );
                    crate::util::Logger::new(&state_dir, false).log(&format!(
                        "pane {local_pane_id}: restarting the streamer (output direction stalled)"
                    ));
                    stop_supervised_streamer(&mut child).await;
                    // the replacement attaches to the same remote pane, so an
                    // orphan of the child we just stopped would refuse it
                    reap_recorded_remote_client(&args, &state_dir).await;
                    desired = crate::state::pane_agent_hint(&state_dir, &local_pane_id).flatten();
                    child = spawn_supervised_streamer(desired.as_deref())?;
                    // a deliberate replacement is not a crash: fresh clock,
                    // fresh budget
                    started = Instant::now();
                    attempts = 0;
                    continue;
                }
                let next = crate::state::pane_agent_hint(&state_dir, &local_pane_id).flatten();
                if next != desired {
                    stop_supervised_streamer(&mut child).await;
                    // the replacement attaches to the same remote pane, so an
                    // orphan of the child we just stopped would refuse it
                    reap_recorded_remote_client(&args, &state_dir).await;
                    desired = next;
                    child = spawn_supervised_streamer(desired.as_deref())?;
                    // a deliberate replacement is not a crash: fresh clock,
                    // fresh budget
                    started = Instant::now();
                    attempts = 0;
                } else if let Some(pid) = child.id() {
                    // No agent change: this is an ordinary addressed pane hint.
                    unsafe { libc::kill(pid as i32, libc::SIGUSR1) };
                }
            }
            _ = sigterm.recv() => {
                stop_supervised_streamer(&mut child).await;
                reap_recorded_remote_client(&args, &state_dir).await;
                return Ok(());
            }
            _ = sigint.recv() => {
                stop_supervised_streamer(&mut child).await;
                reap_recorded_remote_client(&args, &state_dir).await;
                return Ok(());
            }
            _ = sighup.recv() => {
                stop_supervised_streamer(&mut child).await;
                reap_recorded_remote_client(&args, &state_dir).await;
                return Ok(());
            }
        }
    }
}

pub async fn run(args: Args) -> Result<()> {
    run_with(args, None).await.map(|_| ())
}

/// What a `herdr-mirror view` process adds to the ordinary streamer: the input
/// socket the sidebar copy forwards to, and which sidebar copy it stands for.
pub struct ViewRuntime {
    pub listener: std::os::unix::net::UnixListener,
    pub sidebar_pane_id: String,
}

/// How a view's streamer ended.
#[derive(Debug, PartialEq, Eq)]
pub enum ViewEnd {
    /// signal, closed tile, pause: the view is over
    Done,
    /// the remote pane it streamed no longer exists; re-resolve the target
    TargetGone,
}

/// The streamer, run in a tile on behalf of a sidebar copy (see `view.rs`).
pub async fn run_view(args: Args, view: ViewRuntime) -> Result<ViewEnd> {
    run_with(args, Some(view)).await
}

async fn run_with(args: Args, view: Option<ViewRuntime>) -> Result<ViewEnd> {
    let is_view = view.is_some();
    let supervised = !is_view && std::env::var("HERDR_MIRROR_SUPERVISED").as_deref() == Ok("1");
    let tty = !args.dump && unsafe { libc::isatty(libc::STDOUT_FILENO) } == 1;
    let local_pane_id = tty.then(|| std::env::var("HERDR_PANE_ID").ok()).flatten();
    let state_dir = crate::util::state_dir();
    let record_target = if is_view {
        crate::view::view_record_target(&args.ssh_target)
    } else {
        args.ssh_target.clone()
    };

    // The supervisor owns the visible local pane. A child born during an
    // explicit pause exits before opening any transport; the supervisor holds
    // the pane and recreates this child after the staggered resume.
    if supervised && !stream_may_connect(&state_dir) {
        if let Some(id) = &local_pane_id {
            mark_stream_pause(&state_dir, id);
        }
        return Ok(ViewEnd::Done);
    }

    // announce ourselves so the daemon can tell its typed `exec` took
    // (see util::streamer_pid_path); --dump is a human diagnostic, not a
    // daemon-spawned streamer, so it must not claim the slot
    // A view is not the mirror pane's streamer and must not claim its slots.
    let _pidfile = (!args.dump && !is_view).then(|| {
        let path =
            crate::util::streamer_pid_path(&state_dir, &args.ssh_target, &args.pane_target);
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        std::fs::write(&path, std::process::id().to_string()).ok().map(|_| PidfileGuard(path))
    }).flatten();

    // Publish the pid before releasing the launch claim. Recovery always sees
    // at least one guard, even while herdr's process snapshot catches up.
    if _pidfile.is_some() {
        if let Some(id) = &local_pane_id {
            crate::util::clear_streamer_spawn_pending(&state_dir, id);
        }
    }

    // Say which local pane we are drawing, so anything holding only a herdr
    // pane id can find us. herdr hands every event hook the id of the pane it
    // is talking about and no way to write to it; this is how a hook reaches
    // the streamer sitting in that pane. HERDR_PANE_ID comes from herdr itself
    // and is inherited by whatever it starts in a pane, which is us.
    let _pane_pidfile = (!supervised && !is_view).then(|| local_pane_id.as_deref().map(|id| {
        let path = crate::util::pane_pid_path(&state_dir, id);
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let _ = std::fs::write(&path, std::process::id().to_string());
        // drop anything addressed to this pane before we existed: it describes
        // something that happened to a previous occupant
        let _ = crate::state::take_pane_hint(&state_dir, id);
        PidfileGuard(path)
    })).flatten();
    let raw = if tty {
        // 1002/1006: button-event mouse tracking with SGR encoding, so wheel and
        // clicks reach us instead of scrolling the hosting pane's scrollback
        // 2004 (bracketed paste) is asked for so herdr frames a paste for us:
        // it only wraps one when the pane's app has enabled it, and a file
        // drop otherwise arrives as bare text with no terminator at all. The
        // framing is stripped only to recognise a drop and put back on the way
        // out (`route_paste_body`), so the remote app still sees a paste.
        // 1007 OFF (alternate scroll). The grab is always held, so mouse
        // reporting wins the routing and 1007 is never consulted. Kept as a
        // backstop: if the grab were ever lost, herdr would see "alt screen,
        // no mouse reporting, 1007 on" and type an Up/Down arrow per wheel
        // notch into us, which we'd forward, and a shell would walk command
        // history instead of scrolling (#69).
        write_stdout("\x1b[?1049h\x1b[2J\x1b[H\x1b[?1002h\x1b[?1006h\x1b[?2004h\x1b[?1007l");
        RawMode::enable()
    } else {
        None
    };

    let (tx, mut rx) = mpsc::channel::<Msg>(256);

    // stdin reader
    {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut stdin = tokio::io::stdin();
            let mut buf = [0u8; 1024];
            loop {
                match stdin.read(&mut buf).await {
                    Ok(0) | Err(_) => {
                        let _ = tx.send(Msg::StdinEof).await;
                        break;
                    }
                    Ok(n) => {
                        if tx.send(Msg::Stdin(buf[..n].to_vec())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    // One metadata channel for this pane, opened lazily on the first poll and
    // kept for the streamer's life (see `poll_channel`).
    let fg_poller = crate::foreground::shared(match &args.container {
        Some(ct) => crate::poll_channel::Transport::Docker {
            docker_bin: ct.docker_bin.clone(),
            kind: ct.kind.clone(),
        },
        None => crate::poll_channel::Transport::Ssh {
            target: args.ssh_target.clone(),
            ctl_path: args.ctl_path.clone(),
        },
    });

    let mut app = App {
        args,
        state_dir: state_dir.clone(),
        tty,
        grid: Grid::new(),
        renderer: Renderer::new(),
        tx,
        mode: Mode::Observe,
        switching_to: None,
        switch_at: None,
        session: None,
        next_gen: 0,
        backoff_idx: 0,
        reconnect_at: None,
        pending_remote_kills: Vec::new(),
        attach_conflict_retried: false,
        control_failures: 0,
        control_sticky: false,
        pending_input: Vec::new(),
        last_input: Instant::now(),
        hint_clear_at: None,
        predict: Predictor::new(),
        remote_fg: None,
        select: Select::new(),
        last_select_rows: None,
        fg_poll_at: None,
        fg_poll_gate: crate::foreground::PollGate::new(),
        fg_poller,
        last_frame_at: Instant::now(),
        remote_revision: None,
        remote_advanced_at: None,
        health_at: None,
        settle_at: None,
        mouse_grabbed: tty, // startup wrote ?1002h when we're a tty
        // startup leaves the pane in normal cursor mode; the first classification
        // moves it if the remote turns out to be a TUI
        app_cursor_keys: false,
        paste_inflight: false,
        paste_buf: Vec::new(),
        mouse_buf: Vec::new(),
        mouse_flush_at: None,
        paste_queue: Vec::new(),
        paste_original: None,
        is_view,
        view_target_gone: false,
        record_target: record_target.clone(),
        local_pane_id: local_pane_id.clone(),
        claim: None,
        forward: crate::view::Forwarder::default(),
        idle_released: false,
        last_seen: Instant::now(),
        visibility: crate::visibility::Reader::default(),
    };
    // A view takes the sidebar copy's input from its socket, exactly as if it
    // had been typed into the tile.
    if let Some(view) = view {
        let tx = app.tx.clone();
        crate::view::spawn_input_listener(view.listener, move |bytes| {
            let tx = tx.clone();
            tokio::spawn(async move { tx.send(Msg::Stdin(bytes)).await.is_ok() })
        })?;
        let _ = view.sidebar_pane_id;
    }
    // A streamer that was killed before it could clean up (a daemon restart, a
    // closed pane, a SIGKILL) leaves its remote client attached and its pid on
    // disk. We are the next streamer for that same mirror pane, so the record
    // is ours to spend: `connect` clears it before asking for the terminal the
    // orphan is still holding. `--dump` never publishes a record and must not
    // consume one.
    if !app.args.dump {
        let adopted = crate::util::take_remote_client(
            &state_dir,
            &record_target,
            &app.args.pane_target,
        );
        app.queue_remote_kill(adopted);
    }

    // Control is authoritative on the remote: the server resizes the remote pty
    // to whatever we ask for, beating even a larger live client over there. So
    // entering Control with a size we cannot vouch for is what let a local herdr
    // with no client attached drag a healthy remote pane down to its 80x24
    // placeholder (#23). Observe never resizes anything, so it is the safe place
    // to wait: the first resize or keystroke proves a human and promotes us.
    // BEFORE connect: spawning the session awaits a process launch, and a
    // SIGWINCH arriving in that window is lost outright (its default disposition
    // is to be ignored). That window is exactly when a client attaching lays out
    // a freshly created pane — the resize we now promote on. Registered first,
    // tokio buffers it and delivers it once the loop starts.
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sighup = signal(SignalKind::hangup())?; // pane closed — don't orphan the ssh child
    // someone left a notice for this pane and wants it seen now
    let mut sigusr1 = signal(SignalKind::user_defined1())?;
    let mut sigwinch = signal(SignalKind::window_change())?;
    // Only a sidebar copy can be claimed by a view.
    let claimable = !app.is_view && !app.args.dump && app.local_pane_id.is_some();
    // The pause marker, the host's health record, view claims and the
    // visibility record change only when a file in one of these directories is
    // created, replaced or removed. Wait for that, not on a 100 ms timer: forty
    // streamers polling them were ~14k file operations per 15 s (bug 85). The
    // slow tick re-checks what no directory event carries (a dead daemon's
    // visibility record, the idle-release deadline).
    let state_watch = crate::watch::DirWatch::new(&[
        state_dir.as_path(),
        crate::state::host_health_dir(&state_dir).as_path(),
        crate::view::claims_dir(&state_dir).as_path(),
    ]);
    let mut state_tick = tokio::time::interval(if state_watch.is_some() {
        crate::watch::FALLBACK_TICK
    } else {
        crate::watch::UNWATCHED_TICK
    });
    state_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    state_tick.tick().await; // the first tick is immediate; the checks below just ran

    // A view may already hold this pane (the supervisor replaced its child
    // while a tile was showing it): stand aside from the start.
    if claimable {
        app.poll_view_claim().await;
    }
    app.connect(initial_mode(app.args.always_control, term_size())).await;
    if app.claim.is_none() {
        app.health_at = Some(Instant::now() + OUTPUT_HEALTH_INTERVAL);
    }
    // the pane may have been laid out while the session was spawning; the signal
    // for that is buffered above, but check directly too
    if app.claim.is_some() {
        // nothing to size or explain: the notice is up
    } else if app.mode == Mode::Observe && initial_mode(app.args.always_control, term_size()) == Mode::Control
    {
        app.switch_mode(Mode::Control);
    } else if app.args.always_control && app.mode == Mode::Observe {
        // F3: otherwise the pane is inert with no explanation
        app.hint("read-only until this pane is sized — type to take control");
    }

    loop {
        // a view whose remote pane is gone hands back to `cmd_view`
        if app.view_target_gone {
            break;
        }
        // earliest pending deadline: mode-switch gap, reconnect, hint clear, idle release
        let idle_at = (app.mode == Mode::Control
            && app.switching_to.is_none()
            && app.session.is_some()
            && !app.args.always_control
            && app.args.control_idle_secs > 0)
            .then(|| app.last_input + Duration::from_secs(app.args.control_idle_secs));
        let sleep = crate::util::sleep_until_earliest([
            app.switch_at,
            app.reconnect_at.map(|(t, _)| t),
            app.hint_clear_at,
            idle_at,
            app.predict.deadline(),
            app.settle_at,
            app.health_at,
            app.mouse_flush_at,
        ]);

        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    None => break,
                    Some(Msg::Frame { gen, frame }) => app.handle_frame(gen, frame),
                    Some(Msg::RemotePid { gen, pid }) => app.handle_remote_pid(gen, pid),
                    Some(Msg::SessionExit { gen, mode, reason, uptime }) => app.handle_exit(gen, mode, reason, uptime),
                    Some(Msg::Stdin(buf)) => if app.claim.is_some() {
                        app.forward_to_view(buf).await;
                    } else if app.idle_released {
                        app.resume_from_input(buf).await;
                    } else {
                        app.last_seen = Instant::now();
                        app.handle_stdin(buf).await;
                    },
                    // a tile whose terminal went away: the view is over. A
                    // sidebar copy's lifecycle belongs to its pane (SIGHUP).
                    Some(Msg::StdinEof) => if app.is_view { break },
                    // keep the last good classification if a poll failed (None)
                    // (a poll answered after the stream was released must not
                    // re-grab the mouse under the notice)
                    Some(Msg::Foreground(v)) => if v.is_some() && !app.aside() {
                        // a foreground change means the screen belongs to a
                        // different program now, so the old highlight points at
                        // text that is gone
                        if v != app.remote_fg && app.select.clear() {
                            app.paint();
                        }
                        app.remote_fg = v;
                        app.sync_mouse_grab();
                        app.sync_cursor_key_mode();
                    },
                    Some(Msg::RemoteRevision(v)) => app.note_remote_revision(v),
                    Some(Msg::Paste(outcome)) => app.handle_paste(outcome).await,
                    Some(Msg::Drop(result)) => app.handle_drop(result).await,
                }
            }
            _ = sigwinch.recv() => {
                if app.claim.is_some() {
                    app.draw_view_notice();
                    continue;
                }
                if app.idle_released {
                    app.draw_idle_notice();
                    continue;
                }
                app.renderer.invalidate();
                // a resize means a client is laying this pane out, so the size is
                // now a real viewport: take control if that is what we're for.
                // control_sticky means control was refused twice in a row and we
                // told the user "type to retry" — a window drag must not turn
                // that into a reconnect storm.
                if app.args.always_control && app.mode == Mode::Observe && !app.control_sticky {
                    app.switch_mode(Mode::Control);
                }
                if app.mode == Mode::Control {
                    // capped like the initial connect: a local window drag must
                    // not push a capped host past its ceiling either
                    let (cols, rows) = app.control_size();
                    app.send(json!({ "type": "terminal.resize", "cols": cols, "rows": rows })).await;
                }
                app.paint();
            }
            _ = sigusr1.recv() => {
                if let Some(id) = local_pane_id.as_deref() {
                    if let Some(msg) = crate::state::take_pane_hint(&state_dir, id) {
                        app.hint_for(&msg, Duration::from_secs(4));
                    }
                }
            }
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
            _ = sighup.recv() => break,
            _ = state_changed(state_watch.as_ref(), &mut state_tick) => {
                if !stream_may_connect(&state_dir) {
                    if let Some(id) = local_pane_id.as_ref().filter(|_| !is_view) {
                        // The daemon reload may clear its global marker before
                        // our bounded remote-client cleanup finishes. Leave a
                        // pane-local acknowledgement so the supervisor still
                        // knows this clean exit means pause, not user intent.
                        mark_stream_pause(&state_dir, id);
                    }
                    app.renderer.status("Mirror paused — remote keeps running");
                    app.paint();
                    break;
                }
                if !host_admission_open(&state_dir, app.args.host_name.as_deref())
                    && app.session.is_some()
                {
                    app.stop_session();
                    app.reconnect_at = Some((Instant::now() + HOST_HEALTH_WAIT, app.mode));
                    app.renderer.status("waiting for Mirror host health trial");
                    app.paint();
                }
                if claimable {
                    app.poll_view_claim().await;
                    app.poll_idle_release().await;
                }
            }
            _ = sleep => {
                let now = Instant::now();
                if app.switch_at.is_some_and(|t| t <= now) {
                    app.switch_at = None;
                    if let Some(m) = app.switching_to.take() {
                        app.connect(m).await; // pending input from the gap flushes here
                    }
                }
                if let Some((t, m)) = app.reconnect_at {
                    if t <= now {
                        app.reconnect_at = None;
                        app.connect(m).await;
                    }
                }
                if app.hint_clear_at.is_some_and(|t| t <= now) {
                    app.hint_clear_at = None;
                    app.renderer.status("");
                    app.paint();
                }
                if idle_at.is_some_and(|t| t <= now) && app.mode == Mode::Control && app.switching_to.is_none() {
                    app.control_sticky = true;
                    app.switch_mode(Mode::Observe);
                    app.hint("control released (idle) — type to retake");
                }
                if app.settle_at.is_some_and(|t| t <= now) {
                    app.settle_at = None;
                    app.spawn_foreground_poll(true); // forced: bypass the throttle
                }
                if app.health_at.is_some_and(|t| t <= now) {
                    app.health_at = Some(now + OUTPUT_HEALTH_INTERVAL);
                    // forced: the pane this exists for is the one nobody is
                    // typing into, so the input-driven throttle never releases
                    app.spawn_foreground_poll(true);
                    app.publish_stream_health(local_pane_id.as_deref());
                }
                if app.mouse_flush_at.is_some_and(|t| t <= now) {
                    app.flush_mouse_input().await;
                }
                if app.predict.deadline().is_some_and(|t| t <= now) {
                    app.predict.on_tick(); // wipe timed-out ghosts (no-echo prompts)
                    app.paint();
                }
            }
        }
    }

    // clean shutdown: release control if held, kill the whole local transport
    // tree (including any ProxyCommand) AND the remote client it was carrying,
    // restore tty. Killing only the local side is what left fifteen attached
    // clients on `caddypayio-vm`.
    if let Some(mut s) = app.retire_session() {
        if s.mode == Mode::Control {
            let _ = s.stdin.write_all(b"{\"type\":\"terminal.release\"}\n").await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        kill_session_process_group(s.process_group);
    }
    // Bounded by REMOTE_KILL_TIMEOUT for the batch: a supervisor waiting to
    // replace this child, or a pane closing, must not be held up. Whatever is
    // left unconfirmed stays recorded for the next streamer of this pane.
    if host_admission_open(&state_dir, app.args.host_name.as_deref()) {
        app.reap_remote_clients().await;
    }
    // A record outliving its streamer would let the sweep judge a pane that no
    // longer has one; that pane's recovery is `heal_zombie_mirrors`, not a
    // restart request addressed to nobody.
    if let Some(id) = local_pane_id.as_deref().filter(|_| !is_view) {
        crate::state::clear_stream_health(&state_dir, id);
    }
    if let Some(id) = local_pane_id.as_deref().filter(|_| app.idle_released) {
        crate::visibility::clear_idle_released(&state_dir, id);
    }
    if tty {
        if app.aside() {
            write_stdout(crate::view::NOTICE_END);
        }
        // ?1l with the rest: leaving the hosting pane in application cursor mode
        // would misencode arrows for whatever runs there next
        // 1007 back on: it is a default-on mode we turned off, so leaving it
        // clear would silently change the wheel for whatever runs in this pane
        // after the streamer exits
        write_stdout("\x1b[?2004l\x1b[?1002l\x1b[?1006l\x1b[?1l\x1b[?1007h\x1b[?25h\x1b[?1049l");
    }
    if let Some(raw) = raw {
        raw.restore();
    }
    Ok(if app.view_target_gone { ViewEnd::TargetGone } else { ViewEnd::Done })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pane_transport_cleanup_terminates_proxy_command() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid_path = std::env::temp_dir().join(format!(
            "herdr-mirror-pane-proxy-{}-{nonce}.pid",
            std::process::id()
        ));
        let proxy = format!(
            "ProxyCommand=sh -c 'echo $$ > {}; exec sleep 30'",
            pid_path.display()
        );
        let mut command = tokio::process::Command::new("ssh");
        command
            .args([
                "-o",
                "BatchMode=yes",
                "-o",
                &proxy,
                "proxy-test.invalid",
                "true",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        isolate_session_process_group(&mut command);
        let mut child = command.spawn().unwrap();
        let process_group = child.id().unwrap() as i32;

        for _ in 0..40 {
            if pid_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let proxy_pid: i32 = std::fs::read_to_string(&pid_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        kill_session_process_group(process_group);
        tokio::time::timeout(Duration::from_secs(1), child.wait())
            .await
            .expect("ssh did not exit after its process group was killed")
            .unwrap();

        let mut proxy_survived = false;
        for _ in 0..20 {
            proxy_survived = unsafe { libc::kill(proxy_pid, 0) } == 0;
            if !proxy_survived {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if proxy_survived {
            unsafe { libc::kill(proxy_pid, libc::SIGKILL) };
        }
        let _ = std::fs::remove_file(pid_path);
        assert!(!proxy_survived, "ProxyCommand survived pane transport cleanup");
    }

    #[test]
    fn pane_ssh_stream_disables_configured_control_sockets() {
        let argv = ssh_stream_args("work", "exec herdr terminal session observe w5:pM");

        assert_eq!(
            &argv[crate::remote::SSH_COMMON_OPTS.len()..],
            [
                "-S",
                "none",
                "work",
                "exec herdr terminal session observe w5:pM",
            ]
        );
    }

    /// Uncapped must stay byte-identical to the old `term_size()` call, or
    /// every existing headless-remote config silently changes behaviour.
    #[test]
    fn uncapped_control_size_is_the_local_size() {
        assert_eq!(cap_size((253, 50), None, None), (253, 50));
    }

    #[test]
    fn caps_only_bite_when_the_local_pane_is_bigger() {
        // the real case: local 253 cols vs a laptop that renders at 212
        assert_eq!(cap_size((253, 50), Some(212), Some(58)), (212, 50));
        // a local pane smaller than the cap is left alone — a cap is a ceiling,
        // never a demand for a size the local window can't show
        assert_eq!(cap_size((120, 30), Some(212), Some(58)), (120, 30));
        // one axis capped, the other free
        assert_eq!(cap_size((253, 50), Some(212), None), (212, 50));
        assert_eq!(cap_size((253, 90), None, Some(58)), (253, 58));
        // equal is not clamped away
        assert_eq!(cap_size((212, 58), Some(212), Some(58)), (212, 58));
    }

    #[test]
    fn wheel_always_semantic_scroll_even_on_tui_foreground() {
        // remote foreground classified as a TUI (e.g. `claude`) — wheel must
        // still produce a semantic scroll, not a raw forward, or it silently
        // does nothing when the TUI doesn't consume mouse wheel input
        assert_eq!(mouse_action(Some(Fg::Agent), 64, true), MouseAction::Scroll { up: true });
        assert_eq!(mouse_action(Some(Fg::Agent), 65, true), MouseAction::Scroll { up: false });
        // unclassified/shell foreground: wheel still scrolls
        assert_eq!(mouse_action(None, 64, true), MouseAction::Scroll { up: true });
        assert_eq!(mouse_action(Some(Fg::Shell), 65, true), MouseAction::Scroll { up: false });
        // the wheel never reaches the selection path at all, which is what
        // keeps PR #54's scroll regression impossible here by construction
        assert_eq!(mouse_action(Some(Fg::Agent), 0, true), MouseAction::Select);
        assert_eq!(mouse_action(Some(Fg::Shell), 0, true), MouseAction::Select); // shell drag-select
        assert_eq!(mouse_action(None, 0, true), MouseAction::Drop); // unclassified click
    }

    #[test]
    fn every_remote_tui_selects_on_drag_agent_or_not() {
        for fg in [Fg::Agent, Fg::Mouse] {
            assert_eq!(mouse_action(Some(fg), 0, true), MouseAction::Select, "{fg:?} press");
            assert_eq!(mouse_action(Some(fg), 32, true), MouseAction::Select, "{fg:?} drag");
            assert_eq!(mouse_action(Some(fg), 0, false), MouseAction::Select, "{fg:?} release");
            // other buttons still reach the app
            assert_eq!(mouse_action(Some(fg), 1, true), MouseAction::ForwardRaw);
            assert_eq!(mouse_action(Some(fg), 2, true), MouseAction::ForwardRaw);
        }
        // a shell holds the grab too, so the plugin selector runs; middle/right
        // stay dropped so SGR never hits a prompt. unknown fails safe.
        assert_eq!(mouse_action(Some(Fg::Shell), 0, true), MouseAction::Select);
        assert_eq!(mouse_action(Some(Fg::Shell), 32, true), MouseAction::Select);
        assert_eq!(mouse_action(Some(Fg::Shell), 0, false), MouseAction::Select);
        assert_eq!(mouse_action(Some(Fg::Shell), 1, true), MouseAction::Drop);
        assert_eq!(mouse_action(Some(Fg::Shell), 2, true), MouseAction::Drop);
        assert_eq!(mouse_action(None, 0, true), MouseAction::Drop);
    }

    #[test]
    fn a_modified_wheel_still_scrolls_instead_of_becoming_a_click() {
        // The button number is the low two bits PLUS bit 6, so shift+wheel-up is
        // 68 and `btn & 0b11` reads it as a left press: the wheel then drives the
        // selection, the scroll is lost, and a later left release replays the
        // wheel bytes to the app as a click.
        for mods in [4, 8, 16, 4 + 8, 4 + 16, 8 + 16, 4 + 8 + 16] {
            assert_eq!(
                mouse_action(Some(Fg::Agent), 64 + mods, true),
                MouseAction::Scroll { up: true },
                "shift/alt/ctrl + wheel-up (btn {})",
                64 + mods
            );
            assert_eq!(
                mouse_action(Some(Fg::Agent), 65 + mods, true),
                MouseAction::Scroll { up: false },
                "modified wheel-down (btn {})",
                65 + mods
            );
        }
        // horizontal wheel keeps dropping, modified or not
        assert_eq!(mouse_action(Some(Fg::Agent), 66 + 16, true), MouseAction::Drop);
        // buttons 8-11 carry bit 7, which the old mask also leaked
        assert_eq!(mouse_action(Some(Fg::Agent), 128, true), MouseAction::ForwardRaw);
        assert_eq!(mouse_action(Some(Fg::Agent), 128 + 4, true), MouseAction::ForwardRaw);
        // a wheel release is not a left release
        assert_eq!(mouse_action(Some(Fg::Agent), 64, false), MouseAction::ForwardRaw);
    }

    #[test]
    fn button_numbers_decode_the_split_encoding() {
        assert_eq!(button_number(0), 0); // left
        assert_eq!(button_number(32), 0); // left, dragging
        assert_eq!(button_number(16 + 32), 0); // ctrl + left drag
        assert_eq!(button_number(1), 1);
        assert_eq!(button_number(2), 2);
        assert_eq!(button_number(64), 4); // wheel up
        assert_eq!(button_number(68), 4); // shift + wheel up
        assert_eq!(button_number(65), 5);
        assert_eq!(button_number(66), 6);
        assert_eq!(button_number(128), 8);
        assert_eq!(button_number(131), 11);
    }

    #[test]
    fn mouse_parsing() {
        let seq = b"\x1b[<64;10;5M";
        let (btn, x, y, press, len) = parse_mouse(seq, 0).unwrap();
        assert_eq!((btn, x, y, press, len), (64, 10, 5, true, seq.len()));
        assert!(contains_wheel_press(seq));
        assert!(!contains_wheel_press(b"\x1b[<0;3;4M")); // click, not wheel
        assert!(!contains_wheel_press(b"\x1b[<64;10;5m")); // release, not press
        assert!(has_mouse_seq(b"xx\x1b[<0;1;1Myy"));
        assert!(!has_mouse_seq(b"plain text"));
    }

    #[test]
    fn mouse_sequence_split_across_reads_is_reassembled() {
        let mut buf = Vec::new();
        assert_eq!(
            split_mouse(&mut buf, b"pre\x1b[<0;3;2".to_vec()),
            MouseSplit::Passthrough(b"pre".to_vec())
        );
        assert_eq!(buf, b"\x1b[<0;3;2");
        assert_eq!(
            split_mouse(&mut buf, b"Mpost".to_vec()),
            MouseSplit::Passthrough(b"\x1b[<0;3;2Mpost".to_vec())
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn mouse_prefix_can_split_at_escape_and_csi_introducer() {
        let mut buf = Vec::new();
        assert_eq!(split_mouse(&mut buf, b"\x1b".to_vec()), MouseSplit::Pending);
        assert_eq!(split_mouse(&mut buf, b"[".to_vec()), MouseSplit::Pending);
        assert_eq!(
            split_mouse(&mut buf, b"<0;3;2M".to_vec()),
            MouseSplit::Passthrough(b"\x1b[<0;3;2M".to_vec())
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn timed_out_mouse_prefix_is_dropped_but_escape_survives() {
        let mut mouse = b"\x1b[<0;3;2".to_vec();
        assert_eq!(flush_mouse(&mut mouse), None);
        assert!(mouse.is_empty());

        let mut escape = b"\x1b".to_vec();
        assert_eq!(flush_mouse(&mut escape), Some(b"\x1b".to_vec()));
        assert!(escape.is_empty());
    }


    #[test]
    fn sh_quote_escapes_single_quotes() {
        assert_eq!(sh_quote("w9:p1"), "'w9:p1'");
        assert_eq!(sh_quote("a'b"), "'a'\\''b'");
        // overflow-proof mouse params: 11 digits saturate instead of panicking
        let (_, x, _, _, _) = parse_mouse(b"\x1b[<64;99999999999;1M", 0).unwrap();
        assert_eq!(x, u32::MAX);
    }

    #[test]
    fn observe_size_treats_daemon_sizes_as_a_floor() {
        // what the daemon spawns a streamer with for a headless remote: the
        // no-client placeholder rect plus its margin
        let argv: Vec<String> = ["work", "w1:p1", "--cols", "70", "--rows", "31"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let a = parse_args(&argv).unwrap();
        // control has already resized the remote pty to this pane, and release
        // does not shrink it back — observing at 70x31 would stream a crop
        assert_eq!(observe_size_for(&a, (314, 92)), (314, 92));
        // a pane smaller than the remote still gets the daemon's margin
        assert_eq!(observe_size_for(&a, (40, 20)), (70, 31));
        // --dump has no tty: exactly what was asked for
        assert_eq!(observe_size_for(&a, (0, 0)), (70, 31));
    }

    #[test]
    fn a_zero_cap_on_the_cli_is_unset_not_a_zero_request() {
        // herdr rejects a 0-column terminal, so a typo would kill the session
        // twice and strand the pane in "control unavailable"
        let argv: Vec<String> = ["h", "w1:p1", "--max-cols", "0", "--max-rows", "0"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let a = parse_args(&argv).unwrap();
        assert_eq!(a.max_cols, None);
        assert_eq!(a.max_rows, None);

        let argv: Vec<String> =
            ["h", "w1:p1", "--max-cols", "212"].iter().map(|s| s.to_string()).collect();
        assert_eq!(parse_args(&argv).unwrap().max_cols, Some(212));
    }

    #[test]
    fn arg_parsing() {
        let argv: Vec<String> =
            ["work", "w9:p1", "--remote-bin", "/opt/herdr", "--cols", "176", "--rows", "66"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        let a = parse_args(&argv).unwrap();
        assert_eq!(a.ssh_target, "work");
        assert_eq!(a.pane_target, "w9:p1");
        assert_eq!(a.remote_bin.as_deref(), Some("/opt/herdr"));
        assert_eq!((a.cols, a.rows), (176, 66));
        assert!(parse_args(&["onlyone".to_string()]).is_err());
        assert!(parse_args(&["a".into(), "b".into(), "--visibility-file".into(), "x".into()]).is_err());
    }

    // --- birth size trust (#23) ---
    //
    // herdr renders at 80x24 when no client is attached, and chrome only
    // subtracts, so anything larger in either axis is provably a real viewport.

    #[test]
    fn a_placeholder_sized_pane_is_never_trusted() {
        // what a mirror pane is born as when nobody is watching: 80x24 less a
        // 26-col sidebar and the tab bar
        assert!(!size_is_trusted((54, 23)));
        // and the extremes of that layout, in case chrome is configured away
        assert!(!size_is_trusted((80, 24)));
        assert!(!size_is_trusted((80, 23)));
    }

    #[test]
    fn an_ordinary_viewport_is_trusted_immediately() {
        assert!(size_is_trusted((141, 44)));
        assert!(size_is_trusted((133, 47)));
        // one axis is enough: a tall narrow pane cannot come from a 24-row floor
        assert!(size_is_trusted((60, 40)));
        assert!(size_is_trusted((200, 20)));
    }

    #[test]
    fn initial_mode_is_read_only_unless_the_size_vouches_for_itself() {
        // the whole composition: only a trusted size under always_control opens
        // writable, because Control is what can resize the remote
        assert_eq!(initial_mode(true, (141, 44)), Mode::Control);
        assert_eq!(initial_mode(true, (54, 23)), Mode::Observe, "placeholder-sized");
        // without always_control we start read-only regardless, as before
        assert_eq!(initial_mode(false, (141, 44)), Mode::Observe);
        assert_eq!(initial_mode(false, (54, 23)), Mode::Observe);
    }

    #[test]
    fn a_small_client_is_not_trusted_at_birth_and_must_earn_control() {
        // A phone (45x18 -> pane 44x16) and moshi (50x25 -> 49x23) are real
        // viewports, but at birth they are indistinguishable from the placeholder
        // — that is the whole reason shape matching failed. They start read-only
        // and the first resize or keystroke promotes them, rather than being
        // allowed to impose a size we cannot vouch for.
        assert!(!size_is_trusted((44, 16)));
        assert!(!size_is_trusted((49, 23)));
    }

    // --- paste framing -----------------------------------------------------
    // The bugs these pin were all real: a one-shot version dropped the second
    // drop in a read, leaked markers from the tail, and corrupted non-UTF-8.

    const S: &[u8] = b"\x1b[200~";
    const E: &[u8] = b"\x1b[201~";

    fn split(buf: &mut Vec<u8>, chunk: &[u8]) -> PasteSplit {
        split_paste(buf, chunk.to_vec())
    }

    fn seq(parts: &[&[u8]]) -> Vec<u8> {
        parts.concat()
    }

    #[test]
    fn ordinary_typing_passes_straight_through() {
        let mut buf = Vec::new();
        assert_eq!(split(&mut buf, b"a"), PasteSplit::Passthrough(b"a".to_vec()));
        assert_eq!(split(&mut buf, b"\x1b[A"), PasteSplit::Passthrough(b"\x1b[A".to_vec()));
        assert!(buf.is_empty(), "typing must not buffer");
    }

    #[test]
    fn paste_split_across_reads_reassembles() {
        let mut buf = Vec::new();
        assert_eq!(split(&mut buf, &seq(&[S, b"he"])), PasteSplit::Pending);
        assert_eq!(split(&mut buf, b"llo"), PasteSplit::Pending);
        assert_eq!(
            split(&mut buf, E),
            PasteSplit::Complete { before: vec![], body: b"hello".to_vec(), after: vec![] }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn start_marker_split_across_reads_is_not_leaked() {
        // the 6-byte introducer straddling a read boundary must be held, not
        // forwarded in pieces (which would print ESC[20 at the remote)
        let mut buf = Vec::new();
        assert_eq!(split(&mut buf, b"\x1b[20"), PasteSplit::Pending);
        assert_eq!(
            split(&mut buf, &seq(&[b"0~/tmp/a.png", E])),
            PasteSplit::Complete { before: vec![], body: b"/tmp/a.png".to_vec(), after: vec![] }
        );
    }

    #[test]
    fn bytes_around_the_markers_are_preserved() {
        let mut buf = Vec::new();
        assert_eq!(
            split(&mut buf, &seq(&[b"pre", S, b"mid", E, b"post"])),
            PasteSplit::Complete {
                before: b"pre".to_vec(),
                body: b"mid".to_vec(),
                after: b"post".to_vec(),
            }
        );
    }

    #[test]
    fn two_pastes_in_one_read_are_drained_in_order() {
        // the regression: only the first was handled and the rest discarded,
        // so a second drop vanished and its markers reached the remote raw
        let mut buf = Vec::new();
        let PasteSplit::Complete { body, after, .. } =
            split(&mut buf, &seq(&[S, b"one", E, S, b"two", E]))
        else {
            panic!("expected first paste")
        };
        assert_eq!(body, b"one");
        assert_eq!(
            split(&mut buf, &after),
            PasteSplit::Complete { before: vec![], body: b"two".to_vec(), after: vec![] },
            "feeding the tail back must yield the second paste"
        );
    }

    #[test]
    fn keystroke_after_a_paste_survives() {
        let mut buf = Vec::new();
        let PasteSplit::Complete { after, .. } = split(&mut buf, &seq(&[S, b"/tmp/a", E, b"\r"]))
        else {
            panic!("expected paste")
        };
        assert_eq!(after, b"\r", "the trailing keystroke must not be eaten");
    }

    #[test]
    fn unterminated_paste_flushes_at_the_cap_without_duplicating() {
        let mut buf = Vec::new();
        assert_eq!(split(&mut buf, S), PasteSplit::Pending);
        let big = vec![b'x'; MAX_PASTE_BYTES + 1];
        let PasteSplit::Passthrough(out) = split(&mut buf, &big) else {
            panic!("expected a flush")
        };
        assert_eq!(out.len(), S.len() + big.len(), "every byte exactly once");
        assert!(buf.is_empty(), "buffer must not keep a copy");
    }

    #[test]
    fn non_utf8_paste_body_is_forwarded_byte_exact() {
        // the body is only lossily decoded to probe for paths; what gets
        // forwarded must be the original bytes
        let mut buf = Vec::new();
        let PasteSplit::Complete { body, .. } = split(&mut buf, &seq(&[S, b"caf\xe9", E])) else {
            panic!("expected paste")
        };
        assert_eq!(body, b"caf\xe9", "0xE9 must not become U+FFFD");
    }

    #[test]
    fn a_stripped_paste_is_reframed_byte_exact() {
        // what `route_paste_body` sends on: the framing comes off to recognise
        // a drop and has to go back on, or the remote app reads the newlines in
        // a multi-line paste as Enter and submits it a line at a time
        let body = b"first line\nsecond line".to_vec();
        let framed = crate::paste::bracketed_bytes(&body);
        assert_eq!(framed, seq(&[S, &body, E]));

        let mut buf = Vec::new();
        assert_eq!(
            split(&mut buf, &framed),
            PasteSplit::Complete { before: vec![], body, after: vec![] },
            "re-framing must be exactly what the splitter undoes"
        );
    }

    #[test]
    fn target_gone_matches_only_this_pane_being_gone() {
        // the real sentence, captured from herdr against a missing pane
        assert!(target_gone(
            "terminal session observe failed: terminal target w9Z:p99 not found",
            "w9Z:p99"
        ));
        assert!(target_gone(
            "terminal session control failed: terminal target w1:p1 not found",
            "w1:p1"
        ));

        // the false positive that matters most: herdr absent on the remote.
        // The auto-resolver execs `$(command -v herdr || echo ~/.local/bin/herdr)`,
        // so a host without herdr fails with a shell not-found — a different
        // problem that a slow rung would wrongly paper over.
        assert!(!target_gone("sh: 1: exec: /home/u/.local/bin/herdr: not found", "w9Z:p99"));

        // a target that merely shares our prefix: p1 and p10 are both real ids
        assert!(!target_gone(
            "terminal session observe failed: terminal target w1:p10 not found",
            "w1:p1"
        ));
        // ...and another pane's disappearance is not ours
        assert!(!target_gone(
            "terminal session observe failed: terminal target w1:p4 not found",
            "w9Z:p99"
        ));

        // ordinary transients stay on the fast ladder
        assert!(!target_gone("api timeout: session.snapshot", "w9Z:p99"));
        assert!(!target_gone("ssh timeout", "w9Z:p99"));
        assert!(!target_gone("", "w9Z:p99"));
        // an empty target must not turn `contains` into "matches everything"
        assert!(!target_gone("terminal target w1:p1 not found", ""));
    }

    #[test]
    fn a_gone_target_slows_down_without_consuming_the_ladder() {
        // the fix: 10s forever becomes one attempt a minute
        assert_eq!(reconnect_delay(true, 0), (GONE_BACKOFF_MS, 0));
        assert_eq!(reconnect_delay(true, 3), (GONE_BACKOFF_MS, 3));

        // the fast ladder is unchanged and still clamps at its last rung
        assert_eq!(reconnect_delay(false, 0), (1000, 1));
        assert_eq!(reconnect_delay(false, 1), (2000, 2));
        assert_eq!(reconnect_delay(false, 3), (10000, 4));
        assert_eq!(reconnect_delay(false, 99), (10000, 100));

        // a gone spell must not burn rungs: a transient afterwards resumes
        // where the ladder was, rather than restarting at 1s
        let (_, idx) = reconnect_delay(false, 0);
        let (_, idx) = reconnect_delay(true, idx);
        assert_eq!(reconnect_delay(false, idx), (2000, 2));
    }

    #[test]
    fn disconnected_host_health_admits_no_pane_attempts_until_daemon_success() {
        let state = std::env::temp_dir().join(format!(
            "mirror-host-admission-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state);
        std::fs::create_dir_all(&state).unwrap();
        crate::state::publish_host_health(
            &state,
            "cargo",
            &crate::state::HostHealth {
                summary: "disconnected (hung transport)".into(),
                at_iso: "2026-09-14T10:00:00Z".into(),
                next_retry_unix: Some(crate::state::unix_now() + 300.0),
            },
        );
        let mut new_attempts = 0;
        for _ in 0..600 {
            if host_admission_open(&state, Some("cargo")) {
                new_attempts += 1;
            }
        }
        assert_eq!(new_attempts, 0, "local retries admitted new transport attempts");

        crate::state::publish_host_health(
            &state,
            "cargo",
            &crate::state::HostHealth {
                summary: "connected and synced".into(),
                at_iso: "2026-09-14T10:05:00Z".into(),
                next_retry_unix: None,
            },
        );
        assert!(host_admission_open(&state, Some("cargo")));
        let _ = std::fs::remove_dir_all(state);
    }

    /// The rate limit that has to hold on the transport the wedges happen on.
    ///
    /// `.26` took the foreground poll off the API forward and onto an ssh
    /// session per pane; rolled back, because on the socket-transport guests
    /// that ran the poll at its full cadence straight into the exec service.
    /// Back on the forward, the only thing bounding the idle cost is this
    /// spacing, so it is asserted rather than described.
    #[test]
    fn a_forced_poll_over_the_api_forward_still_keeps_its_distance() {
        // ssh and docker: unchanged. Lazy by default, immediate when forced.
        assert_eq!(fg_poll_spacing(false, false), FG_POLL_INTERVAL);
        assert_eq!(fg_poll_spacing(true, false), Duration::ZERO);

        // the API forward: one floor, and `force` does not lift it
        assert_eq!(fg_poll_spacing(false, true), API_FG_POLL_FLOOR);
        assert_eq!(fg_poll_spacing(true, true), API_FG_POLL_FLOOR);

        // and the floor is worth what it claims. A poll there costs one
        // connection (see `foreground::api_poll`), so this is a hard ceiling of
        // six per minute per pane — the idle cost measured before `.26`, when
        // the same 20 s tick spent two connections a poll.
        let ceiling_per_minute = 60.0 / API_FG_POLL_FLOOR.as_secs_f64();
        let pre_26_idle_per_minute = (60.0 / OUTPUT_HEALTH_INTERVAL.as_secs_f64()) * 2.0;
        assert!(
            ceiling_per_minute <= pre_26_idle_per_minute,
            "worst case {ceiling_per_minute}/min exceeds the pre-.26 idle \
             {pre_26_idle_per_minute}/min"
        );

        // at rest nothing else asks, so the health tick is the real cadence:
        // one poll and one connection per tick, half of what it used to be
        assert!(OUTPUT_HEALTH_INTERVAL >= API_FG_POLL_FLOOR);
    }

    fn test_args(container: Option<ContainerArg>) -> Args {
        Args {
            ssh_target: "omnidev-greenroom".into(),
            pane_target: "w1:p3".into(),
            remote_bin: Some("/opt/herdr".into()),
            cols: 240,
            rows: 72,
            dump: false,
            session: Some("default".into()),
            control_idle_secs: 3600,
            always_control: true,
            max_cols: None,
            max_rows: None,
            ctl_path: None,
            api_socket: None,
            host_name: None,
            container,
        }
    }

    /// A suspension's reap may need a slow fresh transport (VM hosts measure
    /// 4–6 s), so its per-client bound is its own, longer than the exit path's.
    #[tokio::test]
    async fn a_suspend_time_kill_outlasts_a_transport_the_exit_bound_gives_up_on() {
        assert!(SUSPEND_REMOTE_KILL_TIMEOUT >= Duration::from_secs(10));
        assert_eq!(REMOTE_KILL_TIMEOUT, Duration::from_secs(2));
        let dir = std::env::temp_dir().join(format!("hm-slowkill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let docker = dir.join("docker");
        // `ps` answers at once; the kill itself takes a second, like a slow hop
        std::fs::write(
            &docker,
            "#!/bin/sh\ncase \"$1\" in ps) echo c1 ;; exec) sleep 1 ;; esac\nexit 0\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let args = test_args(Some(ContainerArg {
            kind: crate::config::HostKind::DockerContainer("c".into()),
            docker_bin: docker.display().to_string(),
        }));
        assert!(!kill_remote_client_within(&args, 4242, Duration::from_millis(300)).await);
        assert!(kill_remote_client_within(&args, 4242, Duration::from_secs(5)).await);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The pid line is the whole handle on the remote client. It has to be
    /// recognisable in a stream that also carries herdr's JSON frames, and
    /// nothing else may be mistaken for it.
    #[test]
    fn the_remote_pid_line_is_read_and_nothing_else_is() {
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid 4703"), Some(4703));
        // ssh can deliver it with the line ending still attached
        assert_eq!(remote_pid_from_line("  herdr-mirror-remote-pid 4703\r"), Some(4703));

        // a frame, a banner, a truncated line, and a pid we must never signal
        assert_eq!(remote_pid_from_line(r#"{"type":"terminal.frame","seq":1}"#), None);
        assert_eq!(remote_pid_from_line("Welcome to Ubuntu 24.04"), None);
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid"), None);
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid nope"), None);
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid 4703 extra"), None);
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid 1"), None);
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid 0"), None);
        assert_eq!(remote_pid_from_line("herdr-mirror-remote-pid -1"), None);
    }

    /// `$$` must be the pid of the process that ends up attached: the wrapper
    /// announces it and then `exec`s the client over itself.
    #[test]
    fn the_remote_script_announces_the_pid_it_then_becomes() {
        let script = remote_session_script(&test_args(None), Mode::Control, 100, 40);

        assert_eq!(
            script,
            "echo herdr-mirror-remote-pid $$; \
exec /opt/herdr --session 'default' terminal session control 'w1:p3' --cols 100 --rows 40"
        );
        // one process, so the announced pid is the one that survives
        assert_eq!(script.matches("exec ").count(), 1);
    }

    /// The remote login shell may be fish or csh, where `$$` is not a pid and
    /// `$(…)` does not parse. Everything unportable has to sit inside single
    /// quotes that only the `sh` we name ever opens.
    #[test]
    fn unportable_syntax_is_hidden_from_the_remote_login_shell() {
        let wrapped = sh_wrapped("echo herdr-mirror-remote-pid $$; exec herdr terminal session");

        assert_eq!(
            wrapped,
            "exec sh -c 'echo herdr-mirror-remote-pid $$; exec herdr terminal session'"
        );
        // a quote in the payload cannot break out of the wrapper
        assert_eq!(sh_wrapped("a 'b' c"), r#"exec sh -c 'a '\''b'\'' c'"#);
    }

    /// A recorded pid can be hours old and remote pids get reused, and the same
    /// host runs the herdr SERVER. The kill must identify its target first.
    #[test]
    fn a_remote_kill_identifies_its_target_before_signalling_it() {
        let script = remote_kill_script(4703);

        assert!(script.contains("ps -o args= -p"));
        assert!(script.contains(r#"*"terminal session"*"#));
        assert!(script.contains("kill -TERM"));
        // no unguarded kill except on a host with no ps at all
        assert_eq!(script.matches("kill -TERM").count(), 2);
        assert!(script.contains("command -v ps"));
    }

    /// Evicting a client is only ever right for one we created ourselves.
    #[test]
    fn only_our_own_orphan_is_cleared_off_a_refused_control_attach() {
        let refused = "terminal attach failed: terminal 7 already has an attached client; retry";

        assert_eq!(attach_conflict_pid(refused, Some(4703), false), Some(4703));
        // a person on the other end, with nothing of ours to blame
        assert_eq!(attach_conflict_pid(refused, None, false), None);
        // once only: two streamers must not fight over one terminal
        assert_eq!(attach_conflict_pid(refused, Some(4703), true), None);
        // any other failure keeps the ordinary control fallback
        assert_eq!(attach_conflict_pid("ssh: connect timed out", Some(4703), false), None);
        assert_eq!(attach_conflict_pid("", Some(4703), false), None);
        // never a pid that cannot be one of ours
        assert_eq!(attach_conflict_pid(refused, Some(1), false), None);
    }

    /// The queue is a retry list, not a leak: bounded, de-duplicated, and
    /// oldest-first so the pid most likely to still be attached is kept.
    #[test]
    fn queued_remote_kills_are_bounded_and_deduplicated() {
        let mut pending: Vec<i32> = Vec::new();
        let queue = |pending: &mut Vec<i32>, pid: i32| {
            if pid <= 1 || pending.contains(&pid) {
                return;
            }
            if pending.len() >= MAX_PENDING_REMOTE_KILLS {
                pending.remove(0);
            }
            pending.push(pid);
        };
        for pid in 100..100 + MAX_PENDING_REMOTE_KILLS as i32 + 2 {
            queue(&mut pending, pid);
        }
        queue(&mut pending, 105);
        queue(&mut pending, 1);

        assert_eq!(pending.len(), MAX_PENDING_REMOTE_KILLS);
        assert_eq!(pending.first().copied(), Some(102));
        assert_eq!(pending.iter().filter(|p| **p == 105).count(), 1);
        assert!(!pending.contains(&1));
    }

    #[test]
    fn failed_remote_cleanup_keeps_the_only_owned_client_handle() {
        let state_dir = std::env::temp_dir().join(format!(
            "herdr-mirror-unresolved-client-{}",
            std::process::id()
        ));
        let args = test_args(None);
        preserve_unresolved_remote_client(&args, &state_dir, 4703, false);
        assert_eq!(
            crate::util::take_remote_client(
                &state_dir,
                &args.ssh_target,
                &args.pane_target,
            ),
            Some(4703)
        );
        preserve_unresolved_remote_client(&args, &state_dir, 4703, true);
        assert_eq!(
            crate::util::take_remote_client(
                &state_dir,
                &args.ssh_target,
                &args.pane_target,
            ),
            None
        );
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[test]
    fn a_streamer_that_ends_cleanly_is_not_respawned() {
        // exit 0 is the remote pane closing or the user quitting: replacing it
        // would resurrect a pane that was deliberately ended
        assert_eq!(respawn_decision(true, Duration::from_secs(1), 0), None);
        assert_eq!(respawn_decision(true, Duration::from_secs(9999), 4), None);
    }

    #[test]
    fn a_crash_on_a_fresh_budget_climbs_the_ladder() {
        let ladder: Vec<u64> = (0..STREAMER_RESPAWN_LIMIT)
            .map(|attempts| {
                let d = respawn_decision(false, Duration::from_secs(1), attempts)
                    .expect("budget not spent");
                assert_eq!(d.attempt, attempts + 1);
                d.delay.as_secs()
            })
            .collect();

        assert_eq!(ladder, vec![1, 2, 4, 8, 8]);
    }

    #[test]
    fn a_crash_after_a_long_healthy_run_gets_a_fresh_budget() {
        // the case this exists for: a stream that worked all afternoon and
        // then lost its transport must not inherit a rung from this morning
        let d = respawn_decision(false, STREAMER_HEALTHY_UPTIME, STREAMER_RESPAWN_LIMIT)
            .expect("a healthy run resets the counter");

        assert_eq!(d, Respawn { delay: Duration::from_secs(1), attempt: 1 });
        // one second short of healthy still counts against the budget
        assert!(respawn_decision(
            false,
            STREAMER_HEALTHY_UPTIME - Duration::from_secs(1),
            STREAMER_RESPAWN_LIMIT,
        )
        .is_none());
    }

    #[test]
    fn an_exhausted_budget_gives_up_and_lets_the_pane_close() {
        assert!(respawn_decision(false, Duration::from_secs(1), STREAMER_RESPAWN_LIMIT).is_none());
        assert!(respawn_decision(false, Duration::ZERO, STREAMER_RESPAWN_LIMIT + 7).is_none());
    }

    fn pause_test_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "herdr-mirror-pause-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn explicit_pause_blocks_stream_transport_but_a_daemon_crash_does_not() {
        let state_dir = pause_test_dir("transport-gate");
        std::fs::create_dir_all(&state_dir).unwrap();

        assert!(stream_may_connect(&state_dir), "no marker is an ordinary daemon crash");
        std::fs::write(crate::daemon::pause_path(&state_dir), b"paused\n").unwrap();
        assert!(!stream_may_connect(&state_dir), "explicit pause must block SSH and polls");

        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn a_paused_supervisor_holds_until_start_clears_the_marker() {
        let state_dir = pause_test_dir("hold");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::write(crate::daemon::pause_path(&state_dir), b"paused\n").unwrap();
        let waiting_dir = state_dir.clone();
        let waiter = tokio::spawn(async move {
            wait_for_stream_resume_with_delay(&waiting_dir, false, Duration::ZERO).await
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiter.is_finished(), "no child may respawn while pause is held");
        std::fs::remove_file(crate::daemon::pause_path(&state_dir)).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("resume noticed")
            .expect("wait task"));

        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn a_child_pause_ack_survives_a_fast_daemon_reload() {
        let state_dir = pause_test_dir("ack");
        std::fs::create_dir_all(&state_dir).unwrap();
        mark_stream_pause(&state_dir, "w8H:p2");

        // The global marker may already be gone when the child's bounded
        // remote-client cleanup completes. Its local acknowledgement still
        // routes the clean exit into resume rather than pane closure.
        let pause_observed = take_stream_pause(&state_dir, "w8H:p2");
        assert!(pause_observed);
        assert!(wait_for_stream_resume_with_delay(
            &state_dir,
            pause_observed,
            Duration::ZERO
        )
        .await);
        assert!(!take_stream_pause(&state_dir, "w8H:p2"));

        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[test]
    fn explicit_resume_is_stable_and_spread_without_global_state() {
        let first = stream_resume_delay("w8H:p2");
        assert_eq!(first, stream_resume_delay("w8H:p2"));
        let slots: std::collections::HashSet<_> =
            (1..=40).map(|n| stream_resume_delay(&format!("w8H:p{n}"))).collect();
        assert!(slots.len() >= 30, "pane identities should not resume in one herd");
        assert!(slots.iter().all(|delay| *delay < Duration::from_secs(8)));
    }

    // Exercise the real input routing: a release and the next press can share
    // one stdin read. A local sink stands in for the session; no SSH is used.
    fn closed_admission_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "hmv-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // A pending health retry closes admission for host "h", which keeps
        // every connect and remote-client reap in these tests local.
        crate::state::publish_host_health(
            &dir,
            "h",
            &crate::state::HostHealth {
                summary: "test".into(),
                at_iso: "test".into(),
                next_retry_unix: Some(1e12),
            },
        );
        dir
    }

    fn local_session(gen: u64, sink: &std::path::Path) -> (Session, tokio::process::Child) {
        let mut command = tokio::process::Command::new("sh");
        command
            .args(["-c", &format!("cat > '{}'", sink.display())])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .kill_on_drop(true);
        isolate_session_process_group(&mut command);
        let mut child = command.spawn().unwrap();
        let session = Session {
            gen,
            mode: Mode::Control,
            process_group: child.id().unwrap() as i32,
            remote_pid: None,
            stdin: child.stdin.take().unwrap(),
        };
        (session, child)
    }

    #[tokio::test]
    async fn a_sidebar_copy_stands_aside_for_a_view_and_takes_the_terminal_back() {
        let dir = closed_admission_dir("side");
        let args = parse_args(&["t".into(), "w1:p1".into(), "--host-name".into(), "h".into()])
            .unwrap();
        let (tx, _rx) = mpsc::channel(16);
        let mut app = test_app(args, dir.clone(), false, tx);
        app.local_pane_id = Some("wS:p1".into());
        let sink = dir.join("session-input");
        let (session, mut child) = local_session(1, &sink);
        app.session = Some(session);
        app.mode = Mode::Control;
        app.health_at = Some(Instant::now() + OUTPUT_HEALTH_INTERVAL);

        // no claim: nothing changes
        app.poll_view_claim().await;
        assert!(app.claim.is_none());
        assert!(app.session.is_some());

        // a view claims the pane: the session is released gracefully and
        // dropped, the release is announced, and nothing reconnects
        let (guard, listener) =
            crate::view::create_claim(&dir, "h--w1_p1", "wM:p2", None).unwrap();
        app.poll_view_claim().await;
        assert_eq!(app.claim.as_ref().map(|c| c.tile_pane_id.as_str()), Some("wM:p2"));
        assert!(app.session.is_none());
        assert!(app.reconnect_at.is_none());
        assert!(app.health_at.is_none());
        assert!(crate::view::is_released(&dir, "h--w1_p1"));
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        let written = std::fs::read_to_string(&sink).unwrap();
        assert!(written.contains("terminal.release"), "{written:?}");
        // connect is refused while claimed
        app.connect(Mode::Control).await;
        assert!(app.session.is_none());

        // input typed (or prompted) into the sidebar copy reaches the view;
        // focus reports do not
        let (itx, mut irx) = mpsc::channel::<Vec<u8>>(8);
        crate::view::spawn_input_listener(listener, move |bytes| {
            let itx = itx.clone();
            tokio::spawn(async move { itx.send(bytes).await.is_ok() })
        })
        .unwrap();
        app.forward_to_view(b"\x1b[Iship it\r".to_vec()).await;
        let got = tokio::time::timeout(Duration::from_secs(2), irx.recv()).await.unwrap().unwrap();
        assert_eq!(got, b"ship it\r".to_vec());

        // the view ends: the claim is gone, and the sidebar copy reconnects
        // (here into the closed admission gate, so it only schedules)
        drop(guard);
        app.poll_view_claim().await;
        assert!(app.claim.is_none());
        assert!(!crate::view::is_released(&dir, "h--w1_p1"));
        assert!(app.reconnect_at.is_some());
        assert!(app.health_at.is_some());
        let _ = std::fs::remove_dir_all(dir);
    }

    fn show(dir: &std::path::Path, secs: u64, panes: &[&str]) {
        crate::visibility::publish(
            dir,
            &crate::visibility::Visible {
                pid: std::process::id() as i32,
                idle_release_secs: secs,
                workspace_id: Some("wS".into()),
                tab_id: Some("wS:t1".into()),
                panes: panes.iter().map(|p| p.to_string()).collect(),
            },
        );
    }

    fn idle_app(tag: &str) -> (App, std::path::PathBuf) {
        let dir = closed_admission_dir(tag);
        let args = parse_args(&["t".into(), "w1:p1".into(), "--host-name".into(), "h".into()])
            .unwrap();
        let (tx, _rx) = mpsc::channel(16);
        let mut app = test_app(args, dir.clone(), false, tx);
        app.local_pane_id = Some("wS:p1".into());
        (app, dir)
    }

    #[tokio::test]
    async fn an_unviewed_sidebar_copy_releases_after_the_grace_and_resumes_when_shown() {
        let (mut app, dir) = idle_app("idle");
        let sink = dir.join("session-input");
        let (session, mut child) = local_session(1, &sink);
        app.session = Some(session);
        app.mode = Mode::Control;
        app.health_at = Some(Instant::now() + OUTPUT_HEALTH_INTERVAL);

        // on screen: never released, however long ago anything else happened
        show(&dir, 120, &["wS:p1"]);
        app.last_seen = Instant::now() - Duration::from_secs(600);
        app.poll_idle_release().await;
        assert!(!app.idle_released && app.session.is_some());

        // off screen: the grace runs from the last moment it was on screen
        show(&dir, 120, &["wS:p9"]);
        app.poll_idle_release().await;
        assert!(!app.idle_released, "inside the grace");
        app.last_seen = Instant::now() - Duration::from_secs(121);
        app.poll_idle_release().await;
        assert!(app.idle_released);
        assert!(app.session.is_none());
        assert!(app.reconnect_at.is_none());
        assert!(app.health_at.is_none());
        assert!(crate::visibility::is_idle_released(&dir, "wS:p1"));
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        let written = std::fs::read_to_string(&sink).unwrap();
        assert!(written.contains("terminal.release"), "{written:?}");
        // nothing reattaches while idle-released
        app.connect(Mode::Control).await;
        assert!(app.session.is_none() && app.reconnect_at.is_none());
        app.poll_idle_release().await;
        assert!(app.idle_released, "still off screen");

        // its tab comes on screen: reattach (here into the closed admission
        // gate, so it only schedules)
        show(&dir, 120, &["wS:p1", "wS:p9"]);
        app.poll_idle_release().await;
        assert!(!app.idle_released);
        assert!(app.reconnect_at.is_some());
        assert!(app.health_at.is_some());
        assert!(!crate::visibility::is_idle_released(&dir, "wS:p1"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn focus_or_input_resumes_an_idle_pane_and_the_input_is_kept_for_the_new_session() {
        let (mut app, dir) = idle_app("idlein");
        show(&dir, 120, &["wS:p9"]);
        app.enter_idle_release().await;
        assert!(app.idle_released);

        // a lone focus-out is the pane being left: nothing happens
        app.resume_from_input(b"\x1b[O".to_vec()).await;
        assert!(app.idle_released);
        // focus-in resumes, without the daemon
        app.resume_from_input(b"\x1b[I".to_vec()).await;
        assert!(!app.idle_released);
        assert!(app.reconnect_at.is_some() || app.switch_at.is_some());
        assert!(app.pending_input.is_empty(), "focus reports are not input");

        // typed or prompted input resumes and is queued for the new session
        app.enter_idle_release().await;
        assert!(app.idle_released && app.pending_input.is_empty());
        app.resume_from_input(b"\x1b[Iship it\r".to_vec()).await;
        assert!(!app.idle_released);
        assert_eq!(app.pending_input.concat(), b"ship it\r".to_vec());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn disabled_or_unpublished_idle_release_keeps_streaming_and_a_view_takes_over() {
        let (mut app, dir) = idle_app("idleoff");
        app.last_seen = Instant::now() - Duration::from_secs(10_000);
        // no daemon has published anything: no release
        app.poll_idle_release().await;
        assert!(!app.idle_released);
        // disabled
        show(&dir, 0, &[]);
        app.poll_idle_release().await;
        assert!(!app.idle_released);
        // switching it off resumes a released pane
        app.enter_idle_release().await;
        app.poll_idle_release().await;
        assert!(!app.idle_released);

        // a view claiming an idle-released pane supersedes the idle release
        show(&dir, 120, &[]);
        app.last_seen = Instant::now() - Duration::from_secs(10_000);
        app.poll_idle_release().await;
        assert!(app.idle_released);
        let (guard, _listener) =
            crate::view::create_claim(&dir, "h--w1_p1", "wM:p2", None).unwrap();
        app.poll_view_claim().await;
        assert!(app.claim.is_some() && !app.idle_released);
        assert!(crate::view::is_released(&dir, "h--w1_p1"));
        assert!(!crate::visibility::is_idle_released(&dir, "wS:p1"));
        // a claimed pane is not the idle logic's business
        app.poll_idle_release().await;
        assert!(!app.idle_released);
        drop(guard);
        app.poll_view_claim().await;
        assert!(app.claim.is_none() && !app.idle_released, "the grace restarts after a view");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A roll (suspend + show) or any daemon recreation gives the sidebar copy
    /// a NEW local pane id. The view resolved the old one; its claim must
    /// still bind the new copy, because both know the same host + remote pane.
    #[tokio::test]
    async fn a_view_survives_its_sidebar_copy_being_recreated_under_a_new_local_pane() {
        let dir = closed_admission_dir("recreate");
        let args = || {
            parse_args(&["t".into(), "w1:p1".into(), "--host-name".into(), "h".into()]).unwrap()
        };
        assert_eq!(crate::view::claim_key_for(&args()), crate::view::claim_key("h", "w1:p1"));
        // the view claimed while the sidebar copy was wHC:p2
        let key = crate::view::claim_key("h", "w1:p1");
        let (guard, listener) =
            crate::view::create_claim(&dir, &key, "wM:p2", Some("/x.sock")).unwrap();
        let (itx, mut irx) = mpsc::channel::<Vec<u8>>(8);
        crate::view::spawn_input_listener(listener, move |bytes| {
            let itx = itx.clone();
            tokio::spawn(async move { itx.send(bytes).await.is_ok() })
        })
        .unwrap();
        let (tx, _rx) = mpsc::channel(16);
        let mut old = test_app(args(), dir.clone(), false, tx.clone());
        old.local_pane_id = Some("wHC:p2".into());
        old.poll_view_claim().await;
        assert!(old.claim.is_some());
        drop(old);
        crate::view::clear_released(&dir, &key); // as if the old copy never said so

        // recreated as wHK:p2: it sees the claim, stands aside, says so
        let (tx, _rx) = mpsc::channel(16);
        let mut new = test_app(args(), dir.clone(), false, tx);
        new.local_pane_id = Some("wHK:p2".into());
        new.poll_view_claim().await;
        let claim = new.claim.clone().expect("claimed across the recreation");
        assert_eq!(claim.tile_pane_id, "wM:p2");
        assert_eq!(claim.herdr_socket.as_deref(), Some("/x.sock"), "focus redirect target kept");
        assert!(crate::view::is_released(&dir, &key));
        assert!(new.session.is_none() && new.reconnect_at.is_none());
        // input (e.g. `herdr agent prompt`) still reaches the view
        new.forward_to_view(b"ship it\r".to_vec()).await;
        let got = tokio::time::timeout(Duration::from_secs(2), irx.recv()).await.unwrap().unwrap();
        assert_eq!(got, b"ship it\r".to_vec());
        // a claimed copy never idle-resumes into the tile's terminal
        show(&dir, 120, &["wHK:p2"]);
        new.poll_idle_release().await;
        assert!(new.session.is_none() && new.reconnect_at.is_none());

        // the view ends: claim and released marker go together
        drop(guard);
        assert!(!crate::view::is_released(&dir, &key));
        new.poll_view_claim().await;
        assert!(new.claim.is_none() && new.reconnect_at.is_some());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_view_keeps_retrying_while_the_sidebar_copy_still_holds_the_terminal() {
        let dir = closed_admission_dir("vconf");
        let args = parse_args(&["t".into(), "w1:p1".into(), "--always-control".into()]).unwrap();
        let (tx, _rx) = mpsc::channel(16);
        let mut app = test_app(args, dir.clone(), false, tx);
        app.is_view = true;
        let sink = dir.join("s");
        for gen in 1..=3 {
            let (session, _child) = local_session(gen, &sink);
            app.session = Some(session);
            app.mode = Mode::Control;
            app.handle_exit(
                gen,
                Mode::Control,
                "terminal t9 already has an attached client; retry".into(),
                Duration::from_millis(50),
            );
            // never falls back to read-only over the sidebar copy's release
            assert_eq!(app.control_failures, 0);
            assert!(!app.control_sticky);
            assert!(app.switching_to.is_none());
            let (at, mode) = app.reconnect_at.expect("retry scheduled");
            assert_eq!(mode, Mode::Control);
            assert!(at <= Instant::now() + crate::view::VIEW_CONFLICT_RETRY);
        }
        // the same refusal in a sidebar copy keeps the old policy
        app.is_view = false;
        for gen in 4..=5 {
            let (session, _child) = local_session(gen, &sink);
            app.session = Some(session);
            app.mode = Mode::Control;
            app.handle_exit(
                gen,
                Mode::Control,
                "terminal t9 already has an attached client; retry".into(),
                Duration::from_millis(50),
            );
        }
        assert!(app.control_sticky, "sidebar copy falls back to observe");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_view_whose_remote_pane_is_gone_stops_retrying_it() {
        let dir = closed_admission_dir("vgone");
        let args = parse_args(&["t".into(), "w2:p1".into(), "--always-control".into()]).unwrap();
        let (tx, _rx) = mpsc::channel(16);
        let mut app = test_app(args, dir.clone(), false, tx);
        let sink = dir.join("s");
        let gone = "terminal session control failed: terminal target w2:p1 not found";
        // a sidebar copy keeps retrying its pane
        let (session, _child) = local_session(1, &sink);
        app.session = Some(session);
        app.mode = Mode::Control;
        app.handle_exit(1, Mode::Control, gone.into(), Duration::from_millis(50));
        assert!(!app.view_target_gone);
        assert!(app.reconnect_at.is_some());
        // a view hands back to re-resolve, in control mode too
        app.is_view = true;
        let (session, _child) = local_session(2, &sink);
        app.session = Some(session);
        app.handle_exit(2, Mode::Control, gone.into(), Duration::from_millis(50));
        assert!(app.view_target_gone);
        assert!(app.reconnect_at.is_none());
        // another pane's absence is not ours
        app.view_target_gone = false;
        app.schedule_reconnect(Mode::Control, "terminal target w2:p10 not found");
        assert!(!app.view_target_gone);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// An `App` with no transport, for exercising its state machine directly.
    fn test_app(
        args: Args,
        state_dir: std::path::PathBuf,
        tty: bool,
        tx: mpsc::Sender<Msg>,
    ) -> App {
        let fg_poller = crate::foreground::shared(crate::poll_channel::Transport::Ssh {
            target: args.ssh_target.clone(),
            ctl_path: None,
        });
    App {
        args,
        state_dir: state_dir.clone(),
        tty,
        grid: Grid::new(),
        renderer: Renderer::new(),
        tx,
        mode: Mode::Observe,
        switching_to: None,
        switch_at: None,
        session: None,
        next_gen: 0,
        backoff_idx: 0,
        reconnect_at: None,
        pending_remote_kills: Vec::new(),
        attach_conflict_retried: false,
        control_failures: 0,
        control_sticky: false,
        pending_input: Vec::new(),
        last_input: Instant::now(),
        hint_clear_at: None,
        predict: Predictor::new(),
        remote_fg: None,
        select: Select::new(),
        last_select_rows: None,
        fg_poll_at: None,
        fg_poll_gate: crate::foreground::PollGate::new(),
        fg_poller,
        last_frame_at: Instant::now(),
        remote_revision: None,
        remote_advanced_at: None,
        health_at: None,
        settle_at: None,
        mouse_grabbed: tty, // startup wrote ?1002h when we're a tty
        // startup leaves the pane in normal cursor mode; the first classification
        // moves it if the remote turns out to be a TUI
        app_cursor_keys: false,
        paste_inflight: false,
        paste_buf: Vec::new(),
        mouse_buf: Vec::new(),
        mouse_flush_at: None,
        paste_queue: Vec::new(),
        paste_original: None,
        is_view: false,
        view_target_gone: false,
        record_target: "unused".into(),
        local_pane_id: None,
        claim: None,
        forward: crate::view::Forwarder::default(),
        idle_released: false,
        last_seen: Instant::now(),
        visibility: crate::visibility::Reader::default(),
    }
    }

    async fn selection_followed_by_press(next_drag: bool) {
        let args = parse_args(&["unused".into(), "p1".into()]).unwrap();
        let tty = true;
        let (tx, _rx) = mpsc::channel(256);
        let state_dir = std::env::temp_dir().join(format!(
            "herdr-mirror-selection-{}-{next_drag}",
            std::process::id()
        ));
        let mut app = test_app(args, state_dir.clone(), tty, tx);
        let mut child = tokio::process::Command::new("cat")
            .stdin(Stdio::piped()).stdout(Stdio::null()).kill_on_drop(true)
            .spawn().unwrap();
        app.session = Some(Session {
            gen: 1, mode: Mode::Control, process_group: child.id().unwrap() as i32,
            remote_pid: None,
            stdin: child.stdin.take().unwrap(),
        });
        app.mode = Mode::Control;
        app.remote_fg = Some(Fg::Mouse);
        // Keep foreground polling local to this fixture: suppress SSH probes.
        app.fg_poll_at = Some(Instant::now());
        // Blank cells exercise copy handling without writing to the clipboard.
        app.grid.resize(80, 24);
        app.handle_stdin(
            b"\x1b[<0;1;1M\x1b[<32;4;1M\x1b[<0;4;1m\x1b[<0;1;2M".to_vec(),
        ).await;
        let result = app.select.release(
            if next_drag { (1, 3) } else { (1, 0) }, b"release",
        );
        drop(app);
        child.kill().await.unwrap();
        let _ = child.wait().await;
        let _ = std::fs::remove_dir_all(&state_dir);
        if next_drag {
            assert_eq!(result, Released::Selection(((1, 0), (1, 3))));
        } else {
            assert_eq!(result, Released::Click(b"\x1b[<0;1;2Mrelease".to_vec()));
        }
    }

    #[tokio::test]
    async fn copying_selection_preserves_next_click_in_same_read() {
        selection_followed_by_press(false).await;
    }

    #[tokio::test]
    async fn copying_selection_preserves_next_drag_in_same_read() {
        selection_followed_by_press(true).await;
    }

}
