// herdr-mirror view: show a mirrored remote pane in another local pane.
//
//   herdr-mirror view <agent-name | local-pane-id>
//
// Run inside a local Herdr pane (a Work Matrix tile). The target is a mirrored
// agent's "sidebar copy": the local pane whose supervisor streams one remote
// pane. The remote Herdr admits one attached client per terminal, so the two
// cannot both stream; instead the tile takes the stream and the sidebar copy
// stands aside while keeping its process — and with it the local agent
// identity Herdr ties to that process — alive.
//
// The handshake is one claim per mirrored REMOTE pane, under the state dir:
//
//   view-claims/<key>.json      who holds the view (tile pane, pid, socket)
//   view-claims/<key>.lock      flock held by the view for its whole life
//   view-claims/<key>.sock      the view's input socket
//   view-claims/<key>.released  the sidebar's "I let go of the terminal"
//
// <key> is `claim_key(host, remote pane)`: the identity the sidebar copy and
// the view both know from the same streamer argv. Not the sidebar copy's local
// pane id — a roll (suspend + show) or any recreation gives the sidebar copy a
// new local pane, and a claim keyed by the old one would leave the new copy
// unclaimed, fighting the tile for the terminal.
//
// The lock is what makes a claim live: the kernel drops it with the process,
// so a SIGKILLed view (or a recycled pid) can never pin a sidebar copy.
//
// A remote layout change can move the agent to another remote pane. When the
// view's pane is reported gone, the view drops its claim, resolves its target
// again (as at startup) and moves to the new pane under that pane's claim; if
// the name resolves nowhere yet, it keeps retrying the resolution.

use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

use crate::util::{err, pid_alive, sane_component, Result};

/// How often a sidebar copy looks for a claim on itself (and for its end).
pub(crate) const VIEW_CLAIM_POLL_INTERVAL: Duration = Duration::from_millis(300);

/// How long a view waits for the sidebar copy to release the remote terminal
/// before trying to attach anyway (attach conflicts are retried after that).
const RELEASE_WAIT: Duration = Duration::from_secs(10);
const RELEASE_POLL: Duration = Duration::from_millis(100);

/// Delay between view attach attempts refused because the sidebar copy's
/// client still holds the terminal.
pub(crate) const VIEW_CONFLICT_RETRY: Duration = Duration::from_millis(500);

const FORWARD_CONNECT_TIMEOUT: Duration = Duration::from_millis(300);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewClaim {
    /// local Herdr pane showing the stream (the tile)
    pub tile_pane_id: String,
    /// pid of the `herdr-mirror view` process
    pub pid: i32,
    /// unix socket the sidebar copy forwards its pty input to
    pub socket: String,
    /// local Herdr API socket the view saw, for focus redirects
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub herdr_socket: Option<String>,
    /// unix seconds
    pub started: f64,
}

fn claims_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("view-claims")
}

/// The claim key for one mirrored remote pane of one host.
pub fn claim_key(host: &str, remote_pane: &str) -> String {
    format!("{}--{}", sane_component(host), sane_component(remote_pane))
}

/// The claim key a streamer's argv implies: `--host-name` (the ssh target
/// when a hand-run streamer has none) and the remote pane target. The sidebar
/// copy and the view parse the same argv, so they always agree.
pub fn claim_key_for(args: &crate::pane::Args) -> String {
    claim_key(args.host_name.as_deref().unwrap_or(&args.ssh_target), &args.pane_target)
}

fn claim_file(state_dir: &Path, key: &str, ext: &str) -> PathBuf {
    claims_dir(state_dir).join(format!("{}.{ext}", sane_component_keep_dash(key)))
}

/// Keys are built by `claim_key`; anything else is made path-safe here.
fn sane_component_keep_dash(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' }).collect()
}

pub fn claim_path(state_dir: &Path, sidebar: &str) -> PathBuf {
    claim_file(state_dir, sidebar, "json")
}

fn lock_path(state_dir: &Path, sidebar: &str) -> PathBuf {
    claim_file(state_dir, sidebar, "lock")
}

pub fn socket_path(state_dir: &Path, sidebar: &str) -> PathBuf {
    claim_file(state_dir, sidebar, "sock")
}

fn released_path(state_dir: &Path, sidebar: &str) -> PathBuf {
    claim_file(state_dir, sidebar, "released")
}

fn try_lock(file: &fs::File) -> bool {
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

fn open_lock(state_dir: &Path, sidebar: &str) -> std::io::Result<fs::File> {
    fs::create_dir_all(claims_dir(state_dir))?;
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(state_dir, sidebar))
}

fn read_claim(state_dir: &Path, sidebar: &str) -> Option<ViewClaim> {
    serde_json::from_str(&fs::read_to_string(claim_path(state_dir, sidebar)).ok()?).ok()
}

/// Remove what a dead view left, its released marker included. Only ever
/// called while holding the lock, so a new view (which locks before it
/// writes) cannot lose its fresh claim.
fn remove_claim_files(state_dir: &Path, sidebar: &str) {
    let _ = fs::remove_file(claim_path(state_dir, sidebar));
    let _ = fs::remove_file(socket_path(state_dir, sidebar));
    let _ = fs::remove_file(released_path(state_dir, sidebar));
}

/// The live claim on this sidebar pane, if any. A claim whose holder is gone
/// is cleaned up here and reads as absent.
pub fn live_claim(state_dir: &Path, sidebar: &str) -> Option<ViewClaim> {
    let claim_file = claim_path(state_dir, sidebar);
    if !claim_file.exists() {
        // a released marker a sidebar copy wrote as the view went away must
        // not linger; only an unheld lock proves no view is starting up
        if released_path(state_dir, sidebar).exists() {
            if let Ok(lock) = open_lock(state_dir, sidebar) {
                if try_lock(&lock) {
                    let _ = fs::remove_file(released_path(state_dir, sidebar));
                }
            }
        }
        return None;
    }
    let lock = open_lock(state_dir, sidebar).ok()?;
    if try_lock(&lock) {
        // Nobody holds the lock, so no view is alive for this pane: whatever
        // is on disk is left over from one that died. Dropping `lock` unlocks.
        remove_claim_files(state_dir, sidebar);
        return None;
    }
    // Held. Either a live view, or one that is still writing its claim.
    read_claim(state_dir, sidebar).filter(|c| pid_alive(c.pid))
}

/// A claim held by this process. Dropping it (any exit path that unwinds)
/// removes the claim; the lock goes with the process in every case.
pub struct ClaimGuard {
    state_dir: PathBuf,
    sidebar: String,
    _lock: fs::File,
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        remove_claim_files(&self.state_dir, &self.sidebar);
    }
}

/// Claim the view of one sidebar pane and bind the input socket it names.
pub fn create_claim(
    state_dir: &Path,
    sidebar: &str,
    tile_pane_id: &str,
    herdr_socket: Option<&str>,
) -> Result<(ClaimGuard, std::os::unix::net::UnixListener)> {
    let lock = open_lock(state_dir, sidebar)?;
    // A sidebar copy or the daemon may hold the lock for an instant while it
    // checks for a claim; only a lock held throughout is another view.
    let mut locked = false;
    for _ in 0..10 {
        if try_lock(&lock) {
            locked = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if !locked {
        let holder = read_claim(state_dir, sidebar)
            .map(|c| format!(" (shown in {})", c.tile_pane_id))
            .unwrap_or_default();
        return Err(err(format!("{sidebar} is already being viewed elsewhere{holder}")));
    }
    // Anything on disk now is left over from a dead view.
    remove_claim_files(state_dir, sidebar);
    let socket = socket_path(state_dir, sidebar);
    let listener = std::os::unix::net::UnixListener::bind(&socket)?;
    let claim = ViewClaim {
        tile_pane_id: tile_pane_id.to_string(),
        pid: std::process::id() as i32,
        socket: socket.display().to_string(),
        herdr_socket: herdr_socket.map(str::to_string),
        started: crate::state::unix_now(),
    };
    let path = claim_path(state_dir, sidebar);
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_string(&claim)?)?;
    fs::rename(&tmp, &path)?;
    Ok((
        ClaimGuard { state_dir: state_dir.to_path_buf(), sidebar: sidebar.to_string(), _lock: lock },
        listener,
    ))
}

/// The sidebar copy says it has let go of the remote terminal.
pub fn mark_released(state_dir: &Path, sidebar: &str) {
    let path = released_path(state_dir, sidebar);
    if let Some(dir) = path.parent() {
        let _ = fs::create_dir_all(dir);
    }
    let _ = fs::write(path, std::process::id().to_string());
}

pub fn clear_released(state_dir: &Path, sidebar: &str) {
    let _ = fs::remove_file(released_path(state_dir, sidebar));
}

pub fn is_released(state_dir: &Path, sidebar: &str) -> bool {
    released_path(state_dir, sidebar).exists()
}

/// The remote-client record key a view uses: its own, so the view and the
/// sidebar copy never adopt (and kill) each other's live client by accident.
pub fn view_record_target(ssh_target: &str) -> String {
    format!("{ssh_target}#view")
}

// ---------------------------------------------------------------------------
// sidebar-side decisions

#[derive(Debug, PartialEq, Eq)]
pub enum SidebarStep {
    /// nothing changed
    Stay,
    /// a view appeared: release the remote terminal and stand aside
    Release,
    /// the view went away: attach again
    Reconnect,
}

/// What a sidebar copy does after one claim poll.
pub fn sidebar_step(was_claimed: bool, claimed_now: bool) -> SidebarStep {
    match (was_claimed, claimed_now) {
        (false, true) => SidebarStep::Release,
        (true, false) => SidebarStep::Reconnect,
        _ => SidebarStep::Stay,
    }
}

/// Split focus reports out of pty input. Returns the input without them and
/// whether a focus-in (`ESC [ I`) was among them. Focus-out is dropped too:
/// neither is meant for the remote app.
pub fn strip_focus_reports(bytes: &[u8]) -> (Vec<u8>, bool) {
    let mut out = Vec::with_capacity(bytes.len());
    let mut focus_in = false;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b && bytes.get(i + 1) == Some(&b'[') {
            match bytes.get(i + 2) {
                Some(b'I') => {
                    focus_in = true;
                    i += 3;
                    continue;
                }
                Some(b'O') => {
                    i += 3;
                    continue;
                }
                _ => {}
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    (out, focus_in)
}

/// The static notice a sidebar copy draws while it stands aside. Also turns
/// its own mouse grab off and focus reporting on.
pub fn notice(tile_pane_id: &str) -> String {
    format!(
        "\x1b[?1002l\x1b[?1006l\x1b[?1004h\x1b[?25l\x1b[2J\x1b[H\
         \x1b[2mshown in {tile_pane_id} \u{2014} close that tile to view here\x1b[0m"
    )
}

/// Leave the stand-aside state: focus reporting off (the mouse grab is put
/// back by the streamer itself).
pub const NOTICE_END: &str = "\x1b[?1004l\x1b[2J\x1b[H";

/// Sidebar side of input forwarding: one connection to the view's socket,
/// reopened when it drops.
#[derive(Default)]
pub struct Forwarder {
    conn: Option<tokio::net::UnixStream>,
    socket: Option<String>,
}

impl Forwarder {
    pub fn reset(&mut self) {
        self.conn = None;
        self.socket = None;
    }

    /// Deliver `bytes` to the view listening on `socket`. `false` when the
    /// view could not be reached; the bytes are then dropped (the claim is
    /// about to be found stale).
    pub async fn send(&mut self, socket: &str, bytes: &[u8]) -> bool {
        if self.socket.as_deref() != Some(socket) {
            self.conn = None;
            self.socket = Some(socket.to_string());
        }
        for _ in 0..2 {
            if self.conn.is_none() {
                let connect = tokio::net::UnixStream::connect(socket);
                match tokio::time::timeout(FORWARD_CONNECT_TIMEOUT, connect).await {
                    Ok(Ok(stream)) => self.conn = Some(stream),
                    _ => return false,
                }
            }
            if let Some(conn) = self.conn.as_mut() {
                if conn.write_all(bytes).await.is_ok() {
                    return true;
                }
            }
            self.conn = None;
        }
        false
    }
}

/// Ask local Herdr to focus the tile. Best effort.
pub async fn focus_tile(herdr_socket: &str, tile_pane_id: &str) {
    let client = crate::api::ApiClient::at(Path::new(herdr_socket));
    let _ = client.request("pane.focus", json!({ "pane_id": tile_pane_id })).await;
}

/// View side: accept forwarded input and hand it to the streamer loop as if it
/// had been typed into the tile.
pub fn spawn_input_listener(
    listener: std::os::unix::net::UnixListener,
    deliver: impl Fn(Vec<u8>) -> tokio::task::JoinHandle<bool> + Send + Sync + 'static,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::UnixListener::from_std(listener)?;
    let deliver = std::sync::Arc::new(deliver);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { break };
            let deliver = deliver.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if !matches!(deliver(buf[..n].to_vec()).await, Ok(true)) {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// target resolution

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub host: String,
    pub remote_pane_id: String,
    pub sidebar_pane_id: String,
}

/// Find the sidebar copy a target names. `target` is a local pane id of a
/// mirrored pane, or a local agent's name (as `herdr agent list` shows it).
/// `agents` is the `agents` array of local `agent.list`; `maps` is each host
/// name with its map file.
pub fn resolve(
    target: &str,
    agents: &[Value],
    maps: &[(String, crate::state::HostState)],
) -> Option<Resolved> {
    let by_local = |local: &str| {
        maps.iter().find_map(|(host, state)| {
            state
                .panes
                .iter()
                .find(|(_, e)| e.local_id == local && !e.is_tombstoned())
                .map(|(rid, e)| Resolved {
                    host: host.clone(),
                    remote_pane_id: rid.clone(),
                    sidebar_pane_id: e.local_id.clone(),
                })
        })
    };
    if let Some(found) = by_local(target) {
        return Some(found);
    }
    let field = |a: &Value, k: &str| a.get(k).and_then(|v| v.as_str()).map(str::to_string);
    agents
        .iter()
        .filter(|a| {
            field(a, "name").as_deref() == Some(target)
                || field(a, "pane_id").as_deref() == Some(target)
                || field(a, "terminal_id").as_deref() == Some(target)
        })
        .find_map(|a| field(a, "pane_id").and_then(|p| by_local(&p)))
}

/// Pure retry rule for the view: an attach refused because the terminal is
/// still held (by the sidebar copy that is releasing it) is retried, not
/// counted as a failing host.
pub fn retry_attach_conflict(is_view: bool, reason: &str) -> bool {
    is_view && reason.to_ascii_lowercase().contains("already has an attached client")
}

// ---------------------------------------------------------------------------
// command

/// How often a view whose remote pane went away tries to find its agent again.
const RERESOLVE_INTERVAL: Duration = Duration::from_secs(5);

/// Resolve `target` from what is on disk and in local Herdr right now. `gone`
/// is the (host, remote pane) a view just lost: a map not yet updated for the
/// remote layout change still points the agent there, and that is not an
/// answer — the caller waits for the daemon to catch up instead.
pub fn resolve_excluding(
    target: &str,
    agents: &[Value],
    maps: &[(String, crate::state::HostState)],
    gone: Option<&(String, String)>,
) -> Option<Resolved> {
    resolve(target, agents, maps)
        .filter(|r| gone.map_or(true, |(h, p)| !(r.host == *h && r.remote_pane_id == *p)))
}

async fn resolve_now(
    env: &crate::util::Env,
    target: &str,
    gone: Option<&(String, String)>,
) -> Result<(crate::config::MirrorConfig, Option<Resolved>)> {
    let cfg = crate::config::load_config(&env.config_search)?;
    let maps: Vec<(String, crate::state::HostState)> = cfg
        .hosts
        .iter()
        .map(|h| (h.name.clone(), crate::state::load_state(&env.state_dir, &h.name)))
        .collect();
    let agents = if maps.iter().any(|(_, s)| s.panes.values().any(|e| e.local_id == target)) {
        Vec::new()
    } else {
        crate::api::ApiClient::at(&env.local_socket)
            .request("agent.list", json!({}))
            .await?
            .get("agents")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
    };
    let found = resolve_excluding(target, &agents, &maps, gone);
    Ok((cfg, found))
}

pub async fn cmd_view(env: crate::util::Env, rest: &[String]) -> Result<()> {
    let target = rest
        .first()
        .filter(|t| !t.starts_with('-'))
        .ok_or_else(|| err("usage: herdr-mirror view <agent-name | local-pane-id>"))?
        .clone();
    let tile = std::env::var("HERDR_PANE_ID")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| err("herdr-mirror view must run inside a Herdr pane (HERDR_PANE_ID unset)"))?;

    // (host, remote pane) the previous round streamed until it was gone
    let mut gone: Option<(String, String)> = None;
    loop {
        let (cfg, found) = match resolve_now(&env, &target, gone.as_ref()).await {
            Ok((cfg, Some(found))) => (cfg, found),
            // at startup a bad target is the user's mistake: say so and stop
            Ok((_, None)) if gone.is_none() => {
                return Err(err(format!(
                    "{target}: not a mirrored agent or mirror pane in {}",
                    env.state_dir.display()
                )))
            }
            // after a move, keep looking (the daemon may not have remapped it
            // yet, or local Herdr may be briefly unreachable)
            unresolved => {
                let (host, pane) = gone.as_ref().expect("only after a gone target");
                let why = match unresolved {
                    Err(e) => format!(" ({e})"),
                    _ => String::new(),
                };
                println!(
                    "remote pane {pane} on {host} is gone; {target} does not resolve to another \
                     mirrored pane yet{why} — retrying in {}s",
                    RERESOLVE_INTERVAL.as_secs()
                );
                if wait_or_quit(RERESOLVE_INTERVAL).await {
                    return Ok(());
                }
                continue;
            }
        };
        if found.sidebar_pane_id == tile {
            return Err(err("refusing to view a mirror pane inside itself"));
        }
        let host = cfg
            .hosts
            .iter()
            .find(|h| h.name == found.host)
            .ok_or_else(|| err(format!("host {} missing from hosts.toml", found.host)))?;
        // The same argv the daemon types into the sidebar copy: one source of
        // truth for transport, session, control and caps.
        let argv = crate::mirror::cmd_for_pane(host, &env.state_dir, &std::collections::HashMap::new())(
            &found.remote_pane_id,
        );
        let args = crate::pane::parse_args(&argv[2..])?;
        if let Some((h, p)) = &gone {
            println!("{target} moved from {p} on {h} to {} on {}", found.remote_pane_id, found.host);
        }

        let herdr_socket = env.local_socket.display().to_string();
        // keyed by the remote pane, so the claim survives the sidebar copy being
        // recreated under a new local pane id
        let key = claim_key_for(&args);
        let (guard, listener) = create_claim(&env.state_dir, &key, &tile, Some(&herdr_socket))?;

        // Give the sidebar copy's stream its moment to let go; with no stream
        // running (paused, or between respawns) nothing holds the terminal.
        if crate::util::streamer_alive(&env.state_dir, &args.ssh_target, &args.pane_target) {
            println!("waiting for {} to release {}…", found.sidebar_pane_id, found.remote_pane_id);
            let deadline = tokio::time::Instant::now() + RELEASE_WAIT;
            while !is_released(&env.state_dir, &key)
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(RELEASE_POLL).await;
            }
        }

        let result = crate::pane::run_view(
            args,
            crate::pane::ViewRuntime { listener, sidebar_pane_id: found.sidebar_pane_id.clone() },
        )
        .await;
        // the old pane's claim goes before the next one is taken
        drop(guard);
        match result? {
            crate::pane::ViewEnd::Done => return Ok(()),
            crate::pane::ViewEnd::TargetGone => {
                println!("remote pane {} on {} is gone; resolving {target} again…", found.remote_pane_id, found.host);
                gone = Some((found.host, found.remote_pane_id));
            }
        }
    }
}

/// Sleep, unless the view is told to quit first (`true`). The streamer
/// installed handlers for these signals, so their default "terminate" no
/// longer applies between rounds.
async fn wait_or_quit(d: Duration) -> bool {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut int), Ok(mut hup)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
        signal(SignalKind::hangup()),
    ) else {
        tokio::time::sleep(d).await;
        return false;
    };
    tokio::select! {
        _ = tokio::time::sleep(d) => false,
        _ = term.recv() => true,
        _ = int.recv() => true,
        _ = hup.recv() => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        // short: unix socket paths are limited to ~104 bytes on macOS
        let d = std::env::temp_dir().join(format!(
            "hmv-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn a_claim_is_live_while_held_and_gone_after_drop() {
        let d = dir("life");
        assert_eq!(live_claim(&d, "w1:p2"), None);
        let (guard, _listener) = create_claim(&d, "w1:p2", "w9:p1", Some("/x.sock")).unwrap();
        let claim = live_claim(&d, "w1:p2").expect("live");
        assert_eq!(claim.tile_pane_id, "w9:p1");
        assert_eq!(claim.pid, std::process::id() as i32);
        assert_eq!(claim.herdr_socket.as_deref(), Some("/x.sock"));
        assert!(Path::new(&claim.socket).exists());
        // other panes are not claimed
        assert_eq!(live_claim(&d, "w1:p3"), None);
        // a second view of the same pane is refused
        assert!(create_claim(&d, "w1:p2", "w9:p5", None).is_err());
        mark_released(&d, "w1:p2");
        drop(guard);
        assert_eq!(live_claim(&d, "w1:p2"), None);
        assert!(!claim_path(&d, "w1:p2").exists());
        assert!(!socket_path(&d, "w1:p2").exists());
        assert!(!is_released(&d, "w1:p2"));
        let _ = fs::remove_dir_all(d);
    }

    #[test]
    fn a_claim_left_by_a_dead_view_is_ignored_and_cleaned() {
        let d = dir("stale");
        fs::create_dir_all(claims_dir(&d)).unwrap();
        let mut dead = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = dead.id() as i32;
        dead.wait().unwrap();
        let claim = ViewClaim {
            tile_pane_id: "w9:p1".into(),
            pid: dead_pid,
            socket: socket_path(&d, "w1:p2").display().to_string(),
            herdr_socket: None,
            started: 1.0,
        };
        fs::write(claim_path(&d, "w1:p2"), serde_json::to_string(&claim).unwrap()).unwrap();
        fs::write(socket_path(&d, "w1:p2"), "").unwrap();
        assert_eq!(live_claim(&d, "w1:p2"), None);
        assert!(!claim_path(&d, "w1:p2").exists(), "stale claim cleaned");
        assert!(!socket_path(&d, "w1:p2").exists(), "stale socket cleaned");

        // A LIVE pid that holds no lock (a recycled pid) is stale too.
        let claim = ViewClaim { pid: std::process::id() as i32, ..claim };
        fs::write(claim_path(&d, "w1:p2"), serde_json::to_string(&claim).unwrap()).unwrap();
        assert_eq!(live_claim(&d, "w1:p2"), None);
        assert!(!claim_path(&d, "w1:p2").exists());

        // and a new view can take the pane afterwards
        let (_g, _l) = create_claim(&d, "w1:p2", "w9:p3", None).unwrap();
        assert_eq!(live_claim(&d, "w1:p2").unwrap().tile_pane_id, "w9:p3");
        let _ = fs::remove_dir_all(d);
    }

    #[test]
    fn a_released_marker_never_outlives_its_claim() {
        let d = dir("rel");
        let key = claim_key("cfo-studio", "w1:p7");
        assert_eq!(key, "cfo_studio--w1_p7");
        // left behind by a sidebar copy that marked as the view went away
        mark_released(&d, &key);
        assert!(is_released(&d, &key));
        assert_eq!(live_claim(&d, &key), None);
        assert!(!is_released(&d, &key), "cleaned with no view holding the lock");
        // and a dead view's claim takes its marker with it
        let d2 = dir("rel2");
        fs::create_dir_all(claims_dir(&d2)).unwrap();
        let mut dead = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = dead.id() as i32;
        dead.wait().unwrap();
        let claim = ViewClaim {
            tile_pane_id: "w9:p1".into(),
            pid: dead_pid,
            socket: socket_path(&d2, &key).display().to_string(),
            herdr_socket: None,
            started: 1.0,
        };
        fs::write(claim_path(&d2, &key), serde_json::to_string(&claim).unwrap()).unwrap();
        mark_released(&d2, &key);
        assert_eq!(live_claim(&d2, &key), None);
        assert!(!is_released(&d2, &key));
        let _ = fs::remove_dir_all(d);
        let _ = fs::remove_dir_all(d2);
    }

    #[test]
    fn sidebar_releases_on_a_new_claim_and_reconnects_when_it_ends() {
        assert_eq!(sidebar_step(false, false), SidebarStep::Stay);
        assert_eq!(sidebar_step(false, true), SidebarStep::Release);
        assert_eq!(sidebar_step(true, true), SidebarStep::Stay);
        assert_eq!(sidebar_step(true, false), SidebarStep::Reconnect);
    }

    #[test]
    fn focus_reports_are_stripped_and_focus_in_is_seen() {
        assert_eq!(strip_focus_reports(b"abc"), (b"abc".to_vec(), false));
        assert_eq!(strip_focus_reports(b"\x1b[I"), (Vec::new(), true));
        assert_eq!(strip_focus_reports(b"a\x1b[Ob\x1b[Ic"), (b"abc".to_vec(), true));
        // other escape sequences pass untouched
        assert_eq!(strip_focus_reports(b"\x1b[A\x1b[200~x\x1b[201~"), (b"\x1b[A\x1b[200~x\x1b[201~".to_vec(), false));
    }

    #[test]
    fn only_a_view_retries_attach_conflicts() {
        let reason = "terminal t1 already has an attached client; retry";
        assert!(retry_attach_conflict(true, reason));
        assert!(!retry_attach_conflict(false, reason));
        assert!(!retry_attach_conflict(true, "connection refused"));
    }

    #[tokio::test]
    async fn sidebar_input_reaches_the_view_over_its_socket() {
        let d = dir("fwd");
        let (_guard, listener) = create_claim(&d, "w1:p2", "w9:p1", None).unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(8);
        spawn_input_listener(listener, move |bytes| {
            let tx = tx.clone();
            tokio::spawn(async move { tx.send(bytes).await.is_ok() })
        })
        .unwrap();
        let claim = live_claim(&d, "w1:p2").unwrap();
        let mut fwd = Forwarder::default();
        assert!(fwd.send(&claim.socket, b"hello\r").await);
        assert!(fwd.send(&claim.socket, b"\x1b[200~multi\nline\x1b[201~").await);
        let mut got = Vec::new();
        while got.len() < b"hello\r\x1b[200~multi\nline\x1b[201~".len() {
            let chunk = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("forwarded input")
                .unwrap();
            got.extend(chunk);
        }
        assert_eq!(got, b"hello\r\x1b[200~multi\nline\x1b[201~".to_vec());
        // nobody listening: reported, not hung
        let mut other = Forwarder::default();
        assert!(!other.send(&d.join("nobody.sock").display().to_string(), b"x").await);
        let _ = fs::remove_dir_all(d);
    }

    #[test]
    fn a_target_resolves_by_local_pane_or_by_agent_name() {
        let mut state = crate::state::HostState::default();
        state.panes.insert(
            "w1:p7".into(),
            crate::state::PaneEntry { local_id: "wL:p3".into(), ..Default::default() },
        );
        state.panes.insert(
            "w1:p8".into(),
            crate::state::PaneEntry {
                local_id: "wL:p4".into(),
                tombstone: Some(true),
                ..Default::default()
            },
        );
        let maps = vec![("cfo-studio".to_string(), state)];
        let want = Resolved {
            host: "cfo-studio".into(),
            remote_pane_id: "w1:p7".into(),
            sidebar_pane_id: "wL:p3".into(),
        };
        assert_eq!(resolve("wL:p3", &[], &maps), Some(want.clone()));
        let agents = vec![
            json!({ "name": "local-only", "pane_id": "wX:p1" }),
            json!({ "name": "cfo-codex", "pane_id": "wL:p3" }),
            json!({ "name": "closed", "pane_id": "wL:p4" }),
        ];
        assert_eq!(resolve("cfo-codex", &agents, &maps), Some(want));
        assert_eq!(resolve("local-only", &agents, &maps), None);
        assert_eq!(resolve("closed", &agents, &maps), None);
        assert_eq!(resolve("nothing", &agents, &maps), None);
    }

    #[test]
    fn a_view_whose_pane_moved_re_resolves_and_never_back_to_the_dead_pane() {
        let agents = vec![json!({ "name": "cargo-vm-conductor", "pane_id": "wL:p3" })];
        let gone = ("cargo-vm".to_string(), "w2:p1".to_string());
        // the daemon has not remapped yet: the agent still points at the dead
        // pane, which is no answer
        let mut stale = crate::state::HostState::default();
        stale.panes.insert(
            "w2:p1".into(),
            crate::state::PaneEntry { local_id: "wL:p3".into(), ..Default::default() },
        );
        let maps = vec![("cargo-vm".to_string(), stale)];
        assert_eq!(resolve_excluding("cargo-vm-conductor", &agents, &maps, Some(&gone)), None);
        // at startup the same map is a valid answer
        assert!(resolve_excluding("cargo-vm-conductor", &agents, &maps, None).is_some());
        // remapped: the agent now lives in w1:p1, and the claim key follows it
        let mut moved = crate::state::HostState::default();
        moved.panes.insert(
            "w1:p1".into(),
            crate::state::PaneEntry { local_id: "wL:p3".into(), ..Default::default() },
        );
        let maps = vec![("cargo-vm".to_string(), moved)];
        let found = resolve_excluding("cargo-vm-conductor", &agents, &maps, Some(&gone)).unwrap();
        assert_eq!(found.remote_pane_id, "w1:p1");
        assert_eq!(claim_key(&found.host, &found.remote_pane_id), "cargo_vm--w1_p1");
        // gone everywhere: unresolved, the caller keeps retrying
        let maps = vec![("cargo-vm".to_string(), crate::state::HostState::default())];
        assert_eq!(resolve_excluding("cargo-vm-conductor", &agents, &maps, Some(&gone)), None);
    }
}
