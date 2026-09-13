// herdr-mirror daemon: lifecycle + sync loop (control plane).
//
//   herdr-mirror daemon       # foreground loop (what `start` spawns)
//   herdr-mirror start        # spawn detached daemon, write pidfile
//   herdr-mirror pause        # halt syncing (sticky); mirrors stay, resume with start
//   herdr-mirror ensure       # start only if not running (cheap event hook)
//   herdr-mirror status       # print daemon/host/mirror state
//   herdr-mirror once         # single converge pass, no daemon
//   herdr-mirror restore [host] [remote-id]   # un-tombstone closed mirrors
//   herdr-mirror teardown     # close all mirror workspaces, wipe id maps
//
// Each host runs as one task owning all its state: events, pokes, and timers
// arrive through one select loop, so converge and the status fast-path never
// interleave.

use std::collections::HashMap;
use std::fs;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::api::{ApiClient, EventStream};
use crate::config::{load_config, HostConfig};
use crate::mirror::{
    apply_remote_closes, converge, mark_unknown, mirror_source, push_pane_status, regroup_sidebar,
    teardown, AgentInfo, ConvergeDeps, PaneStatusDeps,
};
use crate::state::{load_state, save_state, HostState};
use crate::util::{err, now_iso, pid_alive, sleep_until_earliest, Env, Logger, Result};

// --- pidfile / pause marker ---

fn pid_path(env: &Env) -> PathBuf {
    env.state_dir.join("daemon.pid")
}

pub fn running_pid(env: &Env) -> Option<i32> {
    let pid: i32 = fs::read_to_string(pid_path(env)).ok()?.trim().parse().ok()?;
    pid_alive(pid).then_some(pid)
}

// Never unlink the lock file: every writer must lock the same inode.
fn open_daemon_lock(env: &Env) -> Result<fs::File> {
    Ok(fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(env.state_dir.join("daemon.lock"))?)
}

fn try_daemon_lock(env: &Env) -> Result<Option<fs::File>> {
    let file = open_daemon_lock(env)?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(Some(file));
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::WouldBlock {
        Ok(None)
    } else {
        Err(error.into())
    }
}

struct DaemonOwner {
    _lock: fs::File,
    pid_path: PathBuf,
    pid: String,
}

impl DaemonOwner {
    fn acquire(env: &Env) -> Result<Self> {
        let lock = try_daemon_lock(env)?
            .ok_or_else(|| err("mirror daemon already running: daemon.lock is held"))?;
        Ok(Self {
            _lock: lock,
            pid_path: pid_path(env),
            pid: std::process::id().to_string(),
        })
    }

    fn publish(&self) -> Result<()> {
        let temporary = self.pid_path.with_extension(format!("pid.{}.tmp", self.pid));
        fs::write(&temporary, &self.pid)?;
        fs::rename(temporary, &self.pid_path)?;
        Ok(())
    }
}

impl Drop for DaemonOwner {
    fn drop(&mut self) {
        // Also runs on startup/API errors, while we still own the lifetime lock.
        if fs::read_to_string(&self.pid_path).ok().as_deref() == Some(self.pid.as_str()) {
            let _ = fs::remove_file(&self.pid_path);
        }
    }
}

// Sticky pause marker: blocks the focus-hook autostart until an explicit
// start clears it (a crash leaves no marker, so it still auto-recovers).
pub(crate) fn pause_path(state_dir: &std::path::Path) -> PathBuf {
    state_dir.join("daemon.paused")
}

pub fn is_paused(env: &Env) -> bool {
    streams_paused(&env.state_dir)
}

/// The explicit operator pause is shared with pane streamers. A daemon crash
/// leaves no marker, so it must not quiesce otherwise healthy mirror panes.
pub(crate) fn streams_paused(state_dir: &std::path::Path) -> bool {
    pause_path(state_dir).exists()
}

pub fn set_paused(env: &Env, paused: bool) {
    if paused {
        let _ = fs::write(pause_path(&env.state_dir), now_iso());
    } else {
        let _ = fs::remove_file(pause_path(&env.state_dir));
    }
}

// --- per-host runtime ---

struct HostCtx {
    env_state_dir: PathBuf,
    host: HostConfig,
    local: ApiClient,
    log: Logger,
    close_remote_on_local_close: bool,
    closes: crate::closes::Closes,
    names: crate::mirror::SessionNamePlanner,
    // Hermetic acceptance can drive the production host lifecycle against a
    // public-protocol peer without invoking ssh. Production always leaves it
    // unset and uses RemoteHost below.
    #[cfg(test)]
    remote_override: Option<ApiClient>,
    /// Counts entries through the production transport-admission seam. Tests
    /// use it to prove a hidden host never reaches `run_connected`.
    #[cfg(test)]
    connect_attempts: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[derive(Debug)]
enum HostSignal {
    Converge,
    /// An explicit ask from the CLI (`wake`, `show`, `restore`, `hide`), which
    /// reaches every host task through the daemon's SIGUSR1 handler. Ordinary
    /// local traffic is `Converge`, and telling the two apart is what lets a
    /// deliberate act reset a host's reconnect ladder while a split drag does
    /// not.
    Resync,
}

#[derive(Clone)]
struct LocalEventGuards {
    closes: crate::closes::Closes,
}

const BROADCAST_SUBS: &[&str] = &[
    "workspace.created",
    "workspace.renamed",
    "workspace.closed",
    "tab.created",
    "tab.renamed",
    "tab.closed",
    "pane.created",
    "pane.closed",
    "pane.exited",
    // a bare remote resize (no pane created/closed) has no other event to
    // hang a converge off of; falls into the generic converge_at branch below
    // like any subscription this daemon doesn't special-case.
    "layout.updated",
];

fn sub_list(pane_ids: &[String]) -> Vec<Value> {
    let mut subs: Vec<Value> = BROADCAST_SUBS.iter().map(|t| json!({ "type": t })).collect();
    subs.extend(pane_ids.iter().map(|p| json!({ "type": "pane.agent_status_changed", "pane_id": p })));
    subs
}

/// Broadcast structure events + per-pane agent-status subscriptions
/// (pane.agent_status_changed requires a pane_id). A rejected pane
/// subscription degrades to broadcast-only instead of killing the connection.
async fn resubscribe(
    ctx: &HostCtx,
    remote: &ApiClient,
    stream: &mut EventStream,
    subscribed_key: &mut String,
    state: &HostState,
) -> Result<()> {
    // live panes only: tombstoned mirrors' statuses are moot
    let mut pane_ids: Vec<String> = state
        .panes
        .iter()
        .filter(|(_, e)| !e.is_tombstoned())
        .map(|(rid, _)| rid.clone())
        .collect();
    pane_ids.sort();
    let key = pane_ids.join(",");
    if key == *subscribed_key {
        return Ok(());
    }
    match remote.subscribe(sub_list(&pane_ids)).await {
        Ok(s) => {
            *stream = s;
            *subscribed_key = key;
            Ok(())
        }
        Err(e) => {
            ctx.log.log(&format!(
                "[{}] pane subscriptions rejected ({e}) — broadcast only",
                ctx.host.name
            ));
            *stream = remote.subscribe(sub_list(&[])).await?;
            *subscribed_key = "<broadcast>".into();
            Ok(())
        }
    }
}

/// Fast-path: apply coalesced status updates without a remote snapshot.
/// Returns true if an event referenced a pane we don't mirror yet.
async fn flush_status(ctx: &HostCtx, pending: HashMap<String, Value>) -> bool {
    let mut state = load_state(&ctx.env_state_dir, &ctx.host.name);
    let mut need_converge = false;
    for (remote_id, data) in pending {
        if status_event_needs_snapshot(&data) {
            need_converge = true;
            continue;
        }
        let Some(entry) = state.panes.get(&remote_id) else {
            need_converge = true; // unknown pane → let a full pass create it
            continue;
        };
        if entry.is_tombstoned() {
            continue; // user closed this mirror — its statuses are moot
        }
        let info: AgentInfo = serde_json::from_value(data).unwrap_or_default();
        let agent = info.has_agent().then_some(&info);
        let desired_name = agent.and_then(|agent| {
            crate::mirror::mirrored_agent_name(&ctx.host.name, agent.name.as_deref())
        });
        if entry.reported_name != desired_name {
            need_converge = true; // rebuild the complete collision map first
            continue;
        }
        push_pane_status(
            &PaneStatusDeps {
                local: &ctx.local,
                state_dir: &ctx.env_state_dir,
                host_name: &ctx.host.name,
                log: &ctx.log,
            },
            &remote_id,
            &mut state,
            agent,
            desired_name,
            false,
        )
        .await;
    }
    if let Err(e) = save_state(&ctx.env_state_dir, &ctx.host.name, &state) {
        ctx.log.log(&format!("[{}] state save failed: {e}", ctx.host.name));
    }
    need_converge
}

fn status_event_needs_snapshot(data: &Value) -> bool {
    ["name", "interactive_ready", "agent_session", "tokens"]
        .iter()
        .any(|key| data.get(*key).is_none())
}

/// Which transport the next reconnect should try first.
///
/// A fallback to the exec relay is only remembered once it has happened twice
/// running. The probe fails for transient reasons too (the remote herdr
/// restarting, a mux hiccup, one slow ping), and remembering the first one
/// pins a healthy host to the slower transport for the daemon's whole life
/// with a single log line as the only clue. A genuinely broken host wastes one
/// probe on the next reconnect and then sticks, which is what the memory is
/// for.
fn remember_transport(
    last: Option<crate::config::ApiTransport>,
    exec_streak: &mut u32,
) -> Option<crate::config::ApiTransport> {
    match last {
        Some(crate::config::ApiTransport::Exec) => {
            *exec_streak += 1;
            (*exec_streak >= 2).then_some(crate::config::ApiTransport::Exec)
        }
        other => {
            *exec_streak = 0;
            other
        }
    }
}

/// Connected phase: subscribe, converge, then react to events/pokes/timers
/// until the connection drops (returns Err).
async fn run_connected(
    ctx: &HostCtx,
    poke: &mut mpsc::Receiver<HostSignal>,
    ladder: &mut ReconnectLadder,
    remembered_transport: &mut Option<crate::config::ApiTransport>,
    exec_streak: &mut u32,
) -> Result<()> {
    #[cfg(test)]
    if let Some(attempts) = &ctx.connect_attempts {
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    #[cfg(test)]
    if let Some(remote) = &ctx.remote_override {
        ladder.reset();
        return connected_session(ctx, poke, remote.clone()).await;
    }
    let mut remote_host = crate::remote::RemoteHost::new(&ctx.host, &ctx.env_state_dir);
    // a fresh RemoteHost is built on every reconnect, so what worked last
    // time (specifically: an auto host that fell back to the exec relay)
    // would otherwise be re-probed via streamlocal on every single reconnect
    // for the life of the daemon
    remote_host.hint_transport(*remembered_transport);
    let (remote, _status) = remote_host.connect_api().await?;
    *remembered_transport = remember_transport(remote_host.last_api_transport, exec_streak);
    ladder.reset();
    connected_session(ctx, poke, remote).await
}

async fn connected_session(
    ctx: &HostCtx,
    poke: &mut mpsc::Receiver<HostSignal>,
    remote: ApiClient,
) -> Result<()> {
    let deps = ConvergeDeps {
        local: ctx.local.clone(),
        remote: remote.clone(),
        host: ctx.host.clone(),
        state_dir: ctx.env_state_dir.clone(),
        log: ctx.log.clone(),
        close_remote_on_local_close: ctx.close_remote_on_local_close,
        closes: ctx.closes.clone(),
        names: ctx.names.clone(),
    };
    // broadcast-only first: subscribing a since-dead pane id is rejected, so
    // converge must prune the map before the per-pane upgrade
    let mut stream = remote.subscribe(sub_list(&[])).await?;
    let mut subscribed_key = String::from("<broadcast>");
    let mut name_plan_changes = ctx.names.subscribe();
    // Pokes accumulated during the dial are stale; the initial converge below
    // already reads the current local and remote state.
    while poke.try_recv().is_ok() {}
    let state = converge(&deps).await?;
    resubscribe(ctx, &remote, &mut stream, &mut subscribed_key, &state).await?;
    ctx.log.log(&format!("[{}] connected and synced", ctx.host.name));
    crate::state::publish_host_health(
        &ctx.env_state_dir,
        &ctx.host.name,
        &crate::state::HostHealth {
            summary: "connected and synced".into(),
            at_iso: now_iso(),
            next_retry_unix: None,
        },
    );
    // A wake asked for exactly this. Retire it here, or a request made while the
    // host was already up would also shorten the NEXT disconnect's backoff,
    // which nobody asked for.
    crate::state::take_wake(&ctx.env_state_dir, &ctx.host.name);

    let mut converge_at: Option<Instant> = None;
    let mut status_at: Option<Instant> = None;
    let mut closes_at: Option<Instant> = None;
    let mut pending_status: HashMap<String, Value> = HashMap::new();
    let mut pending_closes: Vec<String> = Vec::new();

    loop {
        let sleep = sleep_until_earliest([converge_at, status_at, closes_at]);
        tokio::select! {
            changed = name_plan_changes.changed() => {
                if changed.is_ok() {
                    converge_at.get_or_insert(Instant::now());
                }
            }
            ev = stream.next() => {
                match ev {
                    None => return Err(err("event stream closed")),
                    // status changes take the fast-path; structure changes
                    // need a full reconcile (debounced 500ms)
                    Some(e) if e.event == "pane_agent_status_changed" => {
                        if let Some(pid) = e.data.get("pane_id").and_then(|v| v.as_str()) {
                            // coalesce: keep only the latest per pane
                            pending_status.insert(pid.to_string(), e.data.clone());
                            status_at.get_or_insert(Instant::now() + Duration::from_millis(150));
                        }
                    }
                    // explicit remote closes are authoritative: remove the mirror
                    // directly instead of inferring it from snapshot absence
                    Some(e) if matches!(e.event.as_str(), "workspace_closed" | "tab_closed" | "pane_closed") => {
                        let key = match e.event.as_str() {
                            "workspace_closed" => "workspace_id",
                            "tab_closed" => "tab_id",
                            _ => "pane_id",
                        };
                        if let Some(rid) = e.data.get(key).and_then(|v| v.as_str()) {
                            pending_closes.push(rid.to_string());
                            closes_at.get_or_insert(Instant::now() + Duration::from_millis(150));
                        }
                    }
                    Some(_) => {
                        converge_at.get_or_insert(Instant::now() + Duration::from_millis(500));
                    }
                }
            }
            Some(_) = poke.recv() => {
                converge_at.get_or_insert(Instant::now());
            }
            _ = sleep => {
                let now = Instant::now();
                if status_at.is_some_and(|t| t <= now) {
                    status_at = None;
                    let pending = std::mem::take(&mut pending_status);
                    if flush_status(ctx, pending).await {
                        // unknown pane → let a full pass create it
                        converge_at.get_or_insert(now);
                    }
                }
                if closes_at.is_some_and(|t| t <= now) {
                    closes_at = None;
                    let closed = std::mem::take(&mut pending_closes);
                    apply_remote_closes(&ctx.local, &ctx.env_state_dir, &ctx.host.name, &closed, &ctx.log).await;
                    // reconcile + refresh subscriptions after the removals
                    converge_at.get_or_insert(now);
                }
                if converge_at.is_some_and(|t| t <= now) {
                    converge_at = None;
                    // a hide pressed while we are up arrives as a poke, and
                    // converge only freezes — the closing is here
                    crate::mirror::apply_hidden(
                        &ctx.local,
                        &ctx.env_state_dir,
                        &ctx.host.name,
                        &ctx.log,
                        &ctx.closes,
                    )
                    .await;
                    let state = converge(&deps).await?;
                    // pane set may have changed
                    resubscribe(ctx, &remote, &mut stream, &mut subscribed_key, &state).await?;
                }
            }
        }
    }
}

/// Retry pacing after a lost connection.
///
/// A host whose workspace is simply stopped fails its ssh master with exit 255
/// and used to be redialled every 30 seconds for as long as the daemon ran. On
/// the owner's workstation on 2026-09-09 that was 33 079 of the 33 246 lines in
/// `daemon.log` and another 23 683 in the rotated `.1`, eleven hosts
/// contributing 1 391-4 717 lines each, every one of them a stopped workspace:
/// the one line that mattered was invisible, and each host still cost an ssh
/// attempt every half minute. So consecutive failures with the SAME reason
/// double the delay to a five-minute ceiling.
///
/// It starts at 30 seconds rather than the old five, because the entry never
/// waits this ladder out: `omnidev <workspace> herdr` calls `herdr-mirror wake`
/// before it waits, and a wake resets the ladder and breaks the sleep. Nothing
/// else needs a five-second redial — a host that drops and comes straight back
/// is one converge behind either way.
///
/// A stopped container is not a fault, it is the resting state, so it keeps its
/// own separate 300-second sleep (`DORMANT_DELAY`) and never advances this
/// ladder.
const RECONNECT_DELAYS: [u64; 5] = [30, 60, 120, 240, 300];
const DORMANT_DELAY: u64 = 300;

/// One host's place on that ladder, and whether this failure is worth a line.
///
/// The whole policy is here and nowhere else, because the two halves are one
/// decision: the delay only changes when the ladder advances, and a delay that
/// did not change is exactly the retry nobody needs told about again.
#[derive(Debug, Default)]
struct ReconnectLadder {
    /// index into `RECONNECT_DELAYS` of the delay currently being served
    step: usize,
    last_reason: Option<String>,
    last_delay: Option<u64>,
}

/// What to do after one failed dial.
#[derive(Debug, PartialEq, Eq)]
struct RetryDecision {
    delay: u64,
    /// log the first failure, a changed delay, and a changed reason — never an
    /// identical repeat of the line already in the log
    log: bool,
}

impl ReconnectLadder {
    /// A failed dial with this reason.
    fn fail(&mut self, reason: &str) -> RetryDecision {
        // A different reason is a different fault, so it starts its own ladder
        // rather than inheriting the pace of the one before it — and it is
        // always logged, ceiling or not, because it is news.
        let same_reason = self.last_reason.as_deref() == Some(reason);
        self.step = if same_reason {
            (self.step + 1).min(RECONNECT_DELAYS.len() - 1)
        } else {
            0
        };
        self.decide(reason, RECONNECT_DELAYS[self.step])
    }

    /// A cycle that found the container stopped.
    ///
    /// Dormancy is a separate state with its own fixed sleep, and it does not
    /// advance the ladder it does not use: a container stopped overnight would
    /// otherwise leave the ladder pinned at its ceiling, so the first real
    /// failure while it boots would wait five minutes instead of the first rung.
    /// It shares only the "log a change, not a repeat" rule, which is what kept
    /// a dormant host to one line per dormancy before this ladder existed.
    fn dormant(&mut self, reason: &str) -> RetryDecision {
        self.step = 0;
        self.decide(reason, DORMANT_DELAY)
    }

    fn decide(&mut self, reason: &str, delay: u64) -> RetryDecision {
        let log = self.last_reason.as_deref() != Some(reason) || self.last_delay != Some(delay);
        self.last_reason = Some(reason.to_string());
        self.last_delay = Some(delay);
        RetryDecision { delay, log }
    }

    /// Back to the first rung, and the next failure is news again.
    ///
    /// A successful connect, an explicit `wake`, and the `show`/`restore` resync
    /// all mean somebody or something has changed the situation, so the pace
    /// this host earned before that is no longer evidence about it. A
    /// configuration reload resets it too, by construction: the reload is
    /// `herdr-mirror pause` followed by `start`, and this state lives in the
    /// host task, never on disk.
    fn reset(&mut self) {
        *self = ReconnectLadder::default();
    }
}

/// Whether one signal arriving during a backoff sleep restarts the ladder.
///
/// `wake`, `show`, `restore` and `hide` all reach a host task through the
/// daemon's SIGUSR1 handler as a `Resync`, while the poll tick, the heal sweep
/// and every local Herdr event arrive as ordinary traffic — `local_events_task`
/// fans one poke out to every host on every event, `layout.updated` included, so
/// treating those as deliberate would collapse the ladder on every split drag.
/// `hide` is the one deliberate act that is not a reset: a host taken out of
/// view on purpose keeps the pace it is on.
fn signal_restarts_ladder(signal: &HostSignal, hidden: bool) -> bool {
    matches!(signal, HostSignal::Resync) && !hidden
}

async fn host_task(ctx: HostCtx, mut poke: mpsc::Receiver<HostSignal>) {
    let mut ladder = ReconnectLadder::default();
    // persists across reconnects for the daemon's whole lifetime — the
    // point of remembering at all (see `run_connected`)
    let mut remembered_transport: Option<crate::config::ApiTransport> = None;
    let mut exec_streak = 0u32;
    loop {
        // Before dialling, and again after every failed dial: taking a hidden
        // host's mirrors down needs only the LOCAL api, so it must not wait on a
        // remote that may never come back. That case — a dead host leaving
        // reconnecting panes on screen — is the main reason to hide one.
        crate::mirror::apply_hidden(
            &ctx.local,
            &ctx.env_state_dir,
            &ctx.host.name,
            &ctx.log,
            &ctx.closes,
        )
        .await;
        // Hidden is also a transport admission boundary. `apply_hidden` above
        // needs only the local Herdr API, but entering `run_connected` would
        // still create one remote connection before converge noticed the same
        // marker. Wait on the host's existing signal channel instead: `show`
        // clears the marker and sends Resync, while ordinary local events and
        // `wake` cannot accidentally make a hidden host dial or busy-loop.
        let mut was_hidden = false;
        while crate::state::is_hidden(&ctx.env_state_dir, &ctx.host.name) {
            was_hidden = true;
            let Some(_) = poke.recv().await else { return };
        }
        // A deliberate show starts with a clean ladder. This matches the
        // existing visible-host Resync behavior after a failed connection.
        if was_hidden {
            ladder.reset();
        }
        let e = match run_connected(
            &ctx,
            &mut poke,
            &mut ladder,
            &mut remembered_transport,
            &mut exec_streak,
        )
        .await
        {
            Ok(()) => unreachable!("run_connected only returns on error"),
            Err(e) => e,
        };
        mark_unknown(&ctx.local, &ctx.env_state_dir, &ctx.host.name, "mirror: connection lost")
            .await;
        // starts_with, not contains: the marker is always emitted as a prefix,
        // while the error text can embed user strings (target, remote_bin). A
        // substring test would make an ssh host named `dormant-box` back off
        // for 5 minutes and stop logging on every genuine failure.
        let reason = e.to_string();
        let dormant = reason.starts_with(crate::remote::DORMANT);
        let RetryDecision { delay, log: worth_logging } =
            if dormant { ladder.dormant(&reason) } else { ladder.fail(&reason) };
        // End the transport this connection was using before anything starts a
        // replacement beside it. Only when it can no longer serve — see
        // `retire_unusable_master`. A dormant cycle never had an ssh master.
        let retired = if dormant || ctx.host.kind.is_docker() {
            None
        } else {
            crate::remote::retire_unusable_master(
                &crate::remote::control_path(&ctx.env_state_dir, &ctx.host.name),
                &ctx.host.target,
            )
            .await
        };
        // The record `status` reads. Published on every failed dial, including
        // the ones this loop deliberately no longer logs, so the owner's answer
        // to "why is this host not mirroring" never depends on how long it has
        // been failing.
        crate::state::publish_host_health(
            &ctx.env_state_dir,
            &ctx.host.name,
            &crate::state::HostHealth {
                summary: format!("disconnected ({reason})"),
                at_iso: now_iso(),
                next_retry_unix: Some(crate::state::unix_now() + delay as f64),
            },
        );
        // A change of state, not every attempt: the first failure, a changed
        // delay, and a changed reason are news; the identical retry underneath
        // them is the line that buried everything else.
        if worth_logging {
            // The cause and the pid belong on ONE line: this is the record an
            // investigation reads to tell "the remote went away" from "we threw
            // the transport away", and two lines can be minutes apart in a log
            // nine hosts share.
            let retired = match retired {
                Some(pid) => format!(" — retired unusable ssh master pid {pid}"),
                None => String::new(),
            };
            ctx.log.log(&format!(
                "[{}] disconnected ({e}){retired} — retrying in {delay}s",
                ctx.host.name
            ));
        }
        // drain FIRST: pokes that piled up during a multi-second dial say nothing
        // about now, and honouring them would skip the sleep entirely
        while poke.try_recv().is_ok() {}
        // Wake early only for a hidden host, whose close is genuinely waiting on
        // us and would otherwise sit behind a 300s dormant sleep, or for an
        // explicit `herdr-mirror wake <host>`: someone has just started that
        // container and is waiting on the mirror, and 300s of it is the
        // difference between "it works" and "it is broken". Every other poke is
        // ordinary local traffic — `local_events_task` fans one out to every
        // host on every event, `layout.updated` included, so treating them all
        // as urgent collapses the reconnect ladder and burns a dial (or a
        // `docker ps`) per split drag. The marker is what keeps the ask apart
        // from the traffic, and taking it spends it: a wake is one early retry,
        // not a permanently shortened ladder.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(delay);
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                signal = poke.recv() => {
                    let hidden = crate::state::is_hidden(&ctx.env_state_dir, &ctx.host.name);
                    if let Some(signal) = signal {
                        if signal_restarts_ladder(&signal, hidden) {
                            ladder.reset();
                        }
                    }
                    if hidden {
                        break;
                    }
                    if crate::state::take_wake(&ctx.env_state_dir, &ctx.host.name) {
                        // an explicit wake is one early retry from the first
                        // rung: somebody has just started this workspace and is
                        // waiting on its mirror
                        ladder.reset();
                        break;
                    }
                }
            }
        }
        while poke.try_recv().is_ok() {}
    }
}

/// How often the daemon sweeps for frozen mirrors, on top of the sweep it
/// already does when it (re)subscribes to the local server.
///
/// A subscribe-only sweep heals exactly one cause: the local server restarting
/// under us. It cannot see a streamer that died on its own — a killed pane, a
/// transport cut, a supervisor whose child could not be replaced — and those
/// panes then sit frozen until something unrelated restarts the daemon. Live on
/// `caddypayio-vm`, 2026-08-29: several mirror panes stayed frozen for hours
/// and came back only when the daemon was restarted for an unrelated reason.
///
/// Sweeping on a timer is only safe because the retype is gated on herdr's own
/// per-pane process info (`streamer_exec_needed`): a live streamer is never
/// typed into, whatever a pidfile says.
const HEAL_SECONDS: u64 = 60;

/// A minute, unless this daemon converges more slowly than that — healing more
/// often than the mirror is reconciled would spend `pane.process_info` calls to
/// discover the same thing twice.
fn heal_interval_seconds(poll_seconds: u64) -> u64 {
    HEAL_SECONDS.max(poll_seconds)
}

/// Whether the streamer drawing `local_pane_id` has stopped rendering.
///
/// Reads the record that streamer publishes and applies the stall policy to it.
/// Split out so the whole path — the streamer's own file format, the two
/// clocks, and the verdict — is testable without a live pane, which is the only
/// way to cover a shape that took a real transport fault to produce.
fn streamer_output_stalled(state_dir: &std::path::Path, local_pane_id: &str, now: f64) -> bool {
    crate::state::read_stream_health(state_dir, local_pane_id).is_some_and(|health| {
        crate::state::output_direction_stalled(
            &health,
            now,
            crate::state::OUTPUT_STALL_GRACE_SECS,
        )
    })
}

/// After a local herdr server restart, session-restore resurrects mirror panes
/// as plain shells: their ids match the map, but no streamer processes exist —
/// and converge can't tell (the snapshot has no process info), so the mirrors
/// sit frozen forever. A streamer can also die on its own long after that,
/// leaving one frozen pane beside healthy ones. Heal = re-exec the streamer
/// into each pane that is not already running one, on subscribe and then on a
/// timer. A transient socket blip leaves wrappers running, so the check stays
/// quiet then.
async fn heal_zombie_mirrors(
    local: &ApiClient,
    state_dir: &std::path::Path,
    hosts: &[HostConfig],
    pokers: &[mpsc::Sender<HostSignal>],
    log: &Logger,
) {
    for (i, h) in hosts.iter().enumerate() {
        let state = load_state(state_dir, &h.name);
        let panes: Vec<(String, String, Option<String>)> = state
            .panes
            .iter()
            .filter(|(_, e)| !e.is_tombstoned())
            .map(|(rid, e)| (rid.clone(), e.local_id.clone(), e.reported.clone()))
            .collect();
        if panes.is_empty() {
            continue;
        }
        // Ask per pane, so one live streamer no longer blocks healing every
        // other dead pane on the same host.
        //
        // Fail SAFE: anything other than a definite "nothing running there" is
        // treated as alive. Leaving a frozen mirror is recoverable and visible;
        // exec'ing into a pane whose streamer owns stdin writes the command
        // line into the user's live remote session instead.
        let mut dead: Vec<(String, String, Option<String>)> = Vec::new();
        for (remote_pane_id, local_pane_id, agent_hint) in panes {
            let process_info_live =
                crate::mirror::has_live_streamer(local, &local_pane_id).await;
            let pidfile_live = crate::util::streamer_alive(state_dir, &h.target, &remote_pane_id)
                || crate::util::pane_streamer_alive(state_dir, &local_pane_id);
            if crate::mirror::streamer_exec_needed(process_info_live, pidfile_live) {
                dead.push((remote_pane_id, local_pane_id, agent_hint));
                continue;
            }
            // A live streamer is not the same thing as a working mirror. On
            // 2026-09-08 one pane kept forwarding input to the remote while its
            // output direction was dead: every process alive, the pidfile
            // fresh, herdr reporting a streamer — so every check above passed
            // and only recreating the pane cleared it. The streamer publishes
            // what it knows; the verdict is here, because a stall is invisible
            // from inside a loop that is still running.
            if !streamer_output_stalled(state_dir, &local_pane_id, crate::state::unix_now()) {
                continue;
            }
            // Drop the record with the request: the replacement republishes on
            // its own first tick, and until then a re-read of this one would
            // ask for the same restart again.
            crate::state::clear_stream_health(state_dir, &local_pane_id);
            if crate::state::request_stream_restart(state_dir, &local_pane_id).is_err() {
                continue;
            }
            if crate::util::poke_pane_streamer(state_dir, &local_pane_id) {
                log.log(&format!(
                    "[{}] mirror pane {local_pane_id} ({remote_pane_id}) stopped rendering while \
                     its input still flowed — restarting its streamer in place",
                    h.name
                ));
            } else {
                // No supervisor to take it. Spend the request rather than
                // leaving it to fire at whoever occupies this pane next.
                crate::state::take_stream_restart(state_dir, &local_pane_id);
            }
        }
        if dead.is_empty() {
            continue;
        }
        log.log(&format!(
            "[{}] {} mirror pane(s) mapped but not streaming (server restart?) — re-exec'ing streamers",
            h.name,
            dead.len()
        ));
        // Surgical on purpose: session-restore brought the workspace, tabs, panes
        // and layout back intact — only the streamer processes died. Exec the
        // streamer back into each existing pane rather than closing the workspace
        // and rebuilding it: that rebuild raced its own close (the fresh snapshot
        // still listed the dying workspace, so the adopt path reused it and
        // layout.apply then failed on its dead tab).
        //
        // Sizes live in the remote layout, which we don't have here; the wrapper
        // falls back to its default and the next converge reconciles.
        let cmd_for = crate::mirror::cmd_for_pane(h, state_dir, &HashMap::new());
        for (remote_pane_id, local_pane_id, agent_hint) in dead {
            let argv = cmd_for(&remote_pane_id);
            crate::mirror::spawn_streamer_pane(
                local,
                state_dir,
                &local_pane_id,
                &argv,
                agent_hint.as_deref(),
                log,
            )
            .await;
        }
        let _ = pokers[i].try_send(HostSignal::Converge);
    }
}

// Local events: mirror closes drive tombstoning — poke every host so the
/// next converge records the user's intent promptly.
async fn local_events_task(
    local: ApiClient,
    pokers: Vec<mpsc::Sender<HostSignal>>,
    prefixes: Vec<String>,
    hosts: Vec<HostConfig>,
    state_dir: PathBuf,
    log: Logger,
    guards: LocalEventGuards,
) {
    loop {
        let subs = vec![
            json!({ "type": "workspace.created" }),
            json!({ "type": "workspace.closed" }),
            json!({ "type": "pane.closed" }),
            // closing a TAB emits only tab_closed — no pane_closed for the
            // panes inside it — so without this a tab close never counts as
            // user intent and close-through silently degrades to tombstoning
            json!({ "type": "tab.closed" }),
            // renaming a mirror tab locally is intent for the remote tab, which
            // converge resolves against the label it last stamped; without this
            // the rename is never noticed and the next converge reverts it
            json!({ "type": "tab.renamed" }),
            // resizing a mirror pane locally is an edit the remote should
            // follow on a host we drive; the poke below is what gets it there
            // promptly instead of on the next unrelated event
            json!({ "type": "layout.updated" }),
        ];
        match local.subscribe(subs).await {
            Ok(mut stream) => {
                // catch a sidebar left ungrouped from a previous run
                regroup_sidebar(&local, &prefixes, &log).await;
                // subscribe succeeding after a drop = the server is back up;
                // give session-restore a beat, then sweep for zombie mirrors
                tokio::time::sleep(Duration::from_secs(3)).await;
                heal_zombie_mirrors(&local, &state_dir, &hosts, &pokers, &log).await;
                while let Some(e) = stream.next().await {
                    // A close EVENT is the authoritative "the user closed this";
                    // snapshot absence is not (rebuild/restart/failed converge).
                    // Our own closes are marked beforehand and swallowed here.
                    let key = match e.event.as_str() {
                        "workspace_closed" => Some("workspace_id"),
                        "pane_closed" => Some("pane_id"),
                        "tab_closed" => Some("tab_id"),
                        _ => None,
                    };
                    if let Some(k) = key {
                        if let Some(lid) = e.data.get(k).and_then(|v| v.as_str()) {
                            if let Ok(mut t) = guards.closes.lock() {
                                t.note_close_event(lid);
                            }
                        }
                    }
                    for p in &pokers {
                        let _ = p.try_send(HostSignal::Converge);
                    }
                    // a workspace appeared/left — keep hosts grouped (no-op if already)
                    regroup_sidebar(&local, &prefixes, &log).await;
                }
                log.log("local event stream dropped — resubscribing");
            }
            Err(e) => log.log(&format!("local subscribe failed ({e}) — retrying")),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

// --- commands ---

pub async fn cmd_run(env: Env) -> Result<()> {
    let owner = DaemonOwner::acquire(&env)?;
    let detached = std::env::var("HERDR_MIRROR_DETACHED").is_ok();
    let log = Logger::new(&env.state_dir, !detached);
    let config = load_config(&env.config_search)?;

    log.log(&format!(
        "daemon starting (pid {}, hosts: {}, config: {})",
        std::process::id(),
        config.hosts.iter().map(|h| h.name.as_str()).collect::<Vec<_>>().join(", "),
        config.source.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "?".into())
    ));
    // two configs on disk is a silent trap: the loser is ignored with no sign
    for ignored in &config.shadowed {
        log.log(&format!("warning: ignoring shadowed config at {}", ignored.display()));
    }
    // a skipped host would otherwise just be quietly absent from the sidebar
    for w in &config.warnings {
        log.log(&format!("warning: {w}"));
    }

    let local = ApiClient::connect(&env.local_socket).await?;
    // Detect-and-report only: a broken CLI link means every keybinding through
    // it dies silently, but the repair stays behind the explicit `start`
    // command — the daemon never rewrites the filesystem in the background.
    if let Some(problem) = crate::util::cli_link_problem() {
        log.log(&format!("warning: {problem} — keybindings using it can't fire; run start to repair"));
        let _ = local
            .request(
                "notification.show",
                json!({
                    "title": "mirror: CLI link broken",
                    "body": format!("{problem} — run Mirror: start (or herdr-mirror start) to repair"),
                }),
            )
            .await;
    }
    let closes = crate::closes::new_closes();
    let names = crate::mirror::SessionNamePlanner::new(
        config.hosts.iter().map(|host| host.name.clone()),
    );
    let mut pokers: Vec<mpsc::Sender<HostSignal>> = Vec::new();
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for h in &config.hosts {
        let (tx, rx) = mpsc::channel(8);
        pokers.push(tx);
        let ctx = HostCtx {
            env_state_dir: env.state_dir.clone(),
            host: h.clone(),
            local: local.clone(),
            log: log.clone(),
            close_remote_on_local_close: config.close_remote_on_local_close,
            closes: closes.clone(),
            names: names.clone(),
            #[cfg(test)]
            remote_override: None,
            #[cfg(test)]
            connect_attempts: None,
        };
        tasks.push(tokio::spawn(host_task(ctx, rx)));
    }
    let prefixes: Vec<String> = config.hosts.iter().map(|h| h.prefix.clone()).collect();
    tasks.push(tokio::spawn(local_events_task(
        local.clone(),
        pokers.clone(),
        prefixes,
        config.hosts.clone(),
        env.state_dir.clone(),
        log.clone(),
        LocalEventGuards { closes: closes.clone() },
    )));

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigusr1 = signal(SignalKind::user_defined1())?;
    owner.publish()?;
    let mut poll = tokio::time::interval(Duration::from_secs(config.poll_seconds.max(5)));
    poll.tick().await; // consume the immediate first tick (initial sync already runs)
    // The subscribe-time sweep in local_events_task covers a local server
    // restart. This one covers every other way a streamer dies while the daemon
    // keeps running, which is what left panes frozen until an unrelated restart.
    let mut heal =
        tokio::time::interval(Duration::from_secs(heal_interval_seconds(config.poll_seconds)));
    heal.tick().await; // consume the immediate first tick (subscribe just swept)

    loop {
        tokio::select! {
            _ = poll.tick() => {
                for p in &pokers {
                    let _ = p.try_send(HostSignal::Converge);
                }
            }
            _ = heal.tick() => {
                // Inline, not spawned: two overlapping sweeps would both see
                // the same dead pane and race to claim its launch. Every step
                // is a local socket request, and each pane it does revive is
                // one the owner would otherwise have had to restart the daemon
                // for.
                heal_zombie_mirrors(&local, &env.state_dir, &config.hosts, &pokers, &log).await;
            }
            _ = sigusr1.recv() => {
                // restore pokes us instead of converging itself — single writer
                log.log("sync poke received");
                for p in &pokers {
                    let _ = p.try_send(HostSignal::Resync);
                }
            }
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }

    log.log("daemon stopping — clearing agent authority on mirror panes");
    // stop sync work first, or a live host task could re-report after the clear
    for t in &tasks {
        t.abort();
    }
    for h in &config.hosts {
        let state = load_state(&env.state_dir, &h.name);
        for entry in state.panes.values() {
            if entry.is_tombstoned() {
                continue;
            }
            let _ = local
                .request(
                    "pane.clear_agent_authority",
                    json!({ "pane_id": entry.local_id, "source": mirror_source(&h.name) }),
                )
                .await;
        }
    }
    Ok(())
}

pub fn cmd_start(env: &Env) -> Result<()> {
    // Serialize launchers separately from the daemon lifetime lock. The child
    // owns daemon.lock and publishes its own PID only after startup succeeds.
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(env.state_dir.join("daemon.start.lock"))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(err("cannot lock daemon.start.lock"));
    }
    match try_daemon_lock(env)? {
        None => {
            println!("mirror daemon already running");
            return Ok(());
        }
        Some(probe) => drop(probe),
    }
    let exe = std::env::current_exe()?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(env.state_dir.join("daemon.log"))?;
    let log2 = log.try_clone()?;
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(log)
        .stderr(log2)
        .env("HERDR_MIRROR_DETACHED", "1")
        .process_group(0)
        .spawn()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(err(format!("mirror daemon failed to start ({status}); see daemon.log")));
        }
        if running_pid(env) == Some(child.id() as i32) {
            println!("mirror daemon started (pid {})", child.id());
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(err("mirror daemon startup timed out; see daemon.log"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn cmd_pause(env: &Env) {
    // sticky: mirrors stay, only the sync loop halts; resume with start
    set_paused(env, true);
    match running_pid(env) {
        None => println!("mirror daemon already stopped; paused (won't autostart until you run start)"),
        Some(pid) => {
            unsafe { libc::kill(pid, libc::SIGTERM) };
            println!("paused mirror daemon (pid {pid}); mirrors stay, resume with start");
        }
    }
}

pub fn cmd_ensure(env: &Env) {
    // focus-hook path: cheap, silent, honors autostart opt-out + sticky pause
    if running_pid(env).is_some() || is_paused(env) {
        return;
    }
    match load_config(&env.config_search) {
        Ok(c) if c.autostart => {
            let _ = cmd_start(env);
        }
        _ => { /* no/invalid config → nothing to start */ }
    }
}

/// A host's last observed connection state, as `status` prints it.
///
/// The daemon publishes the record; this turns it into the line, so the whole
/// field — including a countdown that has already run out — is testable without
/// a daemon and a live host.
fn host_health_line(state_dir: &std::path::Path, host: &str, now: f64) -> Option<String> {
    let health = crate::state::read_host_health(state_dir, host)?;
    let next = match health.next_retry_unix {
        // The daemon publishes the deadline, not the delay, so a record written
        // before a long sleep still reads as the time actually left. A deadline
        // already in the past is a dial that is due — while a dial is in flight,
        // or after a daemon that never got to run it.
        Some(at) => format!(", next retry {}", human_countdown(at - now)),
        None => String::new(),
    };
    Some(format!("last: {} at {}{next}", health.summary, health.at_iso))
}

/// `in 4m10s`, `in 30s`, or `now` for a deadline that has passed.
fn human_countdown(seconds: f64) -> String {
    let remaining = seconds.ceil();
    if remaining <= 0.0 {
        return "now".into();
    }
    let remaining = remaining as u64;
    match (remaining / 60, remaining % 60) {
        (0, s) => format!("in {s}s"),
        (m, 0) => format!("in {m}m"),
        (m, s) => format!("in {m}m{s}s"),
    }
}

pub fn cmd_status(env: &Env) -> Result<()> {
    match running_pid(env) {
        Some(pid) => println!("daemon: running (pid {pid})"),
        None => println!(
            "daemon: not running{}",
            if is_paused(env) { " (paused — resume with start)" } else { "" }
        ),
    }
    match crate::util::cli_link_state() {
        crate::util::CliLink::Ok(t) => println!("cli link: ok (-> {})", t.display()),
        crate::util::CliLink::Missing => {
            println!("cli link: MISSING ({}) — keybindings using it can't fire; start repairs it", crate::util::cli_link_path().display())
        }
        crate::util::CliLink::Dangling(t) => {
            println!("cli link: BROKEN (-> {}) — keybindings using it can't fire; start repairs it", t.display())
        }
        crate::util::CliLink::Other(t) => println!("cli link: -> {} (not this binary; left alone)", t.display()),
        crate::util::CliLink::File => {
            println!("cli link: {} is a regular file (not managed)", crate::util::cli_link_path().display())
        }
    }
    let config = load_config(&env.config_search)?;
    if let Some(src) = &config.source {
        println!("config: {}", src.display());
    }
    for ignored in &config.shadowed {
        println!("warning: ignoring shadowed config at {}", ignored.display());
    }
    for w in &config.warnings {
        println!("warning: {w}");
    }
    for h in &config.hosts {
        let state = load_state(&env.state_dir, &h.name);
        let ws = state.workspaces.values().filter(|w| !w.is_tombstoned()).count();
        let panes = state.panes.values().filter(|p| !p.is_tombstoned()).count();
        // `hidden` persists across reboots, so without this a host hidden weeks
        // ago is indistinguishable from one whose remote is unreachable: both
        // print zero mirrors and a healthy daemon
        let hidden = match crate::state::is_hidden(&env.state_dir, &h.name) {
            // marker set but the mirrors are still mapped: the daemon has not
            // applied it yet (it is stopped, or the host has not looped)
            true if ws > 0 || panes > 0 => {
                " — HIDDEN (pending; start the daemon to apply)".to_string()
            }
            true => format!(" — HIDDEN, `herdr-mirror show {}` brings it back", h.name),
            false => String::new(),
        };
        println!(
            "host {} ({}): {ws} mirror workspaces, {panes} mirror panes{hidden}",
            h.name, h.target
        );
        let tombs: Vec<String> = state
            .workspaces
            .iter()
            .filter(|(_, e)| e.is_tombstoned())
            .map(|(rid, _)| format!("workspace {rid}"))
            .chain(state.panes.iter().filter(|(_, e)| e.is_tombstoned()).map(|(rid, _)| format!("pane {rid}")))
            .collect();
        if !tombs.is_empty() {
            println!("  closed mirrors (restorable): {}", tombs.join(", "));
        }
        // AFTER the tombstone line on purpose: OmniDev's console reads the one
        // line following `host <name>` to decide whether that host has closed
        // mirrors, so this field has to go behind it.
        if let Some(line) = host_health_line(&env.state_dir, &h.name, crate::state::unix_now()) {
            println!("  {line}");
        }
    }
    let log_file = env.state_dir.join("daemon.log");
    if let Ok(text) = fs::read_to_string(&log_file) {
        println!("recent log:");
        for l in text.trim_end().lines().rev().take(5).collect::<Vec<_>>().into_iter().rev() {
            println!("  {l}");
        }
    }
    Ok(())
}

pub async fn cmd_once(env: Env) -> Result<()> {
    // A one-shot converge writes the same maps as the daemon.
    let _owner = DaemonOwner::acquire(&env)?;
    let log = Logger::new(&env.state_dir, true);
    let config = load_config(&env.config_search)?;
    let local = ApiClient::connect(&env.local_socket).await?;
    let mut connected = Vec::new();
    for h in &config.hosts {
        // converge would only freeze anyway, and dialling a host someone hid
        // *because* it is dead would abort the whole run with `?`
        if crate::state::is_hidden(&env.state_dir, &h.name) {
            println!("{}: hidden — skipped", h.name);
            continue;
        }
        let mut remote_host = crate::remote::RemoteHost::new(h, &env.state_dir);
        let (remote, _status) = remote_host.connect_api().await?;
        connected.push((h.clone(), remote_host, remote));
    }
    let names = crate::mirror::SessionNamePlanner::new(
        connected.iter().map(|(host, _, _)| host.name.clone()),
    );
    // First pass supplies every configured source snapshot to the shared plan;
    // the second applies that complete plan to hosts encountered before it was
    // ready. RemoteHost owners stay alive for both passes.
    for _ in 0..2 {
        for (host, _remote_host, remote) in &connected {
            converge(&ConvergeDeps {
                local: local.clone(),
                remote: remote.clone(),
                host: host.clone(),
                state_dir: env.state_dir.clone(),
                log: log.clone(),
                close_remote_on_local_close: config.close_remote_on_local_close,
                // one-shot: no local event stream, so there is no authoritative
                // close signal — an empty tracker means this pass syncs but never
                // closes a remote object, which is the correct conservative default
                closes: crate::closes::new_closes(),
                names: names.clone(),
            })
            .await?;
        }
    }
    for (host, _, _) in &connected {
        log.log(&format!("[{}] one-shot mirror complete", host.name));
    }
    Ok(())
}

/// Un-tombstone mirrors the user closed: deleting the entries makes converge
/// recreate them through the normal paths. Pokes the daemon; never converges.
pub fn cmd_restore(env: &Env, filter_host: Option<&str>, filter_id: Option<&str>) -> Result<()> {
    let config = load_config(&env.config_search)?;
    let mut cleared = 0usize;
    for h in &config.hosts {
        if filter_host.is_some_and(|f| f != h.name) {
            continue;
        }
        let mut state = load_state(&env.state_dir, &h.name);
        let ws_doomed: Vec<String> = state
            .workspaces
            .iter()
            .filter(|(rid, e)| e.is_tombstoned() && filter_id.is_none_or(|f| f == rid.as_str()))
            .map(|(rid, _)| rid.clone())
            .collect();
        let pane_doomed: Vec<String> = state
            .panes
            .iter()
            .filter(|(rid, e)| e.is_tombstoned() && filter_id.is_none_or(|f| f == rid.as_str()))
            .map(|(rid, _)| rid.clone())
            .collect();
        for rid in &ws_doomed {
            state.workspaces.remove(rid);
        }
        for rid in &pane_doomed {
            state.panes.remove(rid);
        }
        cleared += ws_doomed.len() + pane_doomed.len();
        save_state(&env.state_dir, &h.name, &state)?;
    }
    if cleared == 0 {
        println!("nothing to restore (no tombstoned mirrors matched)");
        return Ok(());
    }
    // a hidden host freezes converge, so the tombstones really are gone but
    // nothing reappears until `show` — say so rather than claiming a sync
    let hidden: Vec<&str> = config
        .hosts
        .iter()
        .filter(|h| filter_host.is_none_or(|f| f == h.name))
        .filter(|h| crate::state::is_hidden(&env.state_dir, &h.name))
        .map(|h| h.name.as_str())
        .collect();
    if !hidden.is_empty() {
        println!(
            "note: {} is hidden — run `herdr-mirror show {}` to bring mirrors back",
            hidden.join(", "),
            hidden[0]
        );
    }
    match running_pid(env) {
        Some(pid) => {
            unsafe { libc::kill(pid, libc::SIGUSR1) };
            println!("restored {cleared} mirror(s) — daemon syncing now");
        }
        None => println!("restored {cleared} mirror(s) — they will reappear when the daemon starts"),
    }
    Ok(())
}

pub async fn cmd_teardown(env: Env) -> Result<()> {
    let log = Logger::new(&env.state_dir, true);
    if let Some(pid) = running_pid(&env) {
        unsafe { libc::kill(pid, libc::SIGTERM) };
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    set_paused(&env, true); // torn down stays down until an explicit start
    let config = load_config(&env.config_search)?;
    let local = ApiClient::connect(&env.local_socket).await?;
    for h in &config.hosts {
        teardown(&local, &env.state_dir, &h.name, &log, None).await?;
    }
    log.log("teardown complete (autostart paused until next start)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// The defect, as a sequence.
    ///
    /// Six identical failures — one stopped workspace failing its ssh master
    /// with exit 255, which on 2026-09-09 was eleven hosts at once — climb from
    /// 30 s to the 300 s ceiling and stay there.
    ///
    /// The log count follows from the rule "the first failure, every change of
    /// delay, every change of reason, and nothing else": the first failure (30),
    /// then the four delay changes 30→60, 60→120, 120→240 and 240→300, is five.
    /// The sixth failure repeats both the reason and the 300 s delay and is the
    /// line the owner does not need again. Under the old ladder those same six
    /// failures wrote six lines, and the next thousand wrote a thousand more.
    #[test]
    fn identical_failures_climb_to_the_ceiling_and_stop_repeating_themselves() {
        let mut ladder = ReconnectLadder::default();
        let reason = "ssh master to omnidev-greenroom-air failed: exit 255";
        let decisions: Vec<RetryDecision> =
            (0..6).map(|_| ladder.fail(reason)).collect();

        assert_eq!(
            decisions.iter().map(|d| d.delay).collect::<Vec<_>>(),
            vec![30, 60, 120, 240, 300, 300]
        );
        assert_eq!(
            decisions.iter().map(|d| d.log).collect::<Vec<_>>(),
            vec![true, true, true, true, true, false]
        );
        assert_eq!(decisions.iter().filter(|d| d.log).count(), 5);

        // and it stays quiet for as long as the workspace stays stopped: a full
        // day of retries at the ceiling adds nothing to the log
        for _ in 0..288 {
            let decision = ladder.fail(reason);
            assert_eq!(decision.delay, 300);
            assert!(!decision.log);
        }
    }

    /// News is always logged, however long the host has been failing.
    ///
    /// A host at the ceiling whose reason changes has told us something new —
    /// most usefully the ssh failure that replaces "dormant" when a container
    /// comes back — so it is logged, and it starts its own ladder rather than
    /// inheriting the pace of the fault before it.
    #[test]
    fn a_changed_reason_is_logged_and_restarts_the_ladder_even_at_the_ceiling() {
        let mut ladder = ReconnectLadder::default();
        for _ in 0..5 {
            ladder.fail("ssh master to omnidev-greenroom-air failed: exit 255");
        }
        assert_eq!(
            ladder.fail("ssh master to omnidev-greenroom-air failed: exit 255"),
            RetryDecision { delay: 300, log: false }
        );

        let changed = ladder.fail("herdr api handshake timed out");
        assert_eq!(changed, RetryDecision { delay: 30, log: true });

        // dormancy is its own state: fixed 300 s sleep, logged once on entry,
        // and it never advances the ladder — so the first ssh failure after the
        // container comes back waits 30 s, not five minutes
        let dormant = "dormant: no running container for greenroom-air";
        assert_eq!(ladder.dormant(dormant), RetryDecision { delay: 300, log: true });
        assert_eq!(ladder.dormant(dormant), RetryDecision { delay: 300, log: false });
        assert_eq!(
            ladder.fail("ssh master to omnidev-greenroom-air failed: exit 255"),
            RetryDecision { delay: 30, log: true }
        );
    }

    /// Every reset the specification names, and the one act that is not one.
    ///
    /// `wake`, `show`, `restore` and a successful connect all call
    /// `ReconnectLadder::reset`; a configuration reload is `herdr-mirror pause`
    /// followed by `start`, so it resets by construction — the ladder lives in
    /// the host task and is never written to disk, which the default below is
    /// the whole of. What reaches a sleeping host task from `show` and
    /// `restore` is the daemon's explicit `Resync`, and telling that apart from
    /// the poke every local Herdr event fans out is what keeps a split drag from
    /// collapsing the ladder.
    #[test]
    fn a_wake_a_show_a_restore_a_reload_and_a_connect_each_restart_the_ladder() {
        let mut ladder = ReconnectLadder::default();
        let reason = "ssh master to omnidev-greenroom-air failed: exit 255";
        for _ in 0..5 {
            ladder.fail(reason);
        }
        assert_eq!(ladder.fail(reason), RetryDecision { delay: 300, log: false });

        // wake / show / restore / a successful connect
        ladder.reset();
        assert_eq!(ladder.fail(reason), RetryDecision { delay: 30, log: true });

        // a configuration reload restarts the daemon, and this is all the state
        // a fresh host task has
        let mut reloaded = ReconnectLadder::default();
        assert_eq!(reloaded.fail(reason), RetryDecision { delay: 30, log: true });

        // the wiring: only the explicit resync of a visible host
        assert!(signal_restarts_ladder(&HostSignal::Resync, false));
        assert!(!signal_restarts_ladder(&HostSignal::Resync, true)); // `hide`
        assert!(!signal_restarts_ladder(&HostSignal::Converge, false)); // poll, layout, heal
    }

    /// The fields `status` prints, from the record the daemon publishes.
    ///
    /// `status` runs in its own process, so before this the only answer to "why
    /// is this host not mirroring" was a grep of the log the backoff exists to
    /// stop filling.
    #[test]
    fn status_shows_each_host_its_last_reason_and_its_next_retry() {
        let dir = std::env::temp_dir().join(format!("hm-host-status-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let now = 10_000.0;

        // a host the daemon has never dialled says nothing rather than guessing
        assert_eq!(host_health_line(&dir, "greenroom-air", now), None);

        crate::state::publish_host_health(
            &dir,
            "greenroom-air",
            &crate::state::HostHealth {
                summary: "disconnected (ssh master to omnidev-greenroom-air failed: exit 255)"
                    .into(),
                at_iso: "2026-09-09T11:42:42.000Z".into(),
                next_retry_unix: Some(now + 250.0),
            },
        );
        assert_eq!(
            host_health_line(&dir, "greenroom-air", now).unwrap(),
            "last: disconnected (ssh master to omnidev-greenroom-air failed: exit 255) \
at 2026-09-09T11:42:42.000Z, next retry in 4m10s"
        );

        // the deadline is published, not the delay, so the countdown is the time
        // actually left — and a dial that is already due says so
        assert_eq!(
            host_health_line(&dir, "greenroom-air", now + 249.5).unwrap(),
            "last: disconnected (ssh master to omnidev-greenroom-air failed: exit 255) \
at 2026-09-09T11:42:42.000Z, next retry in 1s"
        );
        assert!(host_health_line(&dir, "greenroom-air", now + 400.0)
            .unwrap()
            .ends_with("next retry now"));

        crate::state::publish_host_health(
            &dir,
            "greenroom-studio",
            &crate::state::HostHealth {
                summary: "connected and synced".into(),
                at_iso: "2026-09-09T11:43:12.000Z".into(),
                next_retry_unix: None,
            },
        );
        assert_eq!(
            host_health_line(&dir, "greenroom-studio", now).unwrap(),
            "last: connected and synced at 2026-09-09T11:43:12.000Z"
        );

        assert_eq!(human_countdown(300.0), "in 5m");
        assert_eq!(human_countdown(30.0), "in 30s");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The one-directional stall, driven through the real files.
    ///
    /// Reproduces 2026-09-08's shape: a streamer that is running, that is still
    /// carrying input, and whose remote pane keeps producing output no frame
    /// ever delivers. Everything the sweep already checked — process alive,
    /// pidfile fresh, herdr reporting a streamer — is true throughout, which is
    /// why the record and this verdict are the only thing that can catch it.
    #[test]
    fn a_streamer_that_stopped_rendering_is_caught_while_it_still_looks_alive() {
        let dir = std::env::temp_dir().join(format!("hm-stall-sweep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let pane = "w7R:p6";
        let now = 10_000.0;

        // no report yet: a pane whose streamer has not ticked once is not
        // evidence of anything, and must never be restarted on that basis
        assert!(!streamer_output_stalled(&dir, pane, now));

        // healthy: the remote produced output and the frame arrived
        crate::state::publish_stream_health(
            &dir,
            pane,
            &crate::state::StreamHealth {
                last_frame_unix: now - 5.0,
                remote_advanced_unix: Some(now - 6.0),
            },
        );
        assert!(!streamer_output_stalled(&dir, pane, now));

        // idle: nothing produced remotely for hours. The mirror shows nothing
        // because there is nothing to show — the case that makes plain output
        // silence useless as a signal.
        crate::state::publish_stream_health(
            &dir,
            pane,
            &crate::state::StreamHealth {
                last_frame_unix: now - 7_200.0,
                remote_advanced_unix: None,
            },
        );
        assert!(!streamer_output_stalled(&dir, pane, now));

        // frozen: the remote kept producing, the last frame predates it
        crate::state::publish_stream_health(
            &dir,
            pane,
            &crate::state::StreamHealth {
                last_frame_unix: now - 600.0,
                remote_advanced_unix: Some(now - 300.0),
            },
        );
        assert!(streamer_output_stalled(&dir, pane, now));

        // and the request the sweep then makes reaches this pane and no other,
        // exactly once
        crate::state::request_stream_restart(&dir, pane).unwrap();
        assert!(!crate::state::take_stream_restart(&dir, "w7R:p7"));
        assert!(crate::state::take_stream_restart(&dir, pane));
        assert!(!crate::state::take_stream_restart(&dir, pane));

        let _ = std::fs::remove_dir_all(&dir);
    }

    struct ProtocolPeer {
        path: PathBuf,
        requests: Arc<Mutex<Vec<Value>>>,
        projection_events: Arc<std::sync::atomic::AtomicBool>,
        events: tokio::sync::broadcast::Sender<Value>,
        task: tokio::task::JoinHandle<()>,
    }

    impl ProtocolPeer {
        async fn start(label: &str, snapshot: Value) -> Self {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            use tokio::net::UnixListener;

            let path = std::env::temp_dir().join(format!(
                "herdr-mirror-daemon-{}-{label}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&path);
            let listener = UnixListener::bind(&path).unwrap();
            let snapshot = Arc::new(Mutex::new(snapshot));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let projection_events = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (events, _) = tokio::sync::broadcast::channel::<Value>(32);
            let snapshots = snapshot.clone();
            let captured = requests.clone();
            let emit_projection_events = projection_events.clone();
            let event_bus = events.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else { break };
                    let snapshots = snapshots.clone();
                    let captured = captured.clone();
                    let emit_projection_events = emit_projection_events.clone();
                    let mut event_rx = event_bus.subscribe();
                    let event_tx = event_bus.clone();
                    tokio::spawn(async move {
                        let (read, mut write) = stream.into_split();
                        let mut lines = BufReader::new(read).lines();
                        let Ok(Some(line)) = lines.next_line().await else { return };
                        let request: Value = serde_json::from_str(&line).unwrap();
                        captured.lock().unwrap().push(request.clone());
                        if request["method"] == "events.subscribe" {
                            let subscriptions: std::collections::HashSet<String> = request
                                ["params"]["subscriptions"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(|subscription| subscription["type"].as_str())
                                .map(str::to_string)
                                .collect();
                            let response = json!({"id": request["id"], "result": {"type": "subscription_started"}});
                            write.write_all(format!("{response}\n").as_bytes()).await.unwrap();
                            while let Ok(event) = event_rx.recv().await {
                                if event["disconnect"] == true {
                                    break;
                                }
                                let subscribed = event["event"]
                                    .as_str()
                                    .and_then(|name| name.split_once('_'))
                                    .map(|(scope, name)| format!("{scope}.{name}"))
                                    .is_some_and(|name| subscriptions.contains(&name));
                                if !subscribed {
                                    continue;
                                }
                                if write.write_all(format!("{event}\n").as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                            return;
                        }

                        let method = request["method"].as_str().unwrap_or("");
                        let params = &request["params"];
                        let result = if method == "session.snapshot" {
                            json!({"snapshot": snapshots.lock().unwrap().clone()})
                        } else {
                            let mut snapshot = snapshots.lock().unwrap();
                            if method == "workspace.report_metadata" {
                                let workspace_id = params["workspace_id"].as_str();
                                if let (Some(workspace_id), Some(workspaces), Some(tokens)) = (
                                    workspace_id,
                                    snapshot["workspaces"].as_array_mut(),
                                    params["tokens"].as_object(),
                                ) {
                                    if let Some(workspace) = workspaces.iter_mut().find(|workspace| {
                                        workspace["workspace_id"] == workspace_id
                                    }) {
                                        let target = workspace
                                            .as_object_mut()
                                            .unwrap()
                                            .entry("tokens")
                                            .or_insert_with(|| json!({}))
                                            .as_object_mut()
                                            .unwrap();
                                        for (key, value) in tokens {
                                            if value.is_null() {
                                                target.remove(key);
                                            } else {
                                                target.insert(key.clone(), value.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            let pane_id = params["pane_id"]
                                .as_str()
                                .or_else(|| params["target"].as_str());
                            if let (Some(pane_id), Some(agents)) =
                                (pane_id, snapshot["agents"].as_array_mut())
                            {
                                if let Some(agent) = agents.iter_mut().find(|agent| agent["pane_id"] == pane_id) {
                                    match method {
                                        "agent.rename" => agent["name"] = params["name"].clone(),
                                        "pane.report_agent" => {
                                            agent["agent"] = params["agent"].clone();
                                            agent["agent_status"] = params["state"].clone();
                                            let session = params["agent_session_id"].clone();
                                            agent["interactive_ready"] = json!(!session.is_null());
                                            agent["agent_session"] = if session.is_null() {
                                                Value::Null
                                            } else {
                                                json!({"value": session})
                                            };
                                        }
                                        "pane.report_metadata" => {
                                            if let Some(tokens) = params["tokens"].as_object() {
                                                let target = agent
                                                    .as_object_mut()
                                                    .unwrap()
                                                    .entry("tokens")
                                                    .or_insert_with(|| json!({}));
                                                let target = target.as_object_mut().unwrap();
                                                for (key, value) in tokens {
                                                    if value.is_null() {
                                                        target.remove(key);
                                                    } else {
                                                        target.insert(key.clone(), value.clone());
                                                    }
                                                }
                                            }
                                        }
                                        "pane.clear_agent_authority" => {
                                            agent["interactive_ready"] = json!(false);
                                            agent["agent_session"] = Value::Null;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            if emit_projection_events.load(std::sync::atomic::Ordering::SeqCst)
                                && matches!(method, "pane.report_agent" | "pane.report_metadata")
                            {
                                if let Some(pane_id) = pane_id {
                                    let _ = event_tx.send(json!({
                                        "event": "pane_updated",
                                        "data": {"pane_id": pane_id}
                                    }));
                                }
                            }
                            json!({"type": "ok"})
                        };
                        let response = json!({"id": request["id"], "result": result});
                        write.write_all(format!("{response}\n").as_bytes()).await.unwrap();
                    });
                }
            });
            Self {
                path,
                requests,
                projection_events,
                events,
                task,
            }
        }

        fn requests(&self) -> Vec<Value> {
            self.requests.lock().unwrap().clone()
        }

        fn emit_projection_updates(&self, enabled: bool) {
            self.projection_events
                .store(enabled, std::sync::atomic::Ordering::SeqCst);
        }

        fn send_event(&self, event: Value) {
            let _ = self.events.send(event);
        }

    }

    impl Drop for ProtocolPeer {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn test_host(name: &str) -> HostConfig {
        HostConfig {
            name: name.into(),
            target: name.into(),
            kind: crate::config::HostKind::Ssh,
            docker_bin: "docker".into(),
            prefix: name.into(),
            remote_bin: None,
            session: None,
            api_transport: crate::config::ApiTransport::Auto,
            always_control: true,
            max_cols: None,
            max_rows: None,
        }
    }

    fn remote_snapshot(pane: &str, remote_name: &str, binding: &str) -> Value {
        json!({
            "workspaces": [{"workspace_id": "rw", "label": "feature", "tab_count": 1,
                "pane_count": 1, "active_tab_id": "rt", "tokens": {"rosemary_project": "garden"}}],
            "tabs": [{"tab_id": "rt", "workspace_id": "rw", "label": "main"}],
            "panes": [{"pane_id": pane, "tab_id": "rt", "workspace_id": "rw",
                "label": null, "cwd": "/project", "foreground_cwd": "/project"}],
            "agents": [{"pane_id": pane, "agent": "codex", "display_agent": "Codex",
                "name": remote_name, "agent_status": "idle", "interactive_ready": true,
                "agent_session": {"value": format!("session-{pane}")},
                "tokens": {"rosemary_binding": binding, "rosemary_outcome": "complete",
                    "rosemary_commit": "abc123", "rosemary_summary": "done"}}],
            "layouts": []
        })
    }

    fn local_facade_snapshot() -> Value {
        json!({
            "reachability": "connected",
            "compatible": true,
            "workspaces": [
                {"workspace_id": "lw-a", "label": "alpha: feature", "tab_count": 1, "pane_count": 1, "active_tab_id": "lt-a"},
                {"workspace_id": "lw-b", "label": "alpha-beta: feature", "tab_count": 1, "pane_count": 1, "active_tab_id": "lt-b"},
                {"workspace_id": "native-w", "label": "native", "tab_count": 1, "pane_count": 1, "active_tab_id": "native-t"}],
            "tabs": [
                {"tab_id": "lt-a", "workspace_id": "lw-a", "label": "main"},
                {"tab_id": "lt-b", "workspace_id": "lw-b", "label": "main"},
                {"tab_id": "native-t", "workspace_id": "native-w", "label": "main"}],
            "panes": [
                {"pane_id": "lp-a", "tab_id": "lt-a", "workspace_id": "lw-a", "cwd": "/tmp", "foreground_cwd": "/tmp"},
                {"pane_id": "lp-b", "tab_id": "lt-b", "workspace_id": "lw-b", "cwd": "/tmp", "foreground_cwd": "/tmp"},
                {"pane_id": "native-p", "tab_id": "native-t", "workspace_id": "native-w", "cwd": "/native", "foreground_cwd": "/native"}],
            "agents": [
                {"pane_id": "lp-a", "agent": "codex", "agent_status": "unknown", "present": true, "tokens": {}},
                {"pane_id": "lp-b", "agent": "codex", "agent_status": "unknown", "present": true, "tokens": {}},
                {"pane_id": "native-p", "agent": "codex", "name": "native-agent", "agent_status": "idle",
                    "interactive_ready": true, "agent_session": {"value": "native-session"}, "present": true, "tokens": {}}],
            "layouts": []
        })
    }

    fn seed_host_state(state_dir: &std::path::Path, host: &str, remote_pane: &str, local_pane: &str, local_ws: &str, local_tab: &str) {
        let mut state = HostState::default();
        state.workspaces.insert(
            "rw".into(),
            crate::state::WsEntry {
                local_id: local_ws.into(),
                tombstone: None,
                root_tab_local_id: None,
                last_remote_label: Some("feature".into()),
            },
        );
        state.tabs.insert(
            "rt".into(),
            crate::state::TabEntry {
                local_id: local_tab.into(),
                last_remote_label: Some("main".into()),
            },
        );
        state.panes.insert(
            remote_pane.into(),
            crate::state::PaneEntry {
                local_id: local_pane.into(),
                ..crate::state::PaneEntry::default()
            },
        );
        save_state(state_dir, host, &state).unwrap();
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..400 {
            if predicate() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("condition did not become true");
    }

    fn request_count(peer: &ProtocolPeer, method: &str) -> usize {
        peer.requests()
            .iter()
            .filter(|request| request["method"] == method)
            .count()
    }

    #[tokio::test]
    async fn local_projection_echo_is_ignored_but_remote_status_event_still_projects() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-no-local-projection-feedback-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        let local = ProtocolPeer::start("no-feedback-local", local_facade_snapshot()).await;
        let remote = ProtocolPeer::start(
            "no-feedback-remote",
            remote_snapshot("rp-a", "conductor", "run-1"),
        )
        .await;
        seed_host_state(&state_dir, "alpha", "rp-a", "lp-a", "lw-a", "lt-a");
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let remote_api = ApiClient::connect(&remote.path).await.unwrap();
        let (tx, rx) = mpsc::channel(16);
        let host_task_handle = tokio::spawn(host_task(
            HostCtx {
                env_state_dir: state_dir.clone(),
                host: test_host("alpha"),
                local: local_api.clone(),
                log: Logger::new(&state_dir, false),
                close_remote_on_local_close: false,
                closes: crate::closes::new_closes(),
                names: crate::mirror::SessionNamePlanner::new(["alpha".to_string()]),
                remote_override: Some(remote_api),
                connect_attempts: None,
            },
            rx,
        ));
        let event_task_handle = tokio::spawn(local_events_task(
            local_api,
            vec![tx],
            vec!["alpha".into()],
            vec![test_host("alpha")],
            state_dir.clone(),
            Logger::new(&state_dir, false),
            LocalEventGuards { closes: crate::closes::new_closes() },
        ));

        wait_until(|| request_count(&local, "pane.report_metadata") >= 2).await;
        wait_until(|| request_count(&local, "events.subscribe") >= 1).await;
        // local_events_task deliberately waits three seconds after subscribing
        // before it consumes events, so let the real listener reach its loop.
        tokio::time::sleep(Duration::from_millis(3_100)).await;
        local.emit_projection_updates(true);
        let before_echo = request_count(&remote, "session.snapshot");
        local.send_event(json!({
            "event": "pane_updated",
            "data": {"pane_id": "lp-a"}
        }));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            request_count(&remote, "session.snapshot"),
            before_echo,
            "a local projection echo reached the remote converge path"
        );

        let changed_request_index = local.requests().len();
        let before_remote_event = request_count(&remote, "session.snapshot");
        remote.send_event(json!({
            "event": "pane_agent_status_changed",
            "data": {
                "pane_id": "rp-a",
                "agent": "codex",
                "display_agent": "Codex",
                "name": "conductor",
                "agent_status": "working",
                "interactive_ready": true,
                "agent_session": {"value": "session-rp-a"},
                "tokens": {
                    "rosemary_binding": "run-1",
                    "rosemary_outcome": "complete",
                    "rosemary_commit": "abc123",
                    "rosemary_summary": "done"
                }
            }
        }));
        wait_until(|| {
            local.requests()[changed_request_index..].iter().any(|request| {
                request["method"] == "pane.report_agent"
                    && request["params"]["state"] == "working"
            })
        })
        .await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            request_count(&remote, "session.snapshot"),
            before_remote_event,
            "the complete upstream lifecycle event should use the status fast path"
        );

        host_task_handle.abort();
        event_task_handle.abort();
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn hidden_host_admission_makes_no_remote_request_until_show() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-hidden-admission-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        crate::state::set_hidden(&state_dir, "studio", true).unwrap();

        let local = ProtocolPeer::start("hidden-local", local_facade_snapshot()).await;
        let remote = ProtocolPeer::start(
            "hidden-remote",
            remote_snapshot("rp-hidden", "conductor", "run-hidden"),
        )
        .await;
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let remote_api = ApiClient::connect(&remote.path).await.unwrap();
        // Constructing the test override performs its own protocol handshake;
        // host-task admission starts after that fixed fixture setup.
        let remote_baseline = remote.requests().len();
        let connect_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel(8);
        let ctx = HostCtx {
            env_state_dir: state_dir.clone(),
            host: test_host("studio"),
            local: local_api,
            log: Logger::new(&state_dir, false),
            close_remote_on_local_close: false,
            closes: crate::closes::new_closes(),
            names: crate::mirror::SessionNamePlanner::new(["studio".to_string()]),
            remote_override: Some(remote_api),
            connect_attempts: Some(connect_attempts.clone()),
        };
        let task = tokio::spawn(host_task(ctx, rx));

        // Exercise both kinds of traffic which reach a host task while hidden:
        // an ordinary local event and the explicit Resync used by wake/hide.
        // Neither may cross the remote admission boundary while the marker is
        // present, and recv() keeps this path asleep between those signals.
        tx.send(HostSignal::Converge).await.unwrap();
        tx.send(HostSignal::Resync).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            connect_attempts.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "hidden startup must admit zero remote connections"
        );

        // `show` clears the marker before its daemon poke. Drive those same two
        // operations here and observe the real transport-admission seam plus a
        // request reaching the fake host.
        crate::state::set_hidden(&state_dir, "studio", false).unwrap();
        tx.send(HostSignal::Resync).await.unwrap();
        wait_until(|| connect_attempts.load(std::sync::atomic::Ordering::SeqCst) == 1).await;
        assert!(remote.requests().len() > remote_baseline, "show must reach the fake host");
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            connect_attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "show must admit once rather than busy-loop"
        );

        task.abort();
        let _ = std::fs::remove_dir_all(state_dir);
    }

    /// One fallback is not evidence: the probe also fails when the remote herdr
    /// is restarting or the mux hiccups, and pinning a healthy host to the
    /// slower transport for the daemon's life is worse than re-probing once.
    #[test]
    fn a_single_fallback_is_not_remembered() {
        use crate::config::ApiTransport;
        let mut streak = 0u32;

        // transient: fell back once, then the forward worked again
        assert_eq!(remember_transport(Some(ApiTransport::Exec), &mut streak), None);
        assert_eq!(remember_transport(Some(ApiTransport::Socket), &mut streak), Some(ApiTransport::Socket));
        assert_eq!(streak, 0);

        // genuinely broken: two in a row sticks, and stays stuck
        assert_eq!(remember_transport(Some(ApiTransport::Exec), &mut streak), None);
        assert_eq!(
            remember_transport(Some(ApiTransport::Exec), &mut streak),
            Some(ApiTransport::Exec)
        );
        assert_eq!(
            remember_transport(Some(ApiTransport::Exec), &mut streak),
            Some(ApiTransport::Exec)
        );

        // a later success clears it, so a fixed host returns to the forward
        assert_eq!(remember_transport(Some(ApiTransport::Socket), &mut streak), Some(ApiTransport::Socket));
        assert_eq!(remember_transport(Some(ApiTransport::Exec), &mut streak), None);
    }

    /// The sweep is a backstop, not a second poll loop: a minute by default,
    /// and never more often than the mirror is actually converged.
    #[test]
    fn the_heal_sweep_runs_a_minute_apart_but_never_faster_than_converge() {
        assert_eq!(heal_interval_seconds(60), 60); // the shipped default
        assert_eq!(heal_interval_seconds(30), 60);
        assert_eq!(heal_interval_seconds(5), 60);
        // a deliberately slow daemon heals on its own rhythm
        assert_eq!(heal_interval_seconds(300), 300);
    }

    /// Healing on a timer is only safe because of the process-info gate: with a
    /// live streamer in the pane, no sweep — however often it runs — may type.
    #[test]
    fn a_periodic_sweep_can_never_type_into_a_live_streamer() {
        use crate::mirror::streamer_exec_needed;

        // herdr sees our wrapper: never, whatever the pidfiles say
        assert!(!streamer_exec_needed(Some(true), false));
        assert!(!streamer_exec_needed(Some(true), true));
        // herdr could not answer: still never
        assert!(!streamer_exec_needed(None, false));
        assert!(!streamer_exec_needed(None, true));
        // the one case a sweep acts on: a pane that is provably a bare shell
        assert!(streamer_exec_needed(Some(false), false));
        // ...and not even that while a pidfile is still live
        assert!(!streamer_exec_needed(Some(false), true));
    }

    #[test]
    fn sparse_status_event_requires_a_full_snapshot_for_facade_fields() {
        assert!(status_event_needs_snapshot(&json!({
            "pane_id": "p1", "agent_status": "idle"
        })));
        assert!(!status_event_needs_snapshot(&json!({
            "pane_id": "p1",
            "agent_status": "idle",
            "name": "conductor-rosie",
            "interactive_ready": true,
            "agent_session": {"value": "session-1"},
            "tokens": {}
        })));
    }
}
