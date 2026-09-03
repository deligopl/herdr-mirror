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
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::api::{ApiClient, EventStream};
use crate::config::{load_config, HostConfig};
use crate::mirror::{
    apply_remote_closes, converge, mark_unknown, mirror_source, push_pane_status, regroup_sidebar,
    teardown, AgentInfo, ConvergeDeps, PaneStatusDeps, RosemaryProjectionGate,
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

// Sticky pause marker: blocks the focus-hook autostart until an explicit
// start clears it (a crash leaves no marker, so it still auto-recovers).
fn pause_path(env: &Env) -> PathBuf {
    env.state_dir.join("daemon.paused")
}

pub fn is_paused(env: &Env) -> bool {
    pause_path(env).exists()
}

pub fn set_paused(env: &Env, paused: bool) {
    if paused {
        let _ = fs::write(pause_path(env), now_iso());
    } else {
        let _ = fs::remove_file(pause_path(env));
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
    rosemary_gate: RosemaryProjectionGate,
    // Hermetic acceptance can drive the production host lifecycle against a
    // public-protocol peer without invoking ssh. Production always leaves it
    // unset and uses RemoteHost below.
    #[cfg(test)]
    remote_override: Option<ApiClient>,
}

#[derive(Debug)]
enum HostSignal {
    Converge,
    LocalPaneUpdated {
        pane_id: String,
        acknowledged: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
struct LocalEventGuards {
    closes: crate::closes::Closes,
    rosemary_gate: RosemaryProjectionGate,
}

async fn capture_local_update(ctx: &HostCtx, signal: HostSignal) {
    if let HostSignal::LocalPaneUpdated { pane_id, acknowledged } = signal {
        loop {
            match crate::mirror::capture_local_rosemary_clear(
                &ctx.local,
                &ctx.env_state_dir,
                &ctx.host.name,
                &pane_id,
                &ctx.log,
            )
            .await
            {
                Ok(_) => {
                    let _ = acknowledged.send(());
                    break;
                }
                Err(error) => {
                    ctx.log.log(&format!(
                        "[{}] local Rosemary clear remains unacknowledged; projection blocked: {error}",
                        ctx.host.name
                    ));
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
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
                rosemary_gate: &ctx.rosemary_gate,
            },
            &remote_id,
            &mut state,
            agent,
            desired_name,
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
    backoff_idx: &mut usize,
    remembered_transport: &mut Option<crate::config::ApiTransport>,
    exec_streak: &mut u32,
) -> Result<()> {
    #[cfg(test)]
    if let Some(remote) = &ctx.remote_override {
        *backoff_idx = 0;
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
    *backoff_idx = 0;
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
        rosemary_gate: ctx.rosemary_gate.clone(),
    };
    // broadcast-only first: subscribing a since-dead pane id is rejected, so
    // converge must prune the map before the per-pane upgrade
    let mut stream = remote.subscribe(sub_list(&[])).await?;
    let mut subscribed_key = String::from("<broadcast>");
    let mut name_plan_changes = ctx.names.subscribe();
    // A local clear can arrive while the remote dial is completing. Persist
    // every queued clear before the first projection of this connection.
    while let Ok(signal) = poke.try_recv() {
        capture_local_update(ctx, signal).await;
    }
    let state = converge(&deps).await?;
    resubscribe(ctx, &remote, &mut stream, &mut subscribed_key, &state).await?;
    ctx.log.log(&format!("[{}] connected and synced", ctx.host.name));
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
            Some(signal) = poke.recv() => {
                // This host task is the single writer: observe and durably
                // suppress a local clear before any later projection.
                capture_local_update(ctx, signal).await;
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
/// ssh keeps its original ladder: an unreachable machine is a fault, and you
/// want it back fast. A stopped container is not a fault, it is the resting
/// state, so retrying every 30s forever would burn a `docker ps` per host per
/// half-minute and fill the log with non-events.
const RECONNECT_DELAYS: [u64; 3] = [5, 10, 30];
const DORMANT_DELAY: u64 = 300;

async fn host_task(ctx: HostCtx, mut poke: mpsc::Receiver<HostSignal>) {
    let mut backoff_idx = 0usize;
    let mut was_dormant = false;
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
        let e = match run_connected(
            &ctx,
            &mut poke,
            &mut backoff_idx,
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
        let dormant = e.to_string().starts_with(crate::remote::DORMANT);
        let delay = if dormant {
            DORMANT_DELAY
        } else {
            RECONNECT_DELAYS[backoff_idx.min(RECONNECT_DELAYS.len() - 1)]
        };
        // dormant cycles must not advance the ladder they do not use: a
        // container stopped overnight would otherwise leave backoff_idx pinned
        // at the 30s rung, so the first real failure while it boots waits 30s
        // instead of the 5s the ladder exists to give.
        if !dormant {
            backoff_idx += 1;
        }
        // log dormancy once on entry, not on every poll of a stopped container
        if !dormant || !was_dormant {
            ctx.log.log(&format!("[{}] disconnected ({e}) — retrying in {delay}s", ctx.host.name));
        }
        was_dormant = dormant;
        // drain FIRST: pokes that piled up during a multi-second dial say nothing
        // about now, and honouring them would skip the sleep entirely
        while let Ok(signal) = poke.try_recv() {
            capture_local_update(&ctx, signal).await;
        }
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
                    if let Some(signal) = signal {
                        capture_local_update(&ctx, signal).await;
                    }
                    if crate::state::is_hidden(&ctx.env_state_dir, &ctx.host.name)
                        || crate::state::take_wake(&ctx.env_state_dir, &ctx.host.name)
                    {
                        break;
                    }
                }
            }
        }
        while let Ok(signal) = poke.try_recv() {
            capture_local_update(&ctx, signal).await;
        }
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
            // Rosemary clears run metadata in place. The event is only a
            // doorbell; each host task re-reads the authoritative pane.
            json!({ "type": "pane.updated" }),
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
                    let pane_updated = (e.event == "pane_updated")
                        .then(|| e.data.get("pane_id").and_then(|value| value.as_str()))
                        .flatten()
                        .map(str::to_string);
                    if let Some(pane_id) = &pane_updated {
                        // pane.updated is advisory: it queues an authoritative
                        // snapshot read and lets an in-flight converge notice
                        // that work before its next Rosemary write. Herdr has
                        // no revision/CAS, so a clear overwritten before that
                        // read is intentionally not claimed as observed.
                        guards.rosemary_gate.note_local_update(pane_id);
                    }
                    let mut acknowledgements = Vec::new();
                    for p in &pokers {
                        if let Some(pane_id) = &pane_updated {
                            // A bounded suppression signal is never optional:
                            // backpressure this one event rather than dropping
                            // it behind cosmetic layout traffic.
                            let (acknowledged, acknowledgement) = oneshot::channel();
                            if p.send(HostSignal::LocalPaneUpdated {
                                pane_id: pane_id.clone(),
                                acknowledged,
                            }).await.is_ok() {
                                acknowledgements.push(acknowledgement);
                            }
                        } else {
                            let _ = p.try_send(HostSignal::Converge);
                        }
                    }
                    for acknowledgement in acknowledgements {
                        let _ = acknowledgement.await;
                    }
                    if let Some(pane_id) = &pane_updated {
                        guards.rosemary_gate.finish_local_update(pane_id);
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
    let detached = std::env::var("HERDR_MIRROR_DETACHED").is_ok();
    let log = Logger::new(&env.state_dir, !detached);
    let config = load_config(&env.config_search)?;
    fs::write(pid_path(&env), std::process::id().to_string())?;
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
    let rosemary_gate = RosemaryProjectionGate::default();
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
            rosemary_gate: rosemary_gate.clone(),
            #[cfg(test)]
            remote_override: None,
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
        LocalEventGuards {
            closes: closes.clone(),
            rosemary_gate,
        },
    )));

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigusr1 = signal(SignalKind::user_defined1())?;
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
                    let _ = p.try_send(HostSignal::Converge);
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
    let _ = fs::remove_file(pid_path(&env));
    Ok(())
}

pub fn cmd_start(env: &Env) -> Result<()> {
    // flock + parent-written pidfile: two racing starts (focus hook) must not
    // both see "not running" and spawn duplicate daemons
    use std::os::fd::AsRawFd;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(env.state_dir.join("daemon.lock"))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(err("cannot lock daemon.lock"));
    }
    if running_pid(env).is_some() {
        println!("mirror daemon already running");
        return Ok(());
    }
    let exe = std::env::current_exe()?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(env.state_dir.join("daemon.log"))?;
    let log2 = log.try_clone()?;
    use std::os::unix::process::CommandExt;
    let child = std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(log)
        .stderr(log2)
        .env("HERDR_MIRROR_DETACHED", "1")
        .process_group(0)
        .spawn()?;
    fs::write(pid_path(env), child.id().to_string())?;
    println!("mirror daemon started (pid {})", child.id());
    Ok(())
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
    let rosemary_gate = RosemaryProjectionGate::default();
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
                rosemary_gate: rosemary_gate.clone(),
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
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine;
    use std::sync::{Arc, Mutex};

    type MetadataPause =
        Arc<Mutex<Option<(String, oneshot::Sender<()>, Arc<tokio::sync::Notify>)>>>;

    struct ProtocolPeer {
        path: PathBuf,
        snapshot: Arc<Mutex<Value>>,
        requests: Arc<Mutex<Vec<Value>>>,
        routes: Arc<Mutex<HashMap<String, String>>>,
        terminal_inputs: Arc<Mutex<Vec<Value>>>,
        metadata_pause: MetadataPause,
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
            let routes: Arc<Mutex<HashMap<String, String>>> =
                Arc::new(Mutex::new(HashMap::new()));
            let terminal_inputs = Arc::new(Mutex::new(Vec::new()));
            let metadata_pause: MetadataPause = Arc::new(Mutex::new(None));
            let (events, _) = tokio::sync::broadcast::channel::<Value>(32);
            let snapshots = snapshot.clone();
            let captured = requests.clone();
            let prompt_routes = routes.clone();
            let pane_inputs = terminal_inputs.clone();
            let pauses = metadata_pause.clone();
            let event_bus = events.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else { break };
                    let snapshots = snapshots.clone();
                    let captured = captured.clone();
                    let prompt_routes = prompt_routes.clone();
                    let pane_inputs = pane_inputs.clone();
                    let pauses = pauses.clone();
                    let mut event_rx = event_bus.subscribe();
                    let event_tx = event_bus.clone();
                    tokio::spawn(async move {
                        let (read, mut write) = stream.into_split();
                        let mut lines = BufReader::new(read).lines();
                        let Ok(Some(line)) = lines.next_line().await else { return };
                        let request: Value = serde_json::from_str(&line).unwrap();
                        captured.lock().unwrap().push(request.clone());
                        if request["method"] == "events.subscribe" {
                            let response = json!({"id": request["id"], "result": {"type": "subscription_started"}});
                            write.write_all(format!("{response}\n").as_bytes()).await.unwrap();
                            while let Ok(event) = event_rx.recv().await {
                                if event["disconnect"] == true {
                                    break;
                                }
                                if write.write_all(format!("{event}\n").as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                            return;
                        }

                        let method = request["method"].as_str().unwrap_or("");
                        let params = &request["params"];
                        let pause = if method == "pane.report_metadata" {
                            let source = params["source"].as_str().unwrap_or("");
                            let mut pause = pauses.lock().unwrap();
                            if pause.as_ref().is_some_and(|(expected, _, _)| expected == source) {
                                pause.take().map(|(_, started, release)| {
                                    let _ = started.send(());
                                    release
                                })
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        if let Some(release) = pause {
                            release.notified().await;
                        }
                        let result = if method == "session.snapshot" {
                            json!({"snapshot": snapshots.lock().unwrap().clone()})
                        } else {
                            if method == "agent.prompt" {
                                if let Some(target) = params["target"].as_str() {
                                    let route = prompt_routes.lock().unwrap().get(target).cloned();
                                    if let Some(remote_target) = route {
                                        let text = params["text"].as_str().unwrap_or("");
                                        let mut bytes = text.as_bytes().to_vec();
                                        bytes.push(b'\r');
                                        let mut terminal = crate::pane::test_typed_prompt_through_data_plane(&bytes).await;
                                        terminal["pane_id"] = json!(remote_target);
                                        pane_inputs.lock().unwrap().push(terminal);
                                    }
                                }
                            }
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
                            if method == "pane.report_metadata" && params["seq"] == 999 {
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
                snapshot,
                requests,
                routes,
                terminal_inputs,
                metadata_pause,
                events,
                task,
            }
        }

        fn set_snapshot(&self, snapshot: Value) {
            *self.snapshot.lock().unwrap() = snapshot;
        }

        fn attach_pane_streamer(&self, local_pane: &str, remote_pane: &str) {
            self.routes
                .lock()
                .unwrap()
                .insert(local_pane.to_string(), remote_pane.to_string());
        }

        fn requests(&self) -> Vec<Value> {
            self.requests.lock().unwrap().clone()
        }

        fn terminal_inputs(&self) -> Vec<Value> {
            self.terminal_inputs.lock().unwrap().clone()
        }

        fn disconnect_subscribers(&self) {
            let _ = self.events.send(json!({"disconnect": true}));
        }

        fn pause_next_metadata_from(
            &self,
            source: &str,
        ) -> (oneshot::Receiver<()>, Arc<tokio::sync::Notify>) {
            let (started, observed) = oneshot::channel();
            let release = Arc::new(tokio::sync::Notify::new());
            *self.metadata_pause.lock().unwrap() =
                Some((source.to_string(), started, release.clone()));
            (observed, release)
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

    #[tokio::test]
    async fn daemon_public_protocol_journey_covers_clear_restart_routing_and_readiness() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-daemon-rosemary-journey-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        seed_host_state(&state_dir, "alpha", "rp-a", "lp-a", "lw-a", "lt-a");
        seed_host_state(&state_dir, "alpha-beta", "rp-b", "lp-b", "lw-b", "lt-b");

        let remote_a = ProtocolPeer::start(
            "remote-a",
            remote_snapshot("rp-a", "beta-conductor-rosie", " run-1 "),
        )
        .await;
        let mut remote_b_snapshot = remote_snapshot("rp-b", "conductor-rosie", "run-b");
        remote_b_snapshot["agents"][0]["interactive_ready"] = json!(false);
        remote_b_snapshot["agents"][0]["agent_session"] = Value::Null;
        let remote_b = ProtocolPeer::start("remote-b", remote_b_snapshot).await;
        let local = ProtocolPeer::start("local", local_facade_snapshot()).await;
        local.attach_pane_streamer("lp-a", "rp-a");
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let native_before = local.snapshot.lock().unwrap()["agents"][2].clone();
        let closes = crate::closes::new_closes();
        let names = crate::mirror::SessionNamePlanner::new([
            "alpha".to_string(),
            "alpha-beta".to_string(),
        ]);
        let log = Logger::new(&state_dir, false);
        let rosemary_gate = RosemaryProjectionGate::default();
        let remote_a_api = ApiClient::connect(&remote_a.path).await.unwrap();
        let remote_b_api = ApiClient::connect(&remote_b.path).await.unwrap();
        let (tx_a, rx_a) = mpsc::channel(8);
        let (tx_b, rx_b) = mpsc::channel(8);
        let ctx_a = HostCtx {
            env_state_dir: state_dir.clone(),
            host: test_host("alpha"),
            local: local_api.clone(),
            log: log.clone(),
            close_remote_on_local_close: false,
            closes: closes.clone(),
            names: names.clone(),
            rosemary_gate: rosemary_gate.clone(),
            remote_override: Some(remote_a_api.clone()),
        };
        let ctx_b = HostCtx {
            env_state_dir: state_dir.clone(),
            host: test_host("alpha-beta"),
            local: local_api.clone(),
            log: log.clone(),
            close_remote_on_local_close: false,
            closes: closes.clone(),
            names: names.clone(),
            rosemary_gate: rosemary_gate.clone(),
            remote_override: Some(remote_b_api.clone()),
        };
        let task_a = tokio::spawn(host_task(ctx_a, rx_a));
        let task_b = tokio::spawn(host_task(ctx_b, rx_b));
        let event_task = tokio::spawn(local_events_task(
            local_api.clone(),
            vec![tx_a.clone(), tx_b.clone()],
            vec!["alpha".into(), "alpha-beta".into()],
            vec![test_host("alpha"), test_host("alpha-beta")],
            state_dir.clone(),
            log.clone(),
            LocalEventGuards {
                closes: closes.clone(),
                rosemary_gate: rosemary_gate.clone(),
            },
        ));

        wait_until(|| {
            let snapshot = local.snapshot.lock().unwrap();
            let agents = snapshot["agents"].as_array().unwrap();
            let a = agents.iter().find(|agent| agent["pane_id"] == "lp-a").unwrap();
            let b = agents.iter().find(|agent| agent["pane_id"] == "lp-b").unwrap();
            a["name"].as_str().is_some()
                && b["name"].as_str().is_some()
                && a["name"] != b["name"]
                && a["interactive_ready"] == true
        })
        .await;

        local_api
            .request("agent.prompt", json!({"target": "lp-a", "text": "continue"}))
            .await
            .unwrap();
        wait_until(|| {
            local.terminal_inputs().iter().any(|input| {
                input["type"] == "terminal.input"
                    && input["pane_id"] == "rp-a"
                    && input["bytes"] == B64.encode(b"continue\r")
            })
        })
        .await;

        // Force the clear to overlap a converge which has already taken its
        // snapshots but has not reached the Rosemary write. This is the exact
        // ordering which used to let that pass restore the tuple before the
        // queued pane.updated signal could be consumed.
        let (projection_started, release_projection) =
            local.pause_next_metadata_from("plugin:mirror:alpha");
        tx_a.send(HostSignal::Converge).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), projection_started)
            .await
            .unwrap()
            .unwrap();

        // Use the same public metadata request Rosemary uses. The fake local
        // Herdr mutates its snapshot and emits pane_updated while the earlier
        // converge remains suspended inside its ordinary metadata projection.
        local_api
            .request(
                "pane.report_metadata",
                json!({
                    "pane_id": "lp-a",
                    "source": "rosemary-run",
                    "tokens": {
                        "rosemary_binding": null,
                        "rosemary_outcome": null,
                        "rosemary_commit": null,
                        "rosemary_summary": null
                    },
                    "seq": 999
                }),
            )
            .await
            .unwrap();
        wait_until(|| rosemary_gate.has_local_update("lp-a")).await;
        release_projection.notify_one();
        wait_until(|| {
            load_state(&state_dir, "alpha")
                .rosemary_suppressions
                .get("beta-conductor-rosie")
                .is_some_and(|run| run.binding == " run-1 ")
        })
        .await;
        assert!(local.snapshot.lock().unwrap()["agents"][0]["tokens"]
            .get("rosemary_binding")
            .is_none());

        // Actual task restart and socket re-subscription, with both peer pane
        // identities recreated. The queued clear is inserted before the new
        // connected phase and must be acknowledged before its initial converge.
        task_a.abort();
        task_b.abort();
        event_task.abort();
        let mut local_recreated = local.snapshot.lock().unwrap().clone();
        for pane in local_recreated["panes"].as_array_mut().unwrap() {
            if pane["pane_id"] == "lp-a" {
                pane["pane_id"] = json!("lp-a2");
            }
        }
        for agent in local_recreated["agents"].as_array_mut().unwrap() {
            if agent["pane_id"] == "lp-a" {
                agent["pane_id"] = json!("lp-a2");
            }
        }
        local.set_snapshot(local_recreated);
        let mut remote_recreated = remote_snapshot("rp-a2", "beta-conductor-rosie", " run-1 ");
        remote_recreated["panes"][0]["pane_id"] = json!("rp-a2");
        remote_a.set_snapshot(remote_recreated);
        let mut restarted_state = load_state(&state_dir, "alpha");
        let mut pane = restarted_state.panes.remove("rp-a").unwrap();
        pane.local_id = "lp-a2".into();
        restarted_state.panes.insert("rp-a2".into(), pane);
        save_state(&state_dir, "alpha", &restarted_state).unwrap();
        local.attach_pane_streamer("lp-a2", "rp-a2");

        let names = crate::mirror::SessionNamePlanner::new([
            "alpha".to_string(),
            "alpha-beta".to_string(),
        ]);
        let (tx_a2, rx_a2) = mpsc::channel(8);
        let (ack_tx, ack_rx) = oneshot::channel();
        tx_a2
            .send(HostSignal::LocalPaneUpdated {
                pane_id: "lp-a2".into(),
                acknowledged: ack_tx,
            })
            .await
            .unwrap();
        let (tx_b2, rx_b2) = mpsc::channel(8);
        let ctx_a2 = HostCtx {
            env_state_dir: state_dir.clone(), host: test_host("alpha"), local: local_api.clone(),
            log: log.clone(), close_remote_on_local_close: false, closes: closes.clone(), names: names.clone(),
            rosemary_gate: rosemary_gate.clone(),
            remote_override: Some(remote_a_api.clone()),
        };
        let ctx_b2 = HostCtx {
            env_state_dir: state_dir.clone(), host: test_host("alpha-beta"), local: local_api.clone(),
            log: log.clone(), close_remote_on_local_close: false, closes: closes.clone(), names: names.clone(),
            rosemary_gate: rosemary_gate.clone(),
            remote_override: Some(remote_b_api.clone()),
        };
        let restart_request_index = local.requests().len();
        let task_a2 = tokio::spawn(host_task(ctx_a2, rx_a2));
        let task_b2 = tokio::spawn(host_task(ctx_b2, rx_b2));
        tokio::time::timeout(Duration::from_secs(2), ack_rx).await.unwrap().unwrap();
        wait_until(|| {
            local.requests()[restart_request_index..]
                .iter()
                .any(|request| request["method"] == "pane.report_metadata")
        })
        .await;
        let restarted_requests = local.requests();
        let restarted_requests = &restarted_requests[restart_request_index..];
        let capture_snapshot = restarted_requests
            .iter()
            .position(|request| request["method"] == "session.snapshot")
            .unwrap();
        let first_projection = restarted_requests
            .iter()
            .position(|request| request["method"] == "pane.report_metadata")
            .unwrap();
        assert!(capture_snapshot < first_projection);
        assert!(load_state(&state_dir, "alpha").rosemary_suppressions.contains_key("beta-conductor-rosie"));

        // Drop the remote event stream under the full host task. Its ordinary
        // disconnect/backoff/reconnect loop must open a new subscription and
        // preserve suppression without a direct connected_session call.
        let subscriptions_before = remote_a.requests().iter().filter(|r| r["method"] == "events.subscribe").count();
        remote_a.disconnect_subscribers();
        wait_until(|| {
            remote_a.requests().iter().filter(|r| r["method"] == "events.subscribe").count()
                > subscriptions_before
        })
        .await;
        assert!(load_state(&state_dir, "alpha").rosemary_suppressions.contains_key("beta-conductor-rosie"));

        remote_a.set_snapshot(remote_snapshot("rp-a2", "beta-conductor-rosie", "run-2"));
        tx_a2.send(HostSignal::Converge).await.unwrap();
        wait_until(|| {
            !load_state(&state_dir, "alpha").rosemary_suppressions.contains_key("beta-conductor-rosie")
                && local.snapshot.lock().unwrap()["agents"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|agent| agent["pane_id"] == "lp-a2")
                    .and_then(|agent| agent["tokens"]["rosemary_binding"].as_str())
                    == Some("run-2")
        })
        .await;

        let final_snapshot = local.snapshot.lock().unwrap().clone();
        let conductor = final_snapshot["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|agent| agent["pane_id"] == "lp-a2")
            .unwrap();
        assert!(conductor["name"]
            .as_str()
            .is_some_and(|name| name.ends_with("-conductor-rosie")));
        assert_eq!(conductor["present"], true);
        assert_eq!(conductor["interactive_ready"], true);
        assert_eq!(conductor["agent_session"]["value"], "session-rp-a2");
        assert_eq!(conductor["agent_status"], "idle");
        let workspace = final_snapshot["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|workspace| workspace["workspace_id"] == "lw-a")
            .unwrap();
        assert_eq!(workspace["tokens"]["rosemary_project"], "garden");
        assert_eq!(final_snapshot["agents"][2], native_before);
        task_a2.abort();
        task_b2.abort();
        let _ = tx_a2;
        let _ = tx_b2;
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
