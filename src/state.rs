// The persisted per-host id map — the heart of reconciliation.
//
// remote id → { local id, tombstone, seq, reported }. A tombstone means "the
// user closed this mirror" — never recreate it until restore. Absence of a
// remote id means "remote went away" — close the mirror. Restart-idempotent.
// The camelCase JSON shape matches the TS implementation so an existing
// <host>-map.json carries over.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::util::Result;

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaneEntry {
    pub local_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone: Option<bool>,
    #[serde(default)]
    pub seq: u64,
    /// agent label last reported onto this pane; must be explicitly released
    /// when the remote agent goes away, or it sticks forever
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reported: Option<String>,
    /// remote agent name last applied with `agent.rename`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reported_name: Option<String>,
    /// exact remote name that produced the local dispatch identity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_agent_name: Option<String>,
    /// exact Rosemary tuple most recently written onto this local pane
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projected_rosemary_run: Option<RosemaryRun>,
    /// The session-global planner has not granted this pane a dispatch name.
    #[serde(default, skip_serializing_if = "is_false")]
    pub identity_ineligible: bool,
    /// At least one local identity/authority clear has not succeeded yet.
    #[serde(default, skip_serializing_if = "is_false")]
    pub identity_cleanup_pending: bool,
}

impl PaneEntry {
    pub fn is_tombstoned(&self) -> bool {
        self.tombstone == Some(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsEntry {
    pub local_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone: Option<bool>,
    /// the auto-created root tab of a fresh mirror workspace; consumed by the
    /// first remote tab's layout.apply so it doesn't stack an extra tab
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_tab_local_id: Option<String>,
    /// remote label as of the last converge — distinguishes "remote renamed"
    /// (remote wins, restamp local) from "user renamed the mirror locally"
    /// (push the rename to the remote instead of stomping it)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_remote_label: Option<String>,
}

impl WsEntry {
    pub fn is_tombstoned(&self) -> bool {
        self.tombstone == Some(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TabEntry {
    pub local_id: String,
    /// remote label as of the last converge, exactly as on `WsEntry`: it is
    /// what tells "remote renamed" apart from "user renamed the mirror tab"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_remote_label: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostState {
    #[serde(default)]
    pub workspaces: BTreeMap<String, WsEntry>,
    #[serde(default)]
    pub tabs: BTreeMap<String, TabEntry>,
    #[serde(default)]
    pub panes: BTreeMap<String, PaneEntry>,
    /// remote object ids (ws/tab/pane) seen in the previous converge. A mirror is
    /// only closed on snapshot-absence when the object was absent last pass too,
    /// so a remote that reconnects mid-restore doesn't mass-close mirrors.
    #[serde(default)]
    pub prev_remote_ids: std::collections::BTreeSet<String>,
    /// last split ratio both sides agreed on, keyed `<remote tab id>|<path>`
    /// (see layout_sync::path_key). This is the base of the three-way merge
    /// that makes ratio sync two-way: without it a converge can see that the
    /// two sides differ but not which one was resized, so it has to pick a
    /// permanent winner and revert the other side's drag.
    #[serde(default)]
    pub ratios: BTreeMap<String, f64>,
    /// One durable completion suppression per exact remote agent name. The host
    /// is the state-file key, so together these form the source-pair identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rosemary_suppressions: BTreeMap<String, RosemaryRun>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RosemaryRun {
    pub binding: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Marker for `hide`: this host's mirrors are off the sidebar until `show`.
///
/// Deliberately its OWN file rather than a field on `HostState`. The map file is
/// load-modify-written by the daemon, by every CLI subcommand, and by converge
/// around a pass that spans dozens of awaits, with no lock anywhere — so a flag
/// living inside it is silently reset by whoever saves last, and `hide` reports
/// success having done nothing. A marker file has no such race: it is written by
/// one process and only ever read by the others. Same shape as `daemon.paused`.
pub fn hidden_path(state_dir: &Path, host: &str) -> PathBuf {
    state_dir.join(format!("{host}.hidden"))
}

pub fn is_hidden(state_dir: &Path, host: &str) -> bool {
    hidden_path(state_dir, host).exists()
}

/// Returns the error rather than swallowing it: this one write gates the whole
/// feature, so a read-only state dir or a host name that is not a single path
/// component would otherwise make `hide` claim success forever while nothing
/// ever acts on it.
pub fn set_hidden(state_dir: &Path, host: &str, hidden: bool) -> std::io::Result<()> {
    let path = hidden_path(state_dir, host);
    if hidden {
        std::fs::write(path, "")
    } else {
        match std::fs::remove_file(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }
}

/// An explicit "stop waiting and retry this host now", written by
/// `herdr-mirror wake` and consumed by the host task.
///
/// A dormant host — a sandbox whose container is stopped — parks for
/// `DORMANT_DELAY`, and ordinary pokes deliberately do not shorten that wait:
/// `local_events_task` fans one out to every host on every local event, so
/// honouring them would spend a `docker ps` per host on every split drag. A
/// wake is different in kind: someone has just started that container and is
/// waiting on the mirror. Marking the request keeps the two apart, so the
/// backoff ladder still exists for everything that is not an explicit ask.
///
/// Its own file for the same reason `hidden` is: the map file is written
/// without a lock by the daemon, by converge, and by every CLI subcommand, so
/// a flag living inside it is silently reset by whoever saves last.
pub fn wake_path(state_dir: &Path, host: &str) -> PathBuf {
    state_dir.join(format!("{host}.wake"))
}

/// Returns the error rather than swallowing it: this one write is the entire
/// request, so a read-only state dir would otherwise let `wake` claim success
/// while the host sleeps out its full delay.
pub fn request_wake(state_dir: &Path, host: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(state_dir)?;
    std::fs::write(wake_path(state_dir, host), "")
}

/// Consume a pending wake. True only for the caller that took it, so one
/// `wake` buys exactly one early retry and cannot be replayed into a loop that
/// dials a stopped container forever.
pub fn take_wake(state_dir: &Path, host: &str) -> bool {
    std::fs::remove_file(wake_path(state_dir, host)).is_ok()
}

/// A one-line notice for one specific mirror pane to show.
///
/// The interception runs in its own short-lived process and closes a plain
/// local shell — nothing of ours is in that pane to draw with, and herdr has no
/// API to write into someone else's pane. But the pane the user was *looking
/// at* when they pressed the key is a live mirror with a streamer in it, and a
/// streamer can paint its own status row. So the notice is addressed to that
/// pane by its local id, and it lands in the same row as "reconnecting in 10s".
///
/// Keyed by pane id on purpose: an earlier version left the note unaddressed
/// and the next streamer to start collected it, which was the replacement pane,
/// reporting a move that had already finished.
fn pane_hint_path(state_dir: &Path, local_pane_id: &str) -> PathBuf {
    state_dir.join(format!(".hint-{}", crate::util::sane_component(local_pane_id)))
}

pub fn set_pane_hint(state_dir: &Path, local_pane_id: &str, msg: &str) {
    let _ = std::fs::create_dir_all(state_dir);
    let _ = std::fs::write(pane_hint_path(state_dir, local_pane_id), msg);
}

/// Desired `HERDR_AGENT` value for the streamer occupying a local mirror pane.
///
/// Herdr identifies wrapper processes from this environment variable.  The
/// daemon owns the desired value (copied from the remote pane), while the
/// streamer owns changing its own process environment by cleanly re-execing
/// when this file changes.
fn pane_agent_hint_path(state_dir: &Path, local_pane_id: &str) -> PathBuf {
    state_dir
        .join("pane-agent-hints")
        .join(format!("{}.agent", crate::util::sane_component(local_pane_id)))
}

/// Store the desired wrapper hint. Returns true only when it changed.
pub fn set_pane_agent_hint(
    state_dir: &Path,
    local_pane_id: &str,
    agent: Option<&str>,
) -> std::io::Result<bool> {
    let path = pane_agent_hint_path(state_dir, local_pane_id);
    let desired = agent.unwrap_or("");
    if std::fs::read_to_string(&path).ok().as_deref() == Some(desired) {
        return Ok(false);
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, desired)?;
    Ok(true)
}

/// `Some(None)` means the daemon explicitly wants no wrapper hint; `None`
/// means it has not published a desired value for this pane yet.
pub fn pane_agent_hint(state_dir: &Path, local_pane_id: &str) -> Option<Option<String>> {
    let value = std::fs::read_to_string(pane_agent_hint_path(state_dir, local_pane_id)).ok()?;
    let value = value.trim();
    Some((!value.is_empty()).then(|| value.to_string()))
}

/// Read and consume this pane's notice, if any.
pub fn take_pane_hint(state_dir: &Path, local_pane_id: &str) -> Option<String> {
    let path = pane_hint_path(state_dir, local_pane_id);
    let msg = std::fs::read_to_string(&path).ok()?;
    // A notice is about something that just happened. One left behind by a
    // streamer that died before collecting it is stale, and showing it later
    // would report a close the user has long since forgotten. Stat before the
    // unlink: afterwards there is nothing left to ask.
    let fresh = std::fs::metadata(&path)
        .and_then(|m| m.modified())
        .and_then(|t| t.elapsed().map_err(std::io::Error::other))
        .is_ok_and(|e| e < std::time::Duration::from_secs(30));
    let _ = std::fs::remove_file(&path);
    let msg = msg.trim().to_string();
    (fresh && !msg.is_empty()).then_some(msg)
}

/// What a live streamer knows about its own output direction, published for
/// the daemon's frozen-mirror sweep to judge.
///
/// The streamer cannot decide this for itself the way it decides a crash: on
/// 2026-09-08 the wedged mirror's own event loop was demonstrably alive — text
/// typed locally reached the remote pane — so a self-check running inside it
/// would have concluded it was healthy. What was dead was one direction, and
/// only something outside the pane can act on that. So the streamer reports and
/// the sweep decides, which also means a streamer whose loop later stops
/// entirely is caught by the same record going stale.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamHealth {
    /// when a frame last reached this streamer
    pub last_frame_unix: f64,
    /// when the REMOTE pane's own content revision was last seen to increase —
    /// the last moment the remote definitely produced output
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_advanced_unix: Option<f64>,
}

pub fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

fn stream_health_path(state_dir: &Path, local_pane_id: &str) -> PathBuf {
    state_dir
        .join("stream-health")
        .join(format!("{}.json", crate::util::sane_component(local_pane_id)))
}

pub fn publish_stream_health(state_dir: &Path, local_pane_id: &str, health: &StreamHealth) {
    let path = stream_health_path(state_dir, local_pane_id);
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(text) = serde_json::to_string(health) {
        let _ = std::fs::write(path, text);
    }
}

pub fn read_stream_health(state_dir: &Path, local_pane_id: &str) -> Option<StreamHealth> {
    let text = std::fs::read_to_string(stream_health_path(state_dir, local_pane_id)).ok()?;
    serde_json::from_str(&text).ok()
}

pub fn clear_stream_health(state_dir: &Path, local_pane_id: &str) {
    let _ = std::fs::remove_file(stream_health_path(state_dir, local_pane_id));
}

/// How long the remote may have produced output that never arrived before the
/// stream is called stalled.
///
/// Well above any latency this transport shows — the measured worst single
/// keystroke round trip through Mirror on this fleet is 304 ms — and above the
/// streamer's own reconnect ladder, so a stream that is merely re-attaching is
/// never mistaken for a dead one.
pub const OUTPUT_STALL_GRACE_SECS: f64 = 45.0;

/// Whether a live streamer's OUTPUT direction has stalled.
///
/// Neither half means anything alone. A mirror pane showing nothing may simply
/// have nothing to show, which is why silence is not the signal; and a remote
/// pane that produced output proves nothing on its own, because the frame may
/// still be in flight. Stalled is the conjunction: the remote definitely
/// produced output at a moment, no frame has arrived since that moment, and
/// enough time has passed that ordinary latency cannot explain it.
///
/// Pure, because the two clocks and the three-way comparison are the whole
/// decision and the live shape it was written from cannot be reproduced on
/// demand.
pub fn output_direction_stalled(health: &StreamHealth, now: f64, grace_secs: f64) -> bool {
    let Some(advanced) = health.remote_advanced_unix else {
        return false; // the remote has produced nothing we know of
    };
    health.last_frame_unix < advanced && now - advanced >= grace_secs
}

/// An explicit "replace the streamer in this pane", written by the daemon's
/// sweep and consumed by the pane supervisor on SIGUSR1.
///
/// Its own file, and consumed by exactly one taker, for the same reasons `wake`
/// is: SIGUSR1 already means "collect an addressed notice", so the marker is
/// what tells a restart request apart from that ordinary traffic, and taking it
/// spends it so a stale signal cannot restart a healthy stream twice.
fn stream_restart_path(state_dir: &Path, local_pane_id: &str) -> PathBuf {
    state_dir
        .join("stream-restarts")
        .join(format!("{}.request", crate::util::sane_component(local_pane_id)))
}

pub fn request_stream_restart(state_dir: &Path, local_pane_id: &str) -> std::io::Result<()> {
    let path = stream_restart_path(state_dir, local_pane_id);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, "")
}

pub fn take_stream_restart(state_dir: &Path, local_pane_id: &str) -> bool {
    std::fs::remove_file(stream_restart_path(state_dir, local_pane_id)).is_ok()
}

/// What the daemon last observed about one host's connection, published for
/// `herdr-mirror status` to read.
///
/// `status` runs in a different process from the daemon, so until now the only
/// record of why a host is not mirroring was the daemon log — which is exactly
/// the file the backoff exists to stop filling. One small file per host, written
/// on every connect and every failed dial, gives the owner the same answer as a
/// field instead of a grep.
///
/// Deliberately its own file rather than a field on `HostState`, for the reason
/// `hidden` is: the map file is load-modify-written by the daemon and by every
/// CLI subcommand with no lock anywhere, so a field living inside it is silently
/// reset by whoever saves last.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostHealth {
    /// exactly what the log line says: `connected and synced`, or
    /// `disconnected (<reason>)`
    pub summary: String,
    /// when that was observed, as printed
    pub at_iso: String,
    /// when the next dial is due; absent while the host is connected
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_unix: Option<f64>,
}

fn host_health_path(state_dir: &Path, host: &str) -> PathBuf {
    state_dir
        .join("host-health")
        .join(format!("{}.json", crate::util::sane_component(host)))
}

pub fn publish_host_health(state_dir: &Path, host: &str, health: &HostHealth) {
    let path = host_health_path(state_dir, host);
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(text) = serde_json::to_string(health) {
        let _ = std::fs::write(path, text);
    }
}

pub fn read_host_health(state_dir: &Path, host: &str) -> Option<HostHealth> {
    let text = std::fs::read_to_string(host_health_path(state_dir, host)).ok()?;
    serde_json::from_str(&text).ok()
}

pub fn state_path(state_dir: &Path, host: &str) -> PathBuf {
    state_dir.join(format!("{host}-map.json"))
}

pub fn load_state(state_dir: &Path, host: &str) -> HostState {
    std::fs::read_to_string(state_path(state_dir, host))
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_state(state_dir: &Path, host: &str, state: &HostState) -> Result<()> {
    std::fs::create_dir_all(state_dir)?;
    std::fs::write(state_path(state_dir, host), serde_json::to_string_pretty(state)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact shape the TS implementation writes must round-trip.
    #[test]
    fn ts_state_shape_roundtrips() {
        let ts = r#"{
 "workspaces": {
  "w9": { "localId": "w1234", "rootTabLocalId": "t99" },
  "wB": { "localId": "w5678", "tombstone": true }
 },
 "tabs": { "w9:t1": { "localId": "t42" } },
 "panes": {
  "w9:p1": { "localId": "w1234:p1", "seq": 12, "reported": "claude" },
  "wB:p1": { "localId": "w5678:p1", "tombstone": true, "seq": 3 }
 }
}"#;
        let state: HostState = serde_json::from_str(ts).unwrap();
        assert_eq!(state.workspaces["w9"].local_id, "w1234");
        assert_eq!(state.workspaces["w9"].root_tab_local_id.as_deref(), Some("t99"));
        assert!(state.workspaces["wB"].is_tombstoned());
        // a tab mapped before label history existed loads with none, which the
        // resolver reads as "remote wins once"
        assert_eq!(state.tabs["w9:t1"].last_remote_label, None);
        assert_eq!(state.panes["w9:p1"].seq, 12);
        assert_eq!(state.panes["w9:p1"].reported.as_deref(), Some("claude"));
        assert!(state.panes["wB:p1"].is_tombstoned());

        let out = serde_json::to_string(&state).unwrap();
        let reparsed: HostState = serde_json::from_str(&out).unwrap();
        assert_eq!(reparsed.panes["w9:p1"].local_id, "w1234:p1");
        assert!(out.contains("localId"));
        assert!(out.contains("rootTabLocalId"));
        // absent options stay absent
        assert!(!out.contains("\"reported\":null"));
    }

    #[test]
    fn hidden_is_a_marker_file_not_a_state_field() {
        let dir = std::env::temp_dir().join(format!("hm-hidden-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        assert!(!is_hidden(&dir, "h"));
        set_hidden(&dir, "h", true).unwrap();
        assert!(is_hidden(&dir, "h"));
        // and it survives a map rewrite, which is the whole reason it is not a
        // field on HostState
        save_state(&dir, "h", &HostState::default()).unwrap();
        assert!(is_hidden(&dir, "h"));
        set_hidden(&dir, "h", false).unwrap();
        assert!(!is_hidden(&dir, "h"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pane_agent_hint_distinguishes_agent_plain_and_unpublished() {
        let dir = std::env::temp_dir().join(format!("hm-agent-hint-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(pane_agent_hint(&dir, "w1:p1"), None);
        assert!(set_pane_agent_hint(&dir, "w1:p1", Some("codex")).unwrap());
        assert_eq!(pane_agent_hint(&dir, "w1:p1"), Some(Some("codex".into())));
        assert!(!set_pane_agent_hint(&dir, "w1:p1", Some("codex")).unwrap());
        assert!(set_pane_agent_hint(&dir, "w1:p1", None).unwrap());
        assert_eq!(pane_agent_hint(&dir, "w1:p1"), Some(None));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_wake_is_consumed_by_exactly_one_taker() {
        let dir = std::env::temp_dir().join(format!("hm-wake-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(!take_wake(&dir, "h"));
        request_wake(&dir, "h").unwrap();
        assert!(wake_path(&dir, "h").exists());
        // exactly one early retry, however many tasks look
        assert!(take_wake(&dir, "h"));
        assert!(!take_wake(&dir, "h"));

        // one host's wake never wakes another
        request_wake(&dir, "a").unwrap();
        assert!(!take_wake(&dir, "b"));
        assert!(take_wake(&dir, "a"));

        let _ = std::fs::remove_dir_all(dir);
    }

    /// The three-way comparison that separates an idle mirror from a dead one.
    #[test]
    fn only_output_the_remote_produced_and_never_delivered_counts_as_a_stall() {
        let grace = OUTPUT_STALL_GRACE_SECS;
        let now = 1_000.0;

        // idle: nothing produced remotely, nothing drawn locally. Not a stall,
        // and this is the common case — most mirror panes sit at a prompt.
        assert!(!output_direction_stalled(
            &StreamHealth { last_frame_unix: 100.0, remote_advanced_unix: None },
            now,
            grace
        ));

        // healthy: the remote produced output and a frame arrived after it
        assert!(!output_direction_stalled(
            &StreamHealth { last_frame_unix: 901.0, remote_advanced_unix: Some(900.0) },
            now,
            grace
        ));

        // in flight: produced, not yet delivered, but only a moment ago
        assert!(!output_direction_stalled(
            &StreamHealth { last_frame_unix: 900.0, remote_advanced_unix: Some(999.0) },
            now,
            grace
        ));

        // the live shape: the remote kept producing, the last frame predates it
        // by minutes, and input was still flowing — which this policy never
        // consults, because a working input direction is exactly what made the
        // freeze invisible to every existing check.
        assert!(output_direction_stalled(
            &StreamHealth { last_frame_unix: 500.0, remote_advanced_unix: Some(900.0) },
            now,
            grace
        ));

        // and the boundary is inclusive, so a sweep landing exactly on it acts
        assert!(output_direction_stalled(
            &StreamHealth { last_frame_unix: 500.0, remote_advanced_unix: Some(now - grace) },
            now,
            grace
        ));
    }

    #[test]
    fn a_stream_restart_request_is_consumed_by_exactly_one_taker() {
        let dir = std::env::temp_dir().join(format!("hm-restart-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(!take_stream_restart(&dir, "w7R:p2"));
        request_stream_restart(&dir, "w7R:p2").unwrap();
        // never the neighbour's pane: a restart closes and replaces a stream
        assert!(!take_stream_restart(&dir, "w7R:p3"));
        assert!(take_stream_restart(&dir, "w7R:p2"));
        assert!(!take_stream_restart(&dir, "w7R:p2"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn stream_health_round_trips_and_is_absent_before_the_first_report() {
        let dir = std::env::temp_dir().join(format!("hm-health-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(read_stream_health(&dir, "w7R:p2").is_none());
        publish_stream_health(
            &dir,
            "w7R:p2",
            &StreamHealth { last_frame_unix: 12.5, remote_advanced_unix: Some(30.25) },
        );
        let back = read_stream_health(&dir, "w7R:p2").unwrap();
        assert_eq!(back.last_frame_unix, 12.5);
        assert_eq!(back.remote_advanced_unix, Some(30.25));
        clear_stream_health(&dir, "w7R:p2");
        assert!(read_stream_health(&dir, "w7R:p2").is_none());

        let _ = std::fs::remove_dir_all(dir);
    }

    /// One host's published health is that host's, and it survives the write.
    ///
    /// `status` is a different process from the daemon, so this file is the
    /// whole channel between them.
    #[test]
    fn host_health_round_trips_and_is_absent_before_the_first_dial() {
        let dir = std::env::temp_dir().join(format!("hm-host-health-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(read_host_health(&dir, "greenroom-air").is_none());
        publish_host_health(
            &dir,
            "greenroom-air",
            &HostHealth {
                summary: "disconnected (ssh master to omnidev-greenroom-air failed: exit 255)"
                    .into(),
                at_iso: "2026-09-09T11:42:42.000Z".into(),
                next_retry_unix: Some(1_000.5),
            },
        );
        let back = read_host_health(&dir, "greenroom-air").unwrap();
        assert!(back.summary.starts_with("disconnected (ssh master"));
        assert_eq!(back.at_iso, "2026-09-09T11:42:42.000Z");
        assert_eq!(back.next_retry_unix, Some(1_000.5));
        // one host's record is never another's
        assert!(read_host_health(&dir, "greenroom-studio").is_none());

        publish_host_health(
            &dir,
            "greenroom-air",
            &HostHealth {
                summary: "connected and synced".into(),
                at_iso: "2026-09-09T11:43:12.000Z".into(),
                next_retry_unix: None,
            },
        );
        let back = read_host_health(&dir, "greenroom-air").unwrap();
        assert_eq!(back.summary, "connected and synced");
        assert_eq!(back.next_retry_unix, None);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_pane_hint_goes_to_one_pane_and_only_once() {
        let dir = std::env::temp_dir().join(format!("hm-hint-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        set_pane_hint(&dir, "wBT:p2", "closing the local tab");
        // not the neighbour's: an unaddressed notice is what let the
        // REPLACEMENT pane announce a move that had already finished
        assert_eq!(take_pane_hint(&dir, "wBT:p3"), None);
        assert_eq!(take_pane_hint(&dir, "wBT:p2").as_deref(), Some("closing the local tab"));
        // consumed, so a repaint doesn't resurrect it
        assert_eq!(take_pane_hint(&dir, "wBT:p2"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_stale_pane_hint_is_dropped_not_shown() {
        let dir = std::env::temp_dir().join(format!("hm-hint-old-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        set_pane_hint(&dir, "wBT:p2", "closing the local tab");
        let path = pane_hint_path(&dir, "wBT:p2");
        // backdate it: only the mtime distinguishes a notice from a leftover
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 120;
        let t = libc::timeval { tv_sec: secs, tv_usec: 0 };
        let c = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::utimes(c.as_ptr(), [t, t].as_ptr()) }, 0);
        assert_eq!(take_pane_hint(&dir, "wBT:p2"), None);
        assert!(!path.exists(), "stale or not, it is consumed");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
