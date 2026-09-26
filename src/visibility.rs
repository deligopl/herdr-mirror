// Idle release: a sidebar copy nobody is looking at stops streaming.
//
// The local Herdr server emulates every mirrored pane's output on one thread,
// so ~60 remote streams cost it continuously even though at most a tab's worth
// is ever on screen. A sidebar copy that has not been viewed for the grace
// period releases its remote terminal exactly the way it stands aside for a
// `herdr-mirror view` tile (see `view.rs`): the process stays, so the local
// agent identity and the status the daemon pushes stay; only the stream goes.
//
// Who knows what is viewed: the daemon. It holds ONE extra local event
// subscription (focus + layout events) and, on each change, one
// `workspace.list` and one `layout.export` of the focused tab — never
// `session.snapshot`, which resolves foreground processes for every pane. It
// publishes the result as a small file:
//
//   <state>/visible-panes.json   { pid, idleReleaseSecs, workspaceId, tabId, panes }
//
// Each sidebar copy stats that file on its existing 300 ms claim tick and
// decides for itself. Resume never needs the daemon: a focus-in report or any
// pty input resumes the pane directly, the input buffered until reattached.
//
// A sidebar copy that is idle-released says so for the daemon's stall sweep:
//
//   <state>/idle-released/<pane>.idle   pid of the streamer

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::ApiClient;
use crate::util::{pid_alive, sane_component, Logger};

/// Default grace before a pane nobody views releases its stream.
pub const DEFAULT_IDLE_RELEASE_SECS: u64 = 120;

/// Env override for the daemon (wins over hosts.toml); 0 disables.
pub const IDLE_RELEASE_ENV: &str = "HERDR_MIRROR_IDLE_RELEASE_SECS";

/// The daemon's effective grace: the env override when it parses, else the
/// config value.
pub fn effective_idle_release_secs(config_secs: u64, env: Option<&str>) -> u64 {
    env.and_then(|v| v.trim().parse().ok()).unwrap_or(config_secs)
}

/// Safety refresh of the published view, in case a focus event was missed.
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Focus events come in small bursts (workspace + tab + pane for one switch);
/// resolve once per burst.
const EVENT_SETTLE: Duration = Duration::from_millis(80);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Visible {
    /// the daemon that published this; a dead publisher means "no information"
    pub pid: i32,
    /// grace in seconds; 0 = idle release disabled
    pub idle_release_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_id: Option<String>,
    /// local panes on screen: every pane of the focused workspace's active
    /// tab, or only its focused pane when that tab is zoomed
    #[serde(default)]
    pub panes: Vec<String>,
}

impl Visible {
    pub fn shows(&self, pane: &str) -> bool {
        self.panes.iter().any(|p| p == pane)
    }
}

pub fn visible_path(state_dir: &Path) -> PathBuf {
    state_dir.join("visible-panes.json")
}

/// Write the file only when its content changes, atomically, so readers
/// polling its metadata see one clean switch.
pub fn publish(state_dir: &Path, visible: &Visible) -> bool {
    let path = visible_path(state_dir);
    let Ok(text) = serde_json::to_string(visible) else { return false };
    if fs::read_to_string(&path).ok().as_deref() == Some(text.as_str()) {
        return false;
    }
    let _ = fs::create_dir_all(state_dir);
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &text).and_then(|_| fs::rename(&tmp, &path)).is_ok()
}

/// Remove the file if this process published it.
pub fn withdraw(state_dir: &Path) {
    let path = visible_path(state_dir);
    let ours = fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str::<Visible>(&t).ok())
        .is_some_and(|v| v.pid == std::process::id() as i32);
    if ours {
        let _ = fs::remove_file(path);
    }
}

/// Sidebar-side reader: re-parses only when the file changes, and treats a
/// file whose publisher is gone as absent.
#[derive(Default)]
pub struct Reader {
    stamp: Option<(i64, i64, u64, u64)>,
    cached: Option<Visible>,
}

impl Reader {
    pub fn read(&mut self, state_dir: &Path) -> Option<&Visible> {
        let path = visible_path(state_dir);
        match fs::metadata(&path) {
            Ok(m) => {
                let stamp = (m.mtime(), m.mtime_nsec(), m.ino(), m.len());
                if self.stamp != Some(stamp) {
                    self.stamp = Some(stamp);
                    self.cached = fs::read_to_string(&path)
                        .ok()
                        .and_then(|t| serde_json::from_str(&t).ok());
                }
            }
            Err(_) => {
                self.stamp = None;
                self.cached = None;
            }
        }
        self.cached.as_ref().filter(|v| pid_alive(v.pid))
    }
}

// ---------------------------------------------------------------------------
// sidebar-side decision

#[derive(Debug, PartialEq, Eq)]
pub enum IdleStep {
    Stay,
    /// not viewed for the grace period: release the stream
    Release,
    /// viewed again (or idle release switched off): reattach
    Resume,
}

/// What an unclaimed sidebar copy does after one poll. `last_seen` is the
/// last moment the pane was viewed or received input/focus; the caller
/// refreshes it whenever `visible` shows the pane.
///
/// No information (no daemon, or a dead one) never releases anything, and
/// never mass-resumes either: a released pane then waits for focus or input.
pub fn idle_step(
    released: bool,
    visible: Option<&Visible>,
    pane: &str,
    last_seen: Instant,
    now: Instant,
) -> IdleStep {
    let Some(v) = visible else { return IdleStep::Stay };
    if released {
        return if v.idle_release_secs == 0 || v.shows(pane) {
            IdleStep::Resume
        } else {
            IdleStep::Stay
        };
    }
    if v.idle_release_secs == 0 || v.shows(pane) {
        return IdleStep::Stay;
    }
    if now.saturating_duration_since(last_seen) >= Duration::from_secs(v.idle_release_secs) {
        IdleStep::Release
    } else {
        IdleStep::Stay
    }
}

/// The notice an idle-released sidebar copy draws. Mouse grab off, focus
/// reporting on: the focus-in report is one of the resume triggers.
pub fn idle_notice() -> String {
    "\x1b[?1002l\x1b[?1006l\x1b[?1004h\x1b[?25l\x1b[2J\x1b[H\
     \x1b[2mpaused while not viewed \u{2014} focus or type to resume\x1b[0m"
        .to_string()
}

// ---------------------------------------------------------------------------
// idle marker, for the daemon's stall sweep

fn marker_path(state_dir: &Path, pane: &str) -> PathBuf {
    state_dir.join("idle-released").join(format!("{}.idle", sane_component(pane)))
}

pub fn mark_idle_released(state_dir: &Path, pane: &str) {
    let path = marker_path(state_dir, pane);
    if let Some(dir) = path.parent() {
        let _ = fs::create_dir_all(dir);
    }
    let _ = fs::write(path, std::process::id().to_string());
}

pub fn clear_idle_released(state_dir: &Path, pane: &str) {
    let _ = fs::remove_file(marker_path(state_dir, pane));
}

/// Whether a live streamer says this pane is idle-released.
pub fn is_idle_released(state_dir: &Path, pane: &str) -> bool {
    fs::read_to_string(marker_path(state_dir, pane))
        .ok()
        .and_then(|t| t.trim().parse::<i32>().ok())
        .is_some_and(pid_alive)
}

// ---------------------------------------------------------------------------
// daemon side

/// The focused workspace and its active tab, from a `workspace.list` result.
pub fn focused_tab(workspace_list: &Value) -> Option<(String, String)> {
    let list = workspace_list.get("workspaces")?.as_array()?;
    let ws = list.iter().find(|w| w.get("focused").and_then(Value::as_bool) == Some(true))?;
    let s = |k: &str| ws.get(k).and_then(Value::as_str).map(str::to_string);
    Some((s("workspace_id")?, s("active_tab_id")?))
}

fn collect_pane_ids(node: &Value, out: &mut Vec<String>) {
    if let Some(id) = node.get("pane_id").and_then(Value::as_str) {
        out.push(id.to_string());
    }
    for k in ["first", "second"] {
        if let Some(child) = node.get(k) {
            collect_pane_ids(child, out);
        }
    }
}

fn shown_panes(zoomed: bool, focused: Option<&str>, all: Vec<String>) -> Vec<String> {
    match (zoomed, focused) {
        (true, Some(f)) if all.iter().any(|p| p == f) => vec![f.to_string()],
        _ => all,
    }
}

/// Panes on screen from a `layout.export` result (`{layout: {root, zoomed,
/// focused_pane_id, ...}}`).
pub fn panes_from_export(export: &Value) -> Vec<String> {
    let layout = export.get("layout").unwrap_or(export);
    let mut all = Vec::new();
    if let Some(root) = layout.get("root") {
        collect_pane_ids(root, &mut all);
    }
    let zoomed = layout.get("zoomed").and_then(Value::as_bool).unwrap_or(false);
    shown_panes(zoomed, layout.get("focused_pane_id").and_then(Value::as_str), all)
}

/// Panes on screen from a `layout_updated` event's `layout` (a pane layout
/// snapshot), when it is about `tab`. `None` when it concerns another tab.
pub fn panes_from_layout_event(data: &Value, tab: &str) -> Option<Vec<String>> {
    let layout = data.get("layout")?;
    if layout.get("tab_id").and_then(Value::as_str) != Some(tab) {
        return None;
    }
    let all = layout
        .get("panes")?
        .as_array()?
        .iter()
        .filter_map(|p| p.get("pane_id").and_then(Value::as_str).map(str::to_string))
        .collect();
    let zoomed = layout.get("zoomed").and_then(Value::as_bool).unwrap_or(false);
    Some(shown_panes(zoomed, layout.get("focused_pane_id").and_then(Value::as_str), all))
}

/// What a focus event says came on screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Focus {
    /// don't know: ask which workspace is focused
    Unknown,
    Workspace(String),
    Tab(String),
    Pane(String),
}

/// Read a focus event (`None` for any other event).
pub fn focus_of(event: &str, data: &Value) -> Option<Focus> {
    let field = |k: &str| data.get(k).and_then(Value::as_str).map(str::to_string);
    match event {
        "tab_focused" => Some(field("tab_id").map(Focus::Tab).unwrap_or(Focus::Unknown)),
        "pane_focused" => Some(field("pane_id").map(Focus::Pane).unwrap_or(Focus::Unknown)),
        "workspace_focused" => {
            Some(field("workspace_id").map(Focus::Workspace).unwrap_or(Focus::Unknown))
        }
        _ => None,
    }
}

/// At most two cheap local reads (never `session.snapshot`): which tab is on
/// screen, when the event did not already say, and which panes it holds.
async fn resolve(local: &ApiClient, focus: &Focus) -> Option<(String, String, Vec<String>)> {
    let params = match focus {
        Focus::Tab(tab) => json!({ "tab_id": tab }),
        Focus::Pane(pane) => json!({ "pane_id": pane }),
        Focus::Workspace(ws) => {
            let info = local.request("workspace.get", json!({ "workspace_id": ws })).await.ok()?;
            let info = info.get("workspace").unwrap_or(&info);
            json!({ "tab_id": info.get("active_tab_id")?.as_str()? })
        }
        Focus::Unknown => {
            let list = local.request("workspace.list", json!({})).await.ok()?;
            json!({ "tab_id": focused_tab(&list)?.1 })
        }
    };
    let export = local.request("layout.export", params).await.ok()?;
    let layout = export.get("layout").unwrap_or(&export);
    let s = |k: &str| layout.get(k).and_then(Value::as_str).map(str::to_string);
    Some((s("workspace_id")?, s("tab_id")?, panes_from_export(&export)))
}

/// Keep `visible-panes.json` current. With idle release disabled it publishes
/// that once and holds no subscription at all.
pub async fn visibility_task(local: ApiClient, state_dir: PathBuf, idle_secs: u64, log: Logger) {
    let pid = std::process::id() as i32;
    let mut current =
        Visible { pid, idle_release_secs: idle_secs, ..Default::default() };
    publish(&state_dir, &current);
    if idle_secs == 0 {
        log.log("idle release disabled (idle_release_secs = 0)");
        return;
    }
    log.log(&format!("idle release: mirror panes not viewed for {idle_secs}s stop streaming"));
    let subs = vec![
        json!({ "type": "workspace.focused" }),
        json!({ "type": "tab.focused" }),
        json!({ "type": "pane.focused" }),
        json!({ "type": "layout.updated" }),
    ];
    loop {
        match local.subscribe(subs.clone()).await {
            Ok(mut stream) => {
                // first tick is immediate: the initial read after (re)subscribe
                let mut refresh = tokio::time::interval(REFRESH_INTERVAL);
                refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                'events: loop {
                    let mut focus = None;
                    tokio::select! {
                        _ = refresh.tick() => focus = Some(Focus::Unknown),
                        e = stream.next() => {
                            let Some(e) = e else { break 'events };
                            if let Some(f) = focus_of(&e.event, &e.data) {
                                // one switch is a small burst (workspace, tab,
                                // pane): the last word in it wins
                                focus = Some(f);
                                let settle = tokio::time::sleep(EVENT_SETTLE);
                                tokio::pin!(settle);
                                loop {
                                    tokio::select! {
                                        _ = &mut settle => break,
                                        more = stream.next() => match more {
                                            None => break 'events,
                                            Some(m) => if let Some(f) = focus_of(&m.event, &m.data) {
                                                focus = Some(f);
                                            },
                                        },
                                    }
                                }
                            } else if e.event == "layout_updated" {
                                // the on-screen tab changed shape: take its panes
                                // from the event itself, no request needed
                                if let Some(panes) = current
                                    .tab_id
                                    .as_deref()
                                    .and_then(|t| panes_from_layout_event(&e.data, t))
                                {
                                    current.panes = panes;
                                    publish(&state_dir, &current);
                                }
                            }
                        }
                    }
                    if let Some(focus) = focus {
                        if let Some((ws, tab, panes)) = resolve(&local, &focus).await {
                            current.workspace_id = Some(ws);
                            current.tab_id = Some(tab);
                            current.panes = panes;
                            publish(&state_dir, &current);
                        }
                    }
                }
                log.log("visibility event stream dropped — resubscribing");
            }
            Err(e) => log.log(&format!("visibility subscribe failed ({e}) — retrying")),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "hmvis-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn vis(secs: u64, panes: &[&str]) -> Visible {
        Visible {
            pid: std::process::id() as i32,
            idle_release_secs: secs,
            workspace_id: Some("w3".into()),
            tab_id: Some("w3:t1".into()),
            panes: panes.iter().map(|p| p.to_string()).collect(),
        }
    }

    #[test]
    fn the_focused_workspace_active_tab_and_its_panes_are_what_is_visible() {
        let list = json!({ "workspaces": [
            { "workspace_id": "w1", "focused": false, "active_tab_id": "w1:t2" },
            { "workspace_id": "w3", "focused": true, "active_tab_id": "w3:t4" },
        ]});
        assert_eq!(focused_tab(&list), Some(("w3".into(), "w3:t4".into())));
        assert_eq!(focused_tab(&json!({ "workspaces": [] })), None);

        let export = json!({ "layout": {
            "tab_id": "w3:t4", "zoomed": false, "focused_pane_id": "w3:p2",
            "root": { "type": "split", "direction": "right", "ratio": 0.5,
                "first": { "type": "pane", "pane_id": "w3:p1" },
                "second": { "type": "split", "direction": "down", "ratio": 0.5,
                    "first": { "type": "pane", "pane_id": "w3:p2" },
                    "second": { "type": "pane", "pane_id": "w3:p3" } } } } });
        assert_eq!(panes_from_export(&export), ["w3:p1", "w3:p2", "w3:p3"]);
        // zoomed: only the focused pane is on screen
        let mut zoomed = export.clone();
        zoomed["layout"]["zoomed"] = json!(true);
        assert_eq!(panes_from_export(&zoomed), ["w3:p2"]);

        let event = json!({ "layout": { "tab_id": "w3:t4", "zoomed": false,
            "focused_pane_id": "w3:p1",
            "panes": [ { "pane_id": "w3:p1" }, { "pane_id": "w3:p9" } ] } });
        assert_eq!(panes_from_layout_event(&event, "w3:t4"), Some(vec!["w3:p1".into(), "w3:p9".into()]));
        assert_eq!(panes_from_layout_event(&event, "w1:t1"), None);
        assert_eq!(
            focus_of("tab_focused", &json!({ "tab_id": "w3:t4", "workspace_id": "w3" })),
            Some(Focus::Tab("w3:t4".into()))
        );
        assert_eq!(
            focus_of("pane_focused", &json!({ "pane_id": "w3:p2", "workspace_id": "w3" })),
            Some(Focus::Pane("w3:p2".into()))
        );
        assert_eq!(
            focus_of("workspace_focused", &json!({ "workspace_id": "w3" })),
            Some(Focus::Workspace("w3".into()))
        );
        assert_eq!(focus_of("tab_focused", &json!({})), Some(Focus::Unknown));
        assert_eq!(focus_of("layout_updated", &event), None);
    }

    #[test]
    fn a_pane_releases_only_after_the_grace_and_only_when_not_on_screen() {
        let t0 = Instant::now();
        let v = vis(120, &["w3:p1"]);
        // on screen: never released, whatever the clock says
        assert_eq!(idle_step(false, Some(&v), "w3:p1", t0, t0 + Duration::from_secs(999)), IdleStep::Stay);
        // off screen, inside the grace
        assert_eq!(idle_step(false, Some(&v), "w9:p1", t0, t0 + Duration::from_secs(119)), IdleStep::Stay);
        // off screen past the grace
        assert_eq!(idle_step(false, Some(&v), "w9:p1", t0, t0 + Duration::from_secs(120)), IdleStep::Release);
        // released and still off screen: stays released
        assert_eq!(idle_step(true, Some(&v), "w9:p1", t0, t0 + Duration::from_secs(500)), IdleStep::Stay);
        // released and now on screen: resumes
        assert_eq!(idle_step(true, Some(&v), "w3:p1", t0, t0), IdleStep::Resume);
    }

    #[test]
    fn the_env_override_wins_when_it_parses() {
        assert_eq!(effective_idle_release_secs(120, None), 120);
        assert_eq!(effective_idle_release_secs(120, Some("0")), 0);
        assert_eq!(effective_idle_release_secs(120, Some(" 45 ")), 45);
        assert_eq!(effective_idle_release_secs(120, Some("soon")), 120);
    }

    #[test]
    fn disabled_or_unknown_never_releases_and_disabling_resumes() {
        let t0 = Instant::now();
        let late = t0 + Duration::from_secs(10_000);
        let off = vis(0, &[]);
        assert_eq!(idle_step(false, Some(&off), "w9:p1", t0, late), IdleStep::Stay);
        assert_eq!(idle_step(true, Some(&off), "w9:p1", t0, late), IdleStep::Resume);
        // no daemon: no release, and no mass resume either
        assert_eq!(idle_step(false, None, "w9:p1", t0, late), IdleStep::Stay);
        assert_eq!(idle_step(true, None, "w9:p1", t0, late), IdleStep::Stay);
    }

    #[test]
    fn the_reader_follows_the_file_and_ignores_a_dead_publisher() {
        let d = dir("reader");
        let mut r = Reader::default();
        assert!(r.read(&d).is_none());
        assert!(publish(&d, &vis(120, &["w3:p1"])));
        // unchanged content is not rewritten
        assert!(!publish(&d, &vis(120, &["w3:p1"])));
        assert!(r.read(&d).unwrap().shows("w3:p1"));
        assert!(publish(&d, &vis(120, &["w3:p2"])));
        let v = r.read(&d).unwrap();
        assert!(v.shows("w3:p2") && !v.shows("w3:p1"));

        let mut dead = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = dead.id() as i32;
        dead.wait().unwrap();
        publish(&d, &Visible { pid: dead_pid, ..vis(120, &["w3:p2"]) });
        assert!(r.read(&d).is_none(), "a dead daemon's view is no information");
        // withdraw only removes our own file
        withdraw(&d);
        assert!(visible_path(&d).exists());
        publish(&d, &vis(120, &[]));
        withdraw(&d);
        assert!(!visible_path(&d).exists());
        assert!(r.read(&d).is_none());
        let _ = fs::remove_dir_all(d);
    }

    #[test]
    fn the_idle_marker_is_live_only_while_its_streamer_is() {
        let d = dir("marker");
        assert!(!is_idle_released(&d, "w1:p2"));
        mark_idle_released(&d, "w1:p2");
        assert!(is_idle_released(&d, "w1:p2"));
        assert!(!is_idle_released(&d, "w1:p3"));
        clear_idle_released(&d, "w1:p2");
        assert!(!is_idle_released(&d, "w1:p2"));
        fs::create_dir_all(d.join("idle-released")).unwrap();
        fs::write(marker_path(&d, "w1:p2"), "999999999").unwrap();
        assert!(!is_idle_released(&d, "w1:p2"), "a dead streamer's marker is ignored");
        let _ = fs::remove_dir_all(d);
    }

    /// The daemon side against a fake local server: the first read, a tab
    /// switch, and a layout change on the visible tab — with at most two
    /// cheap reads per change and never a `session.snapshot`.
    #[tokio::test]
    async fn the_daemon_publishes_what_is_on_screen_from_focus_and_layout_events() {
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let d = dir("task");
        let sock = d.join("h.sock");
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        let seen: Arc<Mutex<Vec<String>>> = Arc::default();
        let (events, _) = tokio::sync::broadcast::channel::<Value>(8);
        let (seen2, bus) = (seen.clone(), events.clone());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (seen, mut rx) = (seen2.clone(), bus.subscribe());
                tokio::spawn(async move {
                    let (read, mut write) = stream.into_split();
                    let mut lines = BufReader::new(read).lines();
                    let Ok(Some(line)) = lines.next_line().await else { return };
                    let req: Value = serde_json::from_str(&line).unwrap();
                    let method = req["method"].as_str().unwrap().to_string();
                    seen.lock().unwrap().push(method.clone());
                    let pane = |id: &str| json!({ "type": "pane", "pane_id": id });
                    let result = match method.as_str() {
                        "events.subscribe" => json!({ "type": "subscription_started" }),
                        "workspace.list" => json!({ "workspaces": [
                            { "workspace_id": "w1", "focused": true, "active_tab_id": "w1:t1" } ] }),
                        "layout.export" if req["params"]["tab_id"] == "w2:t3" => json!({ "layout": {
                            "workspace_id": "w2", "tab_id": "w2:t3", "zoomed": false,
                            "focused_pane_id": "w2:p5", "root": pane("w2:p5") } }),
                        "layout.export" => json!({ "layout": {
                            "workspace_id": "w1", "tab_id": "w1:t1", "zoomed": false,
                            "focused_pane_id": "w1:p1", "root": pane("w1:p1") } }),
                        _ => json!({}),
                    };
                    let reply = json!({ "id": req["id"], "result": result });
                    write.write_all(format!("{reply}\n").as_bytes()).await.unwrap();
                    if method == "events.subscribe" {
                        while let Ok(e) = rx.recv().await {
                            if write.write_all(format!("{e}\n").as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        });
        let task = tokio::spawn(visibility_task(
            ApiClient::at(&sock),
            d.clone(),
            120,
            Logger::new(&d, false),
        ));
        let mut reader = Reader::default();
        async fn until(reader: &mut Reader, d: &Path, want: &[&str]) {
            for _ in 0..200 {
                if reader.read(d).is_some_and(|v| v.panes == want) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("never published {want:?}");
        }
        until(&mut reader, &d, &["w1:p1"]).await;
        assert_eq!(reader.read(&d).unwrap().idle_release_secs, 120);

        let before = seen.lock().unwrap().len();
        let _ = events.send(json!({ "event": "workspace_focused", "data": { "workspace_id": "w2" } }));
        let _ = events.send(json!({ "event": "tab_focused",
            "data": { "workspace_id": "w2", "tab_id": "w2:t3" } }));
        until(&mut reader, &d, &["w2:p5"]).await;
        assert_eq!(reader.read(&d).unwrap().tab_id.as_deref(), Some("w2:t3"));
        // one burst, one read: the tab event already named the tab
        assert_eq!(seen.lock().unwrap()[before..], ["layout.export".to_string()]);

        // a split on the visible tab: panes straight from the event
        let before = seen.lock().unwrap().len();
        let _ = events.send(json!({ "event": "layout_updated", "data": { "layout": {
            "workspace_id": "w2", "tab_id": "w2:t3", "zoomed": false, "focused_pane_id": "w2:p5",
            "panes": [ { "pane_id": "w2:p5" }, { "pane_id": "w2:p6" } ] } } }));
        until(&mut reader, &d, &["w2:p5", "w2:p6"]).await;
        assert_eq!(seen.lock().unwrap().len(), before);
        assert!(!seen.lock().unwrap().iter().any(|m| m == "session.snapshot"));
        task.abort();
        let _ = fs::remove_dir_all(d);
    }
}
