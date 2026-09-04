// Reconciliation: project a remote herdr server's workspaces/tabs/panes into
// the local server as `prefix:*` mirror objects, and push the remote's
// authoritative agent statuses onto the mirror panes.
//
// The id map (src/state.rs, persisted per host) distinguishes "user closed the
// mirror locally" (tombstone — don't recreate) from "remote object went away"
// (close the mirror).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::api::ApiClient;
use crate::config::HostConfig;
use crate::state::{load_state, save_state, HostState, PaneEntry, RosemaryRun, WsEntry};
use crate::util::{Logger, Result};

// --- snapshot shapes (subset of the API's SessionSnapshot) ---

#[derive(Debug, Clone, Deserialize)]
pub struct WsInfo {
    pub workspace_id: String,
    #[serde(default)]
    pub label: String,
    pub tab_count: Option<u64>,
    pub pane_count: Option<u64>,
    pub active_tab_id: Option<String>,
    /// custom metadata tokens the remote publishes. `default` on purpose: a
    /// pre-0.7.4 remote never sends this.
    #[serde(default)]
    pub tokens: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TabInfo {
    pub tab_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub label: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaneInfo {
    pub pane_id: String,
    pub tab_id: String,
    pub workspace_id: String,
    /// unread today, but part of the pane wire shape — kept so the struct
    /// documents what the API actually returns
    #[allow(dead_code)]
    pub label: Option<String>,
    pub cwd: Option<String>,
    pub foreground_cwd: Option<String>,
}

/// Agent fields as they appear both in snapshot `agents[]` and in
/// `pane.agent_status_changed` event data (null fields omitted there).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AgentInfo {
    #[serde(default)]
    pub pane_id: String,
    pub agent: Option<String>,
    pub display_agent: Option<String>,
    pub name: Option<String>,
    /// the pane's reported title (`agents[].title`, and what the
    /// pane_agent_status_changed event carries). Its own field, not an alias on
    /// `name`: the remote sets them independently, and an alias funnels both
    /// keys into one slot, a duplicate-field error that fails the whole
    /// snapshot parse, not just this row.
    #[serde(default)]
    pub title: Option<String>,
    /// the remote's live terminal title (e.g. a coding agent's current task
    /// summary), stripped of spinner/status glyphs. Only present on hosts new
    /// enough to publish it — default so older remotes still parse.
    #[serde(default)]
    pub terminal_title_stripped: Option<String>,
    #[serde(default)]
    pub terminal_title: Option<String>,
    #[serde(default)]
    pub agent_status: Option<String>,
    pub custom_status: Option<String>,
    pub state_labels: Option<BTreeMap<String, String>>,
    /// custom metadata tokens the remote publishes ($model, $summary, …).
    /// `default` on purpose: a pre-0.7.4 remote never sends this.
    #[serde(default)]
    pub tokens: HashMap<String, String>,
    /// True only when the remote Herdr has verified that the harness can
    /// accept a typed prompt. Missing on older remotes means not ready.
    #[serde(default)]
    pub interactive_ready: bool,
    /// Harness-session evidence observed by the remote Herdr. Mirror forwards
    /// the public id only when the remote also says the pane is ready.
    #[serde(default)]
    pub agent_session: Option<AgentSessionInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentSessionInfo {
    pub value: String,
}

impl AgentInfo {
    /// Does this describe a live agent (vs. a sparse release event)?
    pub fn has_agent(&self) -> bool {
        self.agent.as_deref().is_some_and(|a| !a.is_empty())
            || self.agent_status.as_deref().is_some_and(|s| s != "unknown")
    }

    /// The single `title` slot `pane.report_metadata` accepts. A remote agent
    /// with a user-given `name` keeps showing it (unchanged behavior — don't
    /// bury a name the user picked under an ever-changing task title). Only
    /// when there's no name do we fall back to the pane's reported title, then
    /// to the remote's live terminal title, so a mirrored agent's current task
    /// is visible instead of blank.
    pub fn effective_title(&self) -> Option<&str> {
        self.name
            .as_deref()
            .or(self.title.as_deref())
            .or(self.terminal_title_stripped.as_deref())
            .or(self.terminal_title.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LayoutRect {
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LayoutPaneSnapshot {
    pub pane_id: String,
    pub rect: LayoutRect,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LayoutSnapshot {
    #[allow(dead_code)]
    pub tab_id: String,
    #[serde(default)]
    pub panes: Vec<LayoutPaneSnapshot>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Snapshot {
    #[serde(default)]
    pub workspaces: Vec<WsInfo>,
    #[serde(default)]
    pub tabs: Vec<TabInfo>,
    #[serde(default)]
    pub panes: Vec<PaneInfo>,
    #[serde(default)]
    pub agents: Vec<AgentInfo>,
    #[serde(default)]
    pub layouts: Vec<LayoutSnapshot>,
}

pub async fn fetch_snapshot(api: &ApiClient) -> Result<Snapshot> {
    #[derive(Deserialize)]
    struct Res {
        snapshot: Snapshot,
    }
    let res: Res = api.request_t("session.snapshot", json!({})).await?;
    Ok(res.snapshot)
}

// --- layout tree ---

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LayoutNode {
    Pane {
        pane_id: Option<String>,
        label: Option<String>,
    },
    Split {
        direction: String,
        ratio: f64,
        first: Box<LayoutNode>,
        second: Box<LayoutNode>,
    },
}

/// Locate `pane_id` in a split tree: the parent split's direction (already
/// "right"/"down", pane.split's vocabulary) plus the sibling subtree's pane ids
/// ordered nearest-to-the-split-point first. Fallback for a pane
/// `layout_sync::plan_placements` can't place faithfully; the shape-preserving
/// path lives there.
pub fn locate_in_layout(node: &LayoutNode, pane_id: &str) -> Option<(String, Vec<String>)> {
    let LayoutNode::Split { direction, first, second, .. } = node else { return None };
    let is_the_pane =
        |n: &LayoutNode| matches!(n, LayoutNode::Pane { pane_id: Some(p), .. } if p == pane_id);
    if is_the_pane(first) {
        let mut sibs = Vec::new();
        walk_pane_ids(second, &mut sibs);
        return Some((direction.clone(), sibs));
    }
    if is_the_pane(second) {
        let mut sibs = Vec::new();
        walk_pane_ids(first, &mut sibs);
        sibs.reverse();
        return Some((direction.clone(), sibs));
    }
    locate_in_layout(first, pane_id).or_else(|| locate_in_layout(second, pane_id))
}

fn walk_pane_ids(node: &LayoutNode, out: &mut Vec<String>) {
    match node {
        LayoutNode::Pane { pane_id, .. } => out.push(pane_id.clone().unwrap_or_default()),
        LayoutNode::Split { first, second, .. } => {
            walk_pane_ids(first, out);
            walk_pane_ids(second, out);
        }
    }
}

/// Layout tree as plain shell panes (no `command`, so herdr won't set
/// `launch_argv` and treat them as agents); the streamer is exec'd in afterward.
fn map_node(node: &LayoutNode, cwd: &str) -> Value {
    match node {
        LayoutNode::Pane { pane_id: _, label } => json!({
            "type": "pane",
            "label": label,
            "cwd": cwd,
        }),
        LayoutNode::Split { direction, ratio, first, second } => json!({
            "type": "split",
            "direction": direction,
            "ratio": ratio,
            "first": map_node(first, cwd),
            "second": map_node(second, cwd),
        }),
    }
}

/// Drop panes whose mirror the user closed (tombstoned) from an exported
/// layout tree, collapsing a split left with one child. None means every pane
/// in the tree is tombstoned — the whole tab's mirror was closed.
fn prune_closed(node: &LayoutNode, panes: &BTreeMap<String, PaneEntry>) -> Option<LayoutNode> {
    match node {
        LayoutNode::Pane { pane_id, .. } => {
            let closed = pane_id
                .as_deref()
                .and_then(|rid| panes.get(rid))
                .is_some_and(|e| e.is_tombstoned());
            (!closed).then(|| node.clone())
        }
        LayoutNode::Split { direction, ratio, first, second } => {
            match (prune_closed(first, panes), prune_closed(second, panes)) {
                (Some(f), Some(s)) => Some(LayoutNode::Split {
                    direction: direction.clone(),
                    ratio: *ratio,
                    first: Box::new(f),
                    second: Box::new(s),
                }),
                (one, two) => one.or(two),
            }
        }
    }
}

/// Which mirrors `apply_hidden` would close, and what survives the map.
///
/// Split out so the gating is testable without a live herdr: the first version
/// of this closed every mirror on EVERY call because it never read the marker,
/// which turned the daemon into a destroy/rebuild loop. Full-green tests said
/// nothing, because nothing reached the function.
pub(crate) fn hidden_close_plan(
    hidden: bool,
    state: &mut HostState,
    live: &std::collections::HashSet<String>,
) -> Vec<String> {
    if !hidden {
        return Vec::new();
    }
    let doomed: Vec<String> = state
        .workspaces
        .values()
        .filter(|e| !e.is_tombstoned() && live.contains(&e.local_id))
        .map(|e| e.local_id.clone())
        .collect();
    if doomed.is_empty() {
        return doomed;
    }
    // tombstones on both maps survive: they mean "the user closed this, do not
    // recreate until `restore`", which hiding must not quietly forget
    state.workspaces.retain(|_, e| e.is_tombstoned());
    state.panes.retain(|_, e| e.is_tombstoned());
    state.tabs.clear();
    doomed
}

/// Take a hidden host's mirrors off the sidebar.
///
/// Lives in the daemon and needs only the LOCAL api, so it works while the
/// remote is unreachable — which is the main reason to hide a connection in the
/// first place (a dead host leaving reconnecting panes on screen). `converge`
/// cannot do this job: it only runs while connected, and it is also called by
/// one-shots whose close tracker nobody reads.
///
/// Tombstoned entries are kept. A tombstone means "the user closed this, do not
/// recreate it until `restore`", and hiding a host must not quietly forget that
/// — otherwise `show` resurrects every mirror they had deliberately closed.
///
/// Two layers against close-through, the same pair `teardown` uses: each id is
/// marked as ours before its close, and the map entries are dropped first so a
/// missed mark has nothing to attribute a close to.
pub async fn apply_hidden(
    local: &ApiClient,
    state_dir: &std::path::Path,
    host_name: &str,
    log: &Logger,
    closes: &crate::closes::Closes,
) {
    // The guard, not the caller's job: this is called on every host_task loop
    // and before every connected converge, so without it the daemon closes the
    // mirrors it just created, forever.
    let hidden = crate::state::is_hidden(state_dir, host_name);
    if !hidden {
        return;
    }
    let mut state = load_state(state_dir, host_name);
    // only ids herdr still shows. Note this filters ids that are GONE, not ids
    // that now belong to someone else: a local server restart can reassign one,
    // and this cannot tell. Converge's own close paths share that weakness.
    let live: std::collections::HashSet<String> = match fetch_snapshot(local).await {
        Ok(snap) => snap.workspaces.iter().map(|w| w.workspace_id.clone()).collect(),
        Err(e) => {
            // the one path where hide legitimately does nothing; say so, or the
            // mirrors stay up with no explanation anywhere
            log.log(&format!("hidden: local snapshot failed for {host_name}: {e}"));
            return;
        }
    };
    let doomed = hidden_close_plan(hidden, &mut state, &live);
    if doomed.is_empty() {
        return;
    }
    if let Err(e) = save_state(state_dir, host_name, &state) {
        log.log(&format!("hidden: could not save state for {host_name}: {e}"));
        return;
    }
    for local_id in &doomed {
        log.log(&format!("hidden — closing mirror workspace {local_id}"));
        if let Ok(mut t) = closes.lock() {
            t.mark_self_close(local_id);
        }
        if let Err(e) = local.request("workspace.close", json!({ "workspace_id": local_id })).await
        {
            log.log(&format!("hidden: close failed for {local_id}: {e}"));
        }
    }
}

/// Mark a local id the plugin itself is about to close, so the close event it
/// raises isn't read back as the user closing the mirror (see closes.rs).
fn mark_self_close(deps: &ConvergeDeps, local_id: &str) {
    if let Ok(mut t) = deps.closes.lock() {
        t.mark_self_close(local_id);
    }
}

fn map_status(remote: &str) -> &'static str {
    match remote {
        "working" => "working",
        "blocked" => "blocked",
        "idle" => "idle",
        // local herdr derives "done" from working→idle while unseen
        "done" => "idle",
        _ => "unknown",
    }
}

pub fn mirror_source(host_name: &str) -> String {
    format!("plugin:mirror:{host_name}")
}

/// The server rejects custom_status longer than this.
const CUSTOM_STATUS_MAX: usize = 32;

fn clamp_status(s: &str) -> String {
    s.chars().take(CUSTOM_STATUS_MAX).collect()
}

// Observe requests = the remote pane's real size + a margin that absorbs
// modest remote resizes (a larger resize clips until the wrapper reconnects).
const OBSERVE_MARGIN_COLS: u32 = 16;
const OBSERVE_MARGIN_ROWS: u32 = 8;

/// How to resolve a mirror label state (workspace or tab).
#[derive(Debug, PartialEq)]
enum LabelAction {
    /// labels agree — nothing to do
    InSync,
    /// user renamed the mirror locally → rename the REMOTE object to this
    PushRemote(String),
    /// remote is the authority (remote renamed, or unknown history) → restamp local
    RestampLocal,
}

/// Two-way rename resolution. `last_remote` is the remote label as of the
/// previous converge (None = pre-upgrade state file / first sight: remote wins).
///
/// `prefix` is `Some` for workspaces, whose mirrors carry the "<prefix>: " form,
/// and `None` for tabs, which carry the remote's label verbatim.
fn resolve_label(
    prefix: Option<&str>,
    remote_label: &str,
    local_label: &str,
    last_remote: Option<&str>,
) -> LabelAction {
    let expected = match prefix {
        Some(p) => format!("{p}: {remote_label}"),
        None => remote_label.to_string(),
    };
    if local_label == expected {
        return LabelAction::InSync;
    }
    if last_remote != Some(remote_label) {
        // remote changed since we last stamped (or no history) — remote wins
        return LabelAction::RestampLocal;
    }
    // remote unchanged, local differs → this is a user rename. Accept it with
    // or without the "<prefix>: " convention; empty/degenerate names restamp.
    let stripped = match prefix {
        Some(p) => local_label.strip_prefix(&format!("{p}: ")).unwrap_or(local_label).trim(),
        None => local_label.trim(),
    };
    if stripped.is_empty() || stripped == remote_label {
        LabelAction::RestampLocal
    } else {
        LabelAction::PushRemote(stripped.to_string())
    }
}

pub struct ConvergeDeps {
    pub local: ApiClient,
    pub remote: ApiClient,
    pub host: HostConfig,
    pub state_dir: PathBuf,
    pub log: Logger,
    /// mirror closing a workspace/pane locally onto the remote (see MirrorConfig)
    pub close_remote_on_local_close: bool,
    /// event-confirmed local closes. Absence from the local snapshot is
    /// ambiguous (rebuild in flight, failed converge, server restart), so only a
    /// close event that wasn't our own may close the remote.
    pub closes: crate::closes::Closes,
    /// One planner for the whole local Herdr session. It sees every configured
    /// source before allowing any mirrored dispatch identity.
    pub names: SessionNamePlanner,
    /// Local `pane.updated` events enter this lightweight gate before they are
    /// queued to a host task. A converge already in progress checks the marker
    /// before its next Rosemary write. The marker is only advisory: suppression
    /// is created solely from the authoritative snapshot read by the host task.
    pub rosemary_gate: RosemaryProjectionGate,
}

#[derive(Clone, Default)]
pub struct RosemaryProjectionGate {
    pending_local_panes: Arc<Mutex<HashSet<String>>>,
}

impl RosemaryProjectionGate {
    pub fn note_local_update(&self, pane_id: &str) {
        if let Ok(mut pending) = self.pending_local_panes.lock() {
            pending.insert(pane_id.to_string());
        }
    }

    pub fn finish_local_update(&self, pane_id: &str) {
        if let Ok(mut pending) = self.pending_local_panes.lock() {
            pending.remove(pane_id);
        }
    }

    pub(crate) fn has_local_update(&self, pane_id: &str) -> bool {
        self.pending_local_panes
            .lock()
            .is_ok_and(|pending| pending.contains(pane_id))
    }
}

pub struct PaneStatusDeps<'a> {
    pub local: &'a ApiClient,
    pub state_dir: &'a std::path::Path,
    pub host_name: &'a str,
    pub log: &'a Logger,
    pub rosemary_gate: &'a RosemaryProjectionGate,
}

#[derive(Clone)]
pub struct SessionNamePlanner {
    inner: Arc<Mutex<SessionNamePlan>>,
    changed: tokio::sync::watch::Sender<u64>,
}

#[derive(Default)]
struct SessionNamePlan {
    expected_hosts: BTreeSet<String>,
    observed: BTreeMap<String, Vec<(String, Option<String>)>>,
    planned: BTreeMap<(String, String), Option<String>>,
    generation: u64,
}

impl SessionNamePlanner {
    pub fn new(hosts: impl IntoIterator<Item = String>) -> Self {
        let expected_hosts = hosts.into_iter().collect();
        let (changed, _) = tokio::sync::watch::channel(0);
        Self {
            inner: Arc::new(Mutex::new(SessionNamePlan {
                expected_hosts,
                ..SessionNamePlan::default()
            })),
            changed,
        }
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn update(
        &self,
        host_name: &str,
        agents: &[AgentInfo],
        local: &Snapshot,
        state_dir: &std::path::Path,
        log: &Logger,
    ) -> HashMap<String, Option<String>> {
        let mut plan = self.inner.lock().expect("session name planner poisoned");
        plan.observed.insert(
            host_name.to_string(),
            agents
                .iter()
                .map(|agent| {
                    (
                        agent.pane_id.clone(),
                        agent.name.clone().filter(|name| !name.trim().is_empty()),
                    )
                })
                .collect(),
        );

        let ready = plan.expected_hosts.iter().all(|host| plan.observed.contains_key(host));
        let mut next = BTreeMap::new();
        if ready {
            let mut records = Vec::new();
            for (host, agents) in &plan.observed {
                for (pane, remote_name) in agents {
                    records.push((host.clone(), pane.clone(), remote_name.clone()));
                }
            }
            let mut unnamed_per_host: HashMap<&str, usize> = HashMap::new();
            for (host, _, remote) in &records {
                if remote.is_none() {
                    *unnamed_per_host.entry(host.as_str()).or_default() += 1;
                }
            }
            let mut candidates: Vec<Option<String>> = records
                .iter()
                .map(|(host, pane, remote)| match remote {
                    Some(remote) => mirrored_agent_name(host, Some(remote)),
                    None => mirrored_unnamed_agent_name(
                        host,
                        pane,
                        unnamed_per_host.get(host.as_str()).copied().unwrap_or_default() > 1,
                    ),
                })
                .collect();

            // A short un-hashed spelling can be ambiguous across source-pair
            // boundaries. Only those duplicate spellings are regenerated with
            // the exact source-pair digest; ordinary legal names stay stable.
            let mut owners: HashMap<String, Vec<usize>> = HashMap::new();
            for (index, candidate) in candidates.iter().enumerate() {
                if let Some(candidate) = candidate {
                    owners.entry(candidate.clone()).or_default().push(index);
                }
            }
            for indexes in owners.values().filter(|indexes| indexes.len() > 1) {
                for index in indexes {
                    let (host, _, remote) = &records[*index];
                    candidates[*index] = match remote {
                        Some(remote) => mirrored_agent_name_hashed(host, remote),
                        None => mirrored_unnamed_agent_name(host, &records[*index].1, true),
                    };
                }
            }

            let mapped_panes: HashSet<String> = plan
                .expected_hosts
                .iter()
                .flat_map(|host| load_state(state_dir, host).panes.into_values().map(|entry| entry.local_id))
                .collect();
            let native_names: HashSet<String> = local
                .agents
                .iter()
                .filter(|agent| !mapped_panes.contains(&agent.pane_id))
                .filter_map(|agent| agent.name.clone())
                .collect();
            let mut final_owners: HashMap<String, Vec<usize>> = HashMap::new();
            for (index, candidate) in candidates.iter().enumerate() {
                if let Some(candidate) = candidate {
                    final_owners.entry(candidate.clone()).or_default().push(index);
                }
            }
            for (index, (host, pane, remote)) in records.iter().enumerate() {
                let mut candidate = candidates[index].clone();
                if remote.is_none()
                    && candidate.as_ref().is_some_and(|name| native_names.contains(name))
                {
                    candidate = mirrored_unnamed_agent_name(host, pane, true);
                }
                let collision = candidate.as_ref().is_some_and(|name| {
                    native_names.contains(name)
                        || final_owners.get(name).is_some_and(|owners| owners.len() > 1)
                });
                let eligible = candidate.filter(|_| !collision);
                if eligible.is_none() {
                    log.log(&format!(
                        "[{host}] mirrored agent {} is ineligible: invalid or session-global name collision",
                        remote.as_deref().unwrap_or("<unnamed>")
                    ));
                }
                next.insert((host.clone(), pane.clone()), eligible);
            }
        }

        if next != plan.planned {
            plan.planned = next;
            plan.generation += 1;
            let _ = self.changed.send(plan.generation);
        }
        plan.planned
            .iter()
            .filter(|((host, _), _)| host == host_name)
            .map(|((_, pane), name)| (pane.clone(), name.clone()))
            .collect()
    }
}

/// argv for one mirror pane: this same binary in `pane` mode. Panes without a
/// known size get no --cols/--rows (the wrapper falls back to a default).
pub(crate) fn cmd_for_pane(
    host: &HostConfig,
    state_dir: &std::path::Path,
    sizes: &HashMap<String, LayoutRect>,
) -> impl Fn(&str) -> Vec<String> {
    let exe = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "herdr-mirror".into());
    let target = host.target.clone();
    let remote_bin = host.remote_bin.clone();
    let session = host.session.clone();
    let always_control = host.always_control;
    let max_cols = host.max_cols;
    let max_rows = host.max_rows;
    let kind = host.kind.clone();
    let docker_bin = host.docker_bin.clone();
    // daemon's ControlMaster socket for this host (see remote.rs); the streamer
    // reuses it for cheap foreground polls
    let ctl_path = crate::remote::control_path(state_dir, &host.name)
        .display()
        .to_string();
    let sizes = sizes.clone();
    move |pane_id: &str| {
        let mut argv = vec![
            exe.clone(),
            "pane".into(),
            target.clone(),
            pane_id.to_string(),
        ];
        // omit --remote-bin when auto (PATH then ~/.local/bin/herdr); pane
        // defaults to the same resolution so the argv stays short
        if let Some(bin) = &remote_bin {
            argv.extend(["--remote-bin".into(), bin.clone()]);
        }
        if let Some(session) = &session {
            argv.extend(["--session".into(), session.clone()]);
        }
        if always_control {
            argv.push("--always-control".into());
        }
        // absent when uncapped, so the argv of an unconfigured host is unchanged
        if let Some(c) = max_cols {
            argv.extend(["--max-cols".into(), c.to_string()]);
        }
        if let Some(r) = max_rows {
            argv.extend(["--max-rows".into(), r.to_string()]);
        }
        // ssh only: the pane reuses the daemon's ControlMaster for cheap
        // foreground polls. Docker has no ControlMaster, and healing no longer
        // needs a host-identity token in the argv at all — it asks herdr what
        // is running in each pane instead (see daemon::has_live_streamer).
        match &kind {
            crate::config::HostKind::Ssh => {
                argv.extend(["--ctl-path".into(), ctl_path.clone()]);
            }
            crate::config::HostKind::DockerContainer(name) => {
                argv.extend(["--container".into(), name.clone()]);
                argv.extend(["--docker-bin".into(), docker_bin.clone()]);
            }
            crate::config::HostKind::DockerFolder(folder) => {
                argv.extend(["--container-folder".into(), folder.clone()]);
                argv.extend(["--docker-bin".into(), docker_bin.clone()]);
            }
        }
        if let Some(rect) = sizes.get(pane_id) {
            argv.extend([
                "--cols".into(),
                (rect.width + OBSERVE_MARGIN_COLS).to_string(),
                "--rows".into(),
                (rect.height + OBSERVE_MARGIN_ROWS).to_string(),
            ]);
        }
        argv
    }
}

/// single-quote for a POSIX shell command line
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn streamer_exec_line(
    argv: &[String],
    state_dir: &std::path::Path,
) -> String {
    let command = argv.iter().map(|a| sh_quote(a)).collect::<Vec<_>>().join(" ");
    let state = sh_quote(&state_dir.display().to_string());
    // The pane's stable supervisor is deliberately not an agent. It launches
    // a child streamer with the desired hint and can replace that child without
    // letting Herdr close the pane when the remote agent changes.
    format!("exec env -u HERDR_AGENT HERDR_MIRROR_STATE_DIR={state} {command}\n")
}

/// Reconcile one mirrored tab's geometry: exchange panes into the arrangement
/// the remote has, then merge every split ratio three ways so a resize
/// propagates in whichever direction it was actually made.
///
/// `always_control` decides ties. It already means "this daemon drives the
/// remote's pane sizes" (the remote is headless, the local window is the only
/// one anyone looks at), so the local side wins there; a watch-only host has its
/// own display and its layout is authoritative.
async fn reconcile_tab_geometry(
    deps: &ConvergeDeps,
    state: &mut HostState,
    remote_tab: &str,
    local_tab: &str,
    remote_panes: &[&PaneInfo],
) {
    #[derive(Deserialize)]
    struct Exported {
        layout: ExportedLayout,
    }
    #[derive(Deserialize)]
    struct ExportedLayout {
        root: LayoutNode,
    }
    let remote_layout =
        deps.remote.request_t::<Exported>("layout.export", json!({ "tab_id": remote_tab })).await;
    let local_layout =
        deps.local.request_t::<Exported>("layout.export", json!({ "tab_id": local_tab })).await;
    let (Ok(remote), Ok(local)) = (remote_layout, local_layout) else { return };

    let map: BTreeMap<String, String> = remote_panes
        .iter()
        .filter_map(|p| {
            state
                .panes
                .get(&p.pane_id)
                .filter(|e| !e.is_tombstoned())
                .map(|e| (p.pane_id.clone(), e.local_id.clone()))
        })
        .collect();
    let prefix = format!("{remote_tab}|");
    let base: BTreeMap<String, f64> = state
        .ratios
        .iter()
        .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|path| (path.to_string(), *v)))
        .collect();
    let plan = crate::layout_sync::plan_sync(
        &remote.layout.root,
        &local.layout.root,
        &map,
        &base,
        deps.host.always_control,
    );

    if plan.structural_mismatch {
        // Shapes disagree, so ratios aren't comparable. Forget the agreement so
        // that whenever the shapes line up again the remote's geometry is
        // adopted cleanly, and say so once — the base is empty on later passes,
        // so this logs on the pass where it diverges, not on every one after.
        if !base.is_empty() {
            log_geometry_drift(&deps.log, remote_tab);
        }
        state.ratios.retain(|k, _| !k.starts_with(&prefix));
        return;
    }

    // swaps first: a ratio describes a position, so it only means the right
    // thing once the right pane is sitting in it
    for (source, target) in &plan.swaps {
        if let Err(e) = deps
            .local
            .request("pane.swap", json!({ "source_pane_id": source, "target_pane_id": target }))
            .await
        {
            deps.log.log(&format!("{remote_tab}: pane swap failed ({e}) — retrying next pass"));
            return;
        }
    }

    let mut failed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for fix in &plan.ratios {
        let (api, tab) = match fix.apply_to {
            crate::layout_sync::Side::Local => (&deps.local, local_tab),
            crate::layout_sync::Side::Remote => (&deps.remote, remote_tab),
        };
        let params = json!({ "tab_id": tab, "path": fix.path, "ratio": fix.ratio });
        if let Err(e) = api.request("layout.set_split_ratio", params).await {
            deps.log.log(&format!("{remote_tab}: split ratio sync failed: {e}"));
            failed.insert(crate::layout_sync::path_key(&fix.path));
        }
    }
    // Only remember what actually landed. Recording an agreement we failed to
    // write would make the next pass read the difference as an edit on the
    // other side and push it the wrong way.
    for (path, ratio) in plan.base {
        if failed.contains(&path) {
            continue;
        }
        state.ratios.insert(format!("{prefix}{path}"), ratio);
    }
}

fn log_geometry_drift(log: &Logger, remote_tab: &str) {
    log.log(&format!(
        "{remote_tab}: mirror no longer matches the remote's split shape — sizes won't track until it does"
    ));
}

/// Split `target` and return the new local pane id. `ratio` is the remote
/// split's own ratio when the placement is faithful, and None when we're
/// falling back, so herdr's default stands rather than a ratio describing a
/// different split.
async fn split_mirror_pane(
    local: &ApiClient,
    target: &str,
    direction: &str,
    ratio: Option<f64>,
    cwd: &str,
) -> Result<String> {
    #[derive(Deserialize)]
    struct Split {
        pane: SplitPane,
    }
    #[derive(Deserialize)]
    struct SplitPane {
        pane_id: String,
    }
    let mut params = json!({
        "target_pane_id": target,
        "direction": direction,
        "cwd": cwd,
        "focus": false,
    });
    if let Some(r) = ratio {
        params["ratio"] = json!(r);
    }
    let split: Split = local.request_t("pane.split", params).await?;
    Ok(split.pane.pane_id)
}

/// Does this local pane already have a live streamer?
///
/// Asks herdr what is actually running in the pane, rather than inferring it
/// from a global `ps` scan and string-matching argv. That inference is what
/// previously required a host-identity token in the argv, end-of-argument
/// anchoring so `work` could not match `work-staging`, and a compatibility
/// shim for streamers predating that token. None of it is needed to answer the
/// only question that matters: is something already running in THIS pane.
///
/// Generation-agnostic by construction — every streamer ever shipped is
/// `herdr-mirror pane …`, whatever flags follow.
///
/// `None` means herdr could not answer (socket blip, pane gone, an older
/// server without `pane.process_info`). Callers must read that as "unknown",
/// never as "nothing is running there".
pub(crate) async fn has_live_streamer(local: &ApiClient, pane_id: &str) -> Option<bool> {
    let v = local.request("pane.process_info", json!({ "pane_id": pane_id })).await.ok()?;
    let procs = v.pointer("/process_info/foreground_processes")?.as_array()?;
    Some(procs.iter().any(|p| {
        p.get("argv").and_then(|a| a.as_array()).is_some_and(|argv| is_streamer_argv(argv))
    }))
}

/// May we type a streamer exec line into this local pane?
///
/// The one predicate behind both paths that type into an existing pane: the
/// daemon healing zombie mirrors after a local server restart, and the startup
/// retype in `spawn_streamer_pane`. Both fail SAFE — only a definite "nothing
/// of ours is running there", from BOTH herdr's per-pane process info and our
/// pidfiles, permits typing.
///
/// Anything else leaves the pane alone. A frozen mirror is visible and
/// recoverable; typing into a pane whose streamer already owns stdin sends the
/// line on to the REMOTE shell, where `exec herdr-mirror …` finds no such
/// binary, kills that shell, and takes the remote pane — and with it a
/// single-pane workspace — down with it. That is exactly what a 3-second
/// pidfile-only timeout did to a Daytona sandbox on 2026-08-29: the owner's
/// slow interactive shell had not yet run the first copy of the line, so the
/// retype was queued in the local pty and forwarded by the streamer that
/// started a moment later.
pub(crate) fn streamer_exec_needed(process_info_live: Option<bool>, pidfile_live: bool) -> bool {
    process_info_live == Some(false) && !pidfile_live
}

/// Is this foreground process one of our pane wrappers?
///
/// argv[0] is the resolved exe path, which varies by install (release build,
/// plugin checkout, `cargo run`), so it is matched by suffix. argv[1] pins the
/// subcommand so an unrelated `herdr-mirror status` in the pane is not mistaken
/// for a live stream.
fn is_streamer_argv(argv: &[Value]) -> bool {
    argv.first().and_then(|s| s.as_str()).is_some_and(|e| e.ends_with("herdr-mirror"))
        && argv.get(1).and_then(|s| s.as_str()) == Some("pane")
}

/// Exec the streamer into an already-created plain pane. Not `agent.start` (or a
/// layout `command`), which set `launch_argv` and would surface every mirror pane
/// as an agent row; a shell `exec` keeps it non-agent until a real agent is
/// reported onto it.
pub(crate) async fn spawn_streamer_pane(
    local: &ApiClient,
    state_dir: &std::path::Path,
    local_pane_id: &str,
    argv: &[String],
    agent_hint: Option<&str>,
    log: &Logger,
) {
    let (Some(ssh_target), Some(pane_target)) = (argv.get(2).cloned(), argv.get(3).cloned())
    else {
        log.log(&format!("refusing malformed streamer command for {local_pane_id}"));
        return;
    };
    match crate::util::claim_streamer_spawn(
        state_dir,
        &ssh_target,
        &pane_target,
        local_pane_id,
    ) {
        Ok(crate::util::StreamerSpawnClaim::Claimed) => {}
        Ok(crate::util::StreamerSpawnClaim::Active) => {
            log.log(&format!("streamer for {pane_target} already active in {local_pane_id}; not retyping"));
            return;
        }
        Ok(crate::util::StreamerSpawnClaim::Pending) => {
            log.log(&format!("streamer launch for {pane_target} already pending in {local_pane_id}; not retyping"));
            return;
        }
        Err(e) => {
            log.log(&format!("cannot claim streamer launch for {local_pane_id}: {e}; not retyping"));
            return;
        }
    }

    if let Err(e) = crate::state::set_pane_agent_hint(state_dir, local_pane_id, agent_hint) {
        crate::util::clear_streamer_spawn_pending(state_dir, local_pane_id);
        log.log(&format!("store agent hint for {local_pane_id}: {e}"));
        return;
    }
    let line = streamer_exec_line(argv, state_dir);
    if let Err(e) = local
        .request("pane.send_text", json!({ "pane_id": local_pane_id, "text": line }))
        .await
    {
        crate::util::clear_streamer_spawn_pending(state_dir, local_pane_id);
        log.log(&format!("spawn streamer {local_pane_id}: {e}"));
        return;
    }

    // Typed input can be eaten by interactive shell startup (oh-my-zsh's
    // update prompt swallows the first key — in EVERY new shell until it's
    // answered). Verify the streamer registered its pidfile and retype the
    // exec if not, off-loop so a slow shell never stalls reconcile.
    //
    // A missing pidfile alone does NOT mean the exec was eaten: an owner's
    // plugin-laden zsh can take longer to reach the line than any timeout worth
    // waiting, and the streamer publishes its pid only after that. So each
    // resend is gated on herdr's own per-pane process info as well (the same
    // question the zombie heal asks): retype only when herdr definitely reports
    // a plain shell in the pane AND no pidfile is alive. Unknown is not
    // permission — see `streamer_exec_needed`.
    //
    // The ladder is 3s+4s+8s so a slow shell gets a real chance before the last
    // attempt, then a 4s settle; 19s total stays inside
    // `util::SPAWN_PENDING_TTL` (30s), so an abandoned claim still expires.
    let (local, log, state_dir) = (local.clone(), log.clone(), state_dir.to_path_buf());
    let pane_id = local_pane_id.to_string();
    tokio::spawn(async move {
        for wait_ms in [3000u64, 4000, 8000] {
            tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
            let pidfile_live = crate::util::streamer_alive(&state_dir, &ssh_target, &pane_target)
                || crate::util::pane_streamer_alive(&state_dir, &pane_id);
            if pidfile_live {
                return;
            }
            let process_info_live = has_live_streamer(&local, &pane_id).await;
            if !streamer_exec_needed(process_info_live, pidfile_live) {
                // Some(true): the streamer is running and simply has not
                // published its pid yet. None: herdr could not tell us. Either
                // way, stop typing and let the pending claim expire; a pane
                // that really is a dead shell is healed on the daemon's next
                // reconnect to the local server, which cannot hurt a live
                // remote session.
                log.log(&format!(
                    "streamer for {pane_target} not up in {pane_id} but herdr reports {} — not retyping",
                    match process_info_live {
                        Some(true) => "a live streamer",
                        _ => "no usable process info",
                    }
                ));
                return;
            }
            log.log(&format!(
                "streamer for {pane_target} not up in {pane_id} and the pane is still a shell — retyping"
            ));
            if local
                .request("pane.send_text", json!({ "pane_id": pane_id, "text": line }))
                .await
                .is_err()
            {
                crate::util::clear_streamer_spawn_pending(&state_dir, &pane_id);
                return; // pane gone (closed meanwhile) — nothing to heal
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(4000)).await;
        if !crate::util::streamer_alive(&state_dir, &ssh_target, &pane_target)
            && !crate::util::pane_streamer_alive(&state_dir, &pane_id)
        {
            crate::util::clear_streamer_spawn_pending(&state_dir, &pane_id);
            log.log(&format!(
                "streamer for {pane_target} still not up in {pane_id} after retries — pane left as a shell"
            ));
        }
    });
}

/// cwd every mirror pane runs in, doubling as the loop-guard marker: it's set at
/// pane creation so it's in the snapshot immediately (no exec race), and its name
/// can't collide with a real dir.
const MIRROR_CWD_MARKER: &str = ".mirror-pane";

fn mirror_pane_cwd(state_dir: &std::path::Path) -> std::path::PathBuf {
    state_dir.join(MIRROR_CWD_MARKER)
}

/// Is this remote pane another herdr-mirror's streamer pane? Read from the
/// snapshot cwd marker — free, and race-free.
fn pane_is_mirror(p: &PaneInfo) -> bool {
    let is_marker = |c: &Option<String>| {
        c.as_deref()
            .and_then(|s| std::path::Path::new(s).file_name())
            .and_then(|f| f.to_str())
            == Some(MIRROR_CWD_MARKER)
    };
    is_marker(&p.foreground_cwd) || is_marker(&p.cwd)
}

// --- the converge pass ---

/// A fresh local id just entered the map (mirror created or adopted). Two
/// duties, both time-sensitive. Purge any stale user-close recorded against
/// the id: herdr reuses freed ids, so a close noted before this moment can
/// only refer to a previous holder, and letting it linger would close-through
/// the new mirror's REMOTE within USER_CLOSE_TTL. And persist the map right
/// away instead of at pass end, so the intercept hook's map read (250ms after
/// pane.created) sees the daemon-built object as mapped instead of judging it
/// native junk and closing it.
fn note_mapped(deps: &ConvergeDeps, state: &HostState, fresh_local_ids: &[String]) {
    if let Ok(mut t) = deps.closes.lock() {
        for id in fresh_local_ids {
            t.forget(id);
        }
    }
    if let Err(e) = save_state(&deps.state_dir, &deps.host.name, state) {
        // Not cosmetic: this write is what tells the intercept hook these
        // objects are ours. An unwritable state dir would otherwise leave every
        // daemon-created pane looking like native junk, silently.
        deps.log.log(&format!("could not persist map after mapping fresh ids: {e}"));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MissingLocalAction {
    Tombstone,
    Rebuild,
}

/// Snapshot absence is not user intent. Herdr emits an authoritative close
/// event for a user action, while a server restart makes every persisted local
/// id disappear without one. Preserve only the former as a tombstone; discard
/// stale mappings from the latter so this converge can rebuild them.
pub(crate) fn missing_local_action(observed_close: bool) -> MissingLocalAction {
    if observed_close {
        MissingLocalAction::Tombstone
    } else {
        MissingLocalAction::Rebuild
    }
}

/// Returns the post-converge state so callers don't re-read the state file.
pub async fn converge(deps: &ConvergeDeps) -> Result<HostState> {
    let mut state = load_state(&deps.state_dir, &deps.host.name);
    let result = converge_inner(deps, &mut state).await;
    // save even on error: a crash mid-pass must not orphan created mirrors
    save_state(&deps.state_dir, &deps.host.name, &state)?;
    result.map(|()| state)
}

async fn converge_inner(deps: &ConvergeDeps, state: &mut HostState) -> Result<()> {
    let host = &deps.host;
    let log = &deps.log;
    // Hidden hosts freeze here and go no further. Deliberately BEFORE the
    // snapshots: a hidden host should be quiet, not keep paying for two RPCs a
    // minute forever.
    //
    // This branch only freezes; it never closes. Closing has to happen exactly
    // once, in the process that owns the authoritative close tracker, and
    // `converge` is called by `once` and other one-shots that carry a throwaway
    // tracker (see `cmd_once`). Doing the close here meant those marked on a
    // tracker nobody reads while the daemon's event stream recorded every close
    // as user intent — which is the chain that closed two real remote
    // workspaces. The daemon does it in `apply_hidden` instead.
    if crate::state::is_hidden(&deps.state_dir, &deps.host.name) {
        // Hidden contributes an empty source to the session-global identity
        // barrier. Otherwise one intentionally hidden host would prevent every
        // visible host from ever receiving a dispatch name.
        if let Ok(local_snap) = fetch_snapshot(&deps.local).await {
            deps.names.update(
                &deps.host.name,
                &[],
                &local_snap,
                &deps.state_dir,
                &deps.log,
            );
        }
        return Ok(());
    }

    let (remote_snap, local_snap) =
        tokio::try_join!(fetch_snapshot(&deps.remote), fetch_snapshot(&deps.local))?;

    let remote_agent_by_pane: HashMap<&str, &AgentInfo> =
        remote_snap.agents.iter().map(|a| (a.pane_id.as_str(), a)).collect();
    let agent_hint_for = |pane_id: &str| {
        remote_agent_by_pane
            .get(pane_id)
            .and_then(|agent| agent.agent.as_deref())
            .filter(|agent| !agent.is_empty())
    };

    let mut local_ws_ids: HashSet<String> =
        local_snap.workspaces.iter().map(|w| w.workspace_id.clone()).collect();
    let local_tab_ids: HashSet<&str> = local_snap.tabs.iter().map(|t| t.tab_id.as_str()).collect();
    let local_pane_ids: HashSet<&str> = local_snap.panes.iter().map(|p| p.pane_id.as_str()).collect();
    let remote_ws_ids: HashSet<&str> = remote_snap.workspaces.iter().map(|w| w.workspace_id.as_str()).collect();
    let remote_tab_ids: HashSet<&str> = remote_snap.tabs.iter().map(|t| t.tab_id.as_str()).collect();
    let remote_pane_ids: HashSet<&str> = remote_snap.panes.iter().map(|p| p.pane_id.as_str()).collect();
    let mut sizes: HashMap<String, LayoutRect> = HashMap::new();
    for layout in &remote_snap.layouts {
        for p in &layout.panes {
            sizes.insert(p.pane_id.clone(), p.rect.clone());
        }
    }
    let cmd_for = cmd_for_pane(&deps.host, &deps.state_dir, &sizes);
    let _ = std::fs::create_dir_all(mirror_pane_cwd(&deps.state_dir));

    // 1. detect mirrors that are gone locally. Only an event-confirmed user
    //    close becomes a tombstone. Absence without that event means the local
    //    id map is stale (most importantly after a local server restart), so
    //    remove it and let this same converge rebuild the mirror.
    //
    //    Closing the REMOTE is destructive and uses the same authoritative
    //    close event. A snapshot alone never closes or suppresses a live remote.
    let close_remote = deps.close_remote_on_local_close;
    let mine: HashSet<String> = state
        .workspaces
        .values()
        .map(|e| e.local_id.clone())
        .chain(state.panes.values().map(|e| e.local_id.clone()))
        .chain(state.tabs.values().map(|e| e.local_id.clone()))
        .collect();
    let user_closed = match deps.closes.lock() {
        Ok(mut t) => t.take_user_closed(&mine),
        Err(_) => HashSet::new(),
    };
    let mut ws_close_remote: Vec<String> = Vec::new();
    let mut drop_workspaces: Vec<String> = Vec::new();
    for (rid, entry) in state.workspaces.iter_mut() {
        if !entry.is_tombstoned() && !local_ws_ids.contains(&entry.local_id) && remote_ws_ids.contains(rid.as_str()) {
            match missing_local_action(user_closed.contains(&entry.local_id)) {
                MissingLocalAction::Tombstone => {
                    entry.tombstone = Some(true);
                    if close_remote {
                        ws_close_remote.push(rid.clone());
                    } else {
                        log.log(&format!(
                            "workspace mirror for {rid} was closed locally — tombstoning"
                        ));
                    }
                }
                MissingLocalAction::Rebuild => {
                    log.log(&format!(
                        "workspace mirror for {rid} vanished without a close event — rebuilding"
                    ));
                    drop_workspaces.push(rid.clone());
                }
            }
        }
    }
    for rid in drop_workspaces {
        state.workspaces.remove(&rid);
    }
    for rid in &ws_close_remote {
        log.log(&format!("workspace mirror for {rid} closed locally — closing remote workspace"));
        if let Err(e) = deps.remote.request("workspace.close", json!({ "workspace_id": rid })).await {
            log.log(&format!("remote workspace close failed for {rid}: {e}"));
        }
    }
    let pane_ws: HashMap<&str, &str> =
        remote_snap.panes.iter().map(|p| (p.pane_id.as_str(), p.workspace_id.as_str())).collect();
    let pane_tab: HashMap<&str, &str> =
        remote_snap.panes.iter().map(|p| (p.pane_id.as_str(), p.tab_id.as_str())).collect();
    let mut drop_panes: Vec<String> = Vec::new();
    let mut pane_close_remote: Vec<String> = Vec::new();
    for (rid, entry) in state.panes.iter_mut() {
        if !entry.is_tombstoned() && !local_pane_ids.contains(entry.local_id.as_str()) && remote_pane_ids.contains(rid.as_str()) {
            let ws_entry = pane_ws.get(rid.as_str()).and_then(|ws| state.workspaces.get(*ws));
            // if the pane's whole mirror workspace is gone, the stale pane
            // entry is collateral — drop it (its tombstoned workspace already
            // blocks recreation)
            match ws_entry {
                Some(w) if !w.is_tombstoned() && local_ws_ids.contains(&w.local_id) => {
                    // user intent covers the pane itself AND its whole tab:
                    // closing a tab emits only tab_closed, so the panes inside
                    // it are claimed through their tab's mapped local id
                    let tab_closed = pane_tab
                        .get(rid.as_str())
                        .and_then(|t| state.tabs.get(*t))
                        .is_some_and(|e| user_closed.contains(&e.local_id));
                    let observed_close = user_closed.contains(&entry.local_id) || tab_closed;
                    match missing_local_action(observed_close) {
                        MissingLocalAction::Tombstone => {
                            entry.tombstone = Some(true);
                            if close_remote {
                                pane_close_remote.push(rid.clone());
                            } else {
                                log.log(&format!(
                                    "pane mirror for {rid} was closed locally — tombstoning"
                                ));
                            }
                        }
                        MissingLocalAction::Rebuild => {
                            log.log(&format!(
                                "pane mirror for {rid} vanished without a close event — rebuilding"
                            ));
                            drop_panes.push(rid.clone());
                        }
                    }
                }
                _ => drop_panes.push(rid.clone()),
            }
        }
    }
    for rid in &pane_close_remote {
        log.log(&format!("pane mirror for {rid} closed locally — closing remote pane"));
        if let Err(e) = deps.remote.request("pane.close", json!({ "pane_id": rid })).await {
            log.log(&format!("remote pane close failed for {rid}: {e}"));
        }
    }
    for rid in drop_panes {
        state.panes.remove(&rid);
    }

    // 2. remote objects that disappeared → close their mirrors. Explicit
    //    `*.closed` events are the authoritative close path (see apply_remote_closes);
    //    this snapshot-absence sweep is only a backstop for missed events, and it
    //    acts only when the object was ALSO absent last pass — so a remote that
    //    reconnected mid-restore (transiently empty/partial snapshot) can't
    //    mass-close mirrors.
    let prev_ids = std::mem::take(&mut state.prev_remote_ids);
    let absent_twice = |rid: &str, present: &HashSet<&str>| {
        !present.contains(rid) && !prev_ids.contains(rid)
    };
    let gone_ws: Vec<String> =
        state.workspaces.keys().filter(|rid| absent_twice(rid, &remote_ws_ids)).cloned().collect();
    for rid in gone_ws {
        let entry = state.workspaces.remove(&rid).unwrap();
        if !entry.is_tombstoned() && local_ws_ids.contains(&entry.local_id) {
            log.log(&format!("remote workspace {rid} gone — closing mirror {}", entry.local_id));
            mark_self_close(deps, &entry.local_id);
            if let Err(e) = deps.local.request("workspace.close", json!({ "workspace_id": entry.local_id })).await {
                log.log(&format!("close failed: {e}"));
            }
        }
    }
    let gone_tabs: Vec<String> =
        state.tabs.keys().filter(|rid| absent_twice(rid, &remote_tab_ids)).cloned().collect();
    for rid in gone_tabs {
        let entry = state.tabs.remove(&rid).unwrap();
        if local_tab_ids.contains(entry.local_id.as_str()) {
            let _ = deps.local.request("tab.close", json!({ "tab_id": entry.local_id })).await;
        }
    }
    let gone_panes: Vec<String> =
        state.panes.keys().filter(|rid| absent_twice(rid, &remote_pane_ids)).cloned().collect();
    for rid in gone_panes {
        let entry = state.panes.remove(&rid).unwrap();
        if !entry.is_tombstoned() && local_pane_ids.contains(entry.local_id.as_str()) {
            mark_self_close(deps, &entry.local_id);
            let _ = deps.local.request("pane.close", json!({ "pane_id": entry.local_id })).await;
        }
    }
    // record this pass's remote ids for the next comparison
    state.prev_remote_ids = remote_ws_ids
        .iter()
        .chain(remote_tab_ids.iter())
        .chain(remote_pane_ids.iter())
        .map(|s| s.to_string())
        .collect();

    // skip remote workspaces that are entirely another herdr-mirror's streamer
    // panes (a machine mirroring us back), so mutual mirroring can't nest.
    let mut panes_by_ws: HashMap<&str, Vec<&PaneInfo>> = HashMap::new();
    for p in &remote_snap.panes {
        panes_by_ws.entry(p.workspace_id.as_str()).or_default().push(p);
    }
    let mut mirror_ws_ids: HashSet<String> = HashSet::new();
    for rws in &remote_snap.workspaces {
        let Some(panes) = panes_by_ws.get(rws.workspace_id.as_str()).filter(|p| !p.is_empty()) else {
            continue;
        };
        if panes.iter().all(|p| pane_is_mirror(p)) {
            mirror_ws_ids.insert(rws.workspace_id.clone());
        }
    }

    // 3. remote workspaces → ensure mirrors exist with the right label
    for rws in &remote_snap.workspaces {
        if mirror_ws_ids.contains(&rws.workspace_id) {
            continue;
        }
        let label = format!("{}: {}", host.prefix, rws.label);
        if state.workspaces.get(&rws.workspace_id).is_some_and(|e| e.is_tombstoned()) {
            continue;
        }
        let existing = state
            .workspaces
            .get(&rws.workspace_id)
            .filter(|e| local_ws_ids.contains(&e.local_id))
            .cloned();
        if let Some(entry) = existing {
            let local_ws = local_snap.workspaces.iter().find(|w| w.workspace_id == entry.local_id);
            if let Some(lws) = local_ws {
                match resolve_label(Some(&host.prefix), &rws.label, &lws.label, entry.last_remote_label.as_deref()) {
                    LabelAction::PushRemote(new_remote) => {
                        // the user renamed the mirror → the rename is intent for
                        // the REMOTE workspace; push it there and restamp local
                        // with the canonical "<prefix>: <name>" form
                        log.log(&format!(
                            "local rename of {} → pushing \"{new_remote}\" to remote {}",
                            lws.label, rws.workspace_id
                        ));
                        deps.remote
                            .request(
                                "workspace.rename",
                                json!({ "workspace_id": rws.workspace_id, "label": new_remote }),
                            )
                            .await?;
                        let stamped = format!("{}: {}", host.prefix, new_remote);
                        if lws.label != stamped {
                            deps.local
                                .request("workspace.rename", json!({ "workspace_id": entry.local_id, "label": stamped }))
                                .await?;
                        }
                        if let Some(e) = state.workspaces.get_mut(&rws.workspace_id) {
                            e.last_remote_label = Some(new_remote);
                        }
                    }
                    LabelAction::RestampLocal => {
                        deps.local
                            .request("workspace.rename", json!({ "workspace_id": entry.local_id, "label": label }))
                            .await?;
                        if let Some(e) = state.workspaces.get_mut(&rws.workspace_id) {
                            e.last_remote_label = Some(rws.label.clone());
                        }
                    }
                    LabelAction::InSync => {
                        if entry.last_remote_label.as_deref() != Some(rws.label.as_str()) {
                            if let Some(e) = state.workspaces.get_mut(&rws.workspace_id) {
                                e.last_remote_label = Some(rws.label.clone());
                            }
                        }
                    }
                }
            }
        } else {
            // adopt a label-matching unmapped local workspace (orphan from a crash)
            let mapped: HashSet<&str> = state.workspaces.values().map(|e| e.local_id.as_str()).collect();
            let orphan = local_snap
                .workspaces
                .iter()
                .find(|w| w.label == label && !mapped.contains(w.workspace_id.as_str()));
            let entry = if let Some(orphan) = orphan {
                log.log(&format!("adopting existing workspace {label} ({})", orphan.workspace_id));
                WsEntry {
                    local_id: orphan.workspace_id.clone(),
                    tombstone: None,
                    root_tab_local_id: if orphan.tab_count == Some(1) && orphan.pane_count == Some(1) {
                        orphan.active_tab_id.clone()
                    } else {
                        None
                    },
                    last_remote_label: Some(rws.label.clone()),
                }
            } else {
                log.log(&format!("creating mirror workspace {label}"));
                #[derive(Deserialize)]
                struct Created {
                    workspace: CreatedWs,
                    tab: CreatedTab,
                }
                #[derive(Deserialize)]
                struct CreatedWs {
                    workspace_id: String,
                }
                #[derive(Deserialize)]
                struct CreatedTab {
                    tab_id: String,
                }
                // same non-git marker cwd the mirror panes use, so the
                // workspace's default pane never flashes a (misleading) sidebar
                // git branch before layout.apply swaps in the real mirror panes
                let cwd = mirror_pane_cwd(&deps.state_dir).display().to_string();
                let created: Created = deps
                    .local
                    .request_t("workspace.create", json!({ "label": label, "cwd": cwd, "focus": false }))
                    .await?;
                WsEntry {
                    local_id: created.workspace.workspace_id,
                    tombstone: None,
                    root_tab_local_id: Some(created.tab.tab_id),
                    last_remote_label: Some(rws.label.clone()),
                }
            };
            local_ws_ids.insert(entry.local_id.clone());
            let fresh: Vec<String> = std::iter::once(entry.local_id.clone())
                .chain(entry.root_tab_local_id.clone())
                .collect();
            state.workspaces.insert(rws.workspace_id.clone(), entry);
            note_mapped(deps, state, &fresh);
        }
    }

    // 3b. forward the remote's workspace tokens onto the mirror rows, so a mirror
    //     carries the same values a native workspace does under whatever layout is
    //     configured locally. Ignored by a pre-0.7.4 local server.
    let source = mirror_source(&host.name);
    for rws in &remote_snap.workspaces {
        if rws.tokens.is_empty() {
            continue; // nothing to forward (also the pre-0.7.4 remote case)
        }
        let Some(entry) = state.workspaces.get(&rws.workspace_id) else { continue };
        if entry.is_tombstoned() || !local_ws_ids.contains(&entry.local_id) {
            continue;
        }
        let _ = deps
            .local
            .request(
                "workspace.report_metadata",
                json!({ "workspace_id": entry.local_id, "source": source, "tokens": rws.tokens }),
            )
            .await;
    }

    // 4. remote tabs → replicate layout with wrapper commands
    for rtab in &remote_snap.tabs {
        let Some(ws_entry) = state.workspaces.get(&rtab.workspace_id).cloned() else { continue };
        if ws_entry.is_tombstoned() {
            continue;
        }
        let tab_entry = state.tabs.get(&rtab.tab_id).cloned();
        let tab_exists = tab_entry.as_ref().is_some_and(|t| local_tab_ids.contains(t.local_id.as_str()));
        let remote_panes_in_tab: Vec<&PaneInfo> =
            remote_snap.panes.iter().filter(|p| p.tab_id == rtab.tab_id).collect();
        // A tab whose mirror the user closed leaves only tombstoned pane
        // entries behind (a TabEntry has no tombstone of its own — its stale
        // local id just stops resolving). Rebuilding it would recreate panes
        // the tombstones then forbid wiring up; skip before the layout.export
        // round-trip. `restore` deletes the tombstones, which lifts this.
        if !tab_exists
            && !remote_panes_in_tab.is_empty()
            && remote_panes_in_tab
                .iter()
                .all(|p| state.panes.get(&p.pane_id).is_some_and(|e| e.is_tombstoned()))
        {
            continue;
        }

        if !tab_exists || remote_panes_in_tab.iter().any(|p| !state.panes.contains_key(&p.pane_id)) {
            #[derive(Deserialize)]
            struct Exported {
                layout: ExportedLayout,
            }
            #[derive(Deserialize)]
            struct ExportedLayout {
                root: LayoutNode,
            }
            let exported: Exported =
                deps.remote.request_t("layout.export", json!({ "tab_id": rtab.tab_id })).await?;

            if !tab_exists {
                // apply only the non-tombstoned part of the tree: layout.apply
                // creates a real local pane per leaf, so a tombstoned leaf
                // would materialize as a titled dead shell in the marker cwd
                // that the mapping loop below then can't wire a streamer into
                let Some(live_root) = prune_closed(&exported.layout.root, &state.panes) else {
                    continue;
                };
                let mut remote_order = Vec::new();
                walk_pane_ids(&live_root, &mut remote_order);
                // non-git cwd so herdr shows no (misleading) sidebar git status
                // for the mirror; the pane exec's the streamer regardless
                let cwd = mirror_pane_cwd(&deps.state_dir).display().to_string();
                let root = map_node(&live_root, &cwd);
                let target_tab = ws_entry.root_tab_local_id.clone();
                // tab_id and workspace_id are mutually exclusive on layout.apply
                let mut params = json!({ "tab_label": rtab.label, "root": root, "focus": false });
                match &target_tab {
                    Some(t) => params["tab_id"] = json!(t),
                    None => params["workspace_id"] = json!(ws_entry.local_id),
                }
                #[derive(Deserialize)]
                struct Applied {
                    layout: AppliedLayout,
                }
                #[derive(Deserialize)]
                struct AppliedLayout {
                    tab_id: String,
                    root: LayoutNode,
                }
                let applied: Applied = deps.local.request_t("layout.apply", params).await?;
                // consume the root tab only AFTER a successful apply, so a
                // transient failure retries against it instead of stacking a tab
                if let Some(ws) = state.workspaces.get_mut(&rtab.workspace_id) {
                    ws.root_tab_local_id = None;
                }
                // applied with `tab_label: rtab.label`, so the two agree from birth
                let mut fresh = vec![applied.layout.tab_id.clone()];
                state.tabs.insert(
                    rtab.tab_id.clone(),
                    crate::state::TabEntry {
                        local_id: applied.layout.tab_id,
                        last_remote_label: Some(rtab.label.clone()),
                    },
                );
                let mut local_order = Vec::new();
                walk_pane_ids(&applied.layout.root, &mut local_order);
                // map every pane first and persist, THEN exec streamers: the
                // panes already exist (layout.apply made them), so the map
                // write must not wait behind the send_text round-trips
                let mut to_spawn: Vec<(String, String)> = Vec::new();
                for (i, rid) in remote_order.iter().enumerate() {
                    if rid.is_empty() || local_order.get(i).is_none_or(|l| l.is_empty()) {
                        continue;
                    }
                    let local_id = local_order[i].clone();
                    let seq = state.panes.get(rid).map(|e| e.seq).unwrap_or(0);
                    state.panes.insert(
                        rid.clone(),
                        PaneEntry {
                            local_id: local_id.clone(),
                            tombstone: None,
                            seq,
                            reported: None,
                            reported_name: None,
                            remote_agent_name: None,
                            projected_rosemary_run: None,
                            identity_ineligible: false,
                            identity_cleanup_pending: false,
                        },
                    );
                    fresh.push(local_id.clone());
                    to_spawn.push((local_id, rid.clone()));
                }
                note_mapped(deps, state, &fresh);
                for (local_id, rid) in &to_spawn {
                    // plain pane created above; exec the streamer into it
                    spawn_streamer_pane(
                        &deps.local,
                        &deps.state_dir,
                        local_id,
                        &cmd_for(rid),
                        agent_hint_for(rid),
                        &deps.log,
                    )
                    .await;
                }
            } else {
                // tab exists — add mirrors for individual new remote panes as
                // PLAIN split panes (not agent.start), then exec the streamer in.
                // agent.start would set launch_argv and surface every plain
                // terminal as a phantom "mirror" agent row.
                // non-git cwd so herdr shows no (misleading) sidebar git status
                // for the mirror; the pane exec's the streamer regardless
                let cwd = mirror_pane_cwd(&deps.state_dir).display().to_string();
                // Place new panes where the REMOTE tree says they live, in
                // dependency order, so a burst of several (a converge that fell
                // behind, or a whole nested tab) reproduces the remote's shape
                // instead of flattening every new pane onto one target. Each
                // split carries the remote split's ratio; `swap` covers the
                // case where the remote has the new pane as the FIRST child,
                // which pane.split can't do directly.
                let mirrored: std::collections::BTreeSet<String> = state
                    .panes
                    .iter()
                    .filter(|(_, e)| !e.is_tombstoned())
                    .map(|(rid, _)| rid.clone())
                    .collect();
                let (placements, unplaceable) =
                    crate::layout_sync::plan_placements(&exported.layout.root, &mirrored);
                let in_this_tab = |rid: &String| remote_panes_in_tab.iter().any(|p| &p.pane_id == rid);
                for place in placements.iter().filter(|p| in_this_tab(&p.pane)) {
                    if state.panes.contains_key(&place.pane) {
                        continue;
                    }
                    let Some(target) = state.panes.get(&place.target).map(|e| e.local_id.clone())
                    else {
                        continue;
                    };
                    let local_id = split_mirror_pane(
                        &deps.local,
                        &target,
                        &place.direction,
                        Some(place.ratio),
                        &cwd,
                    )
                    .await?;
                    // the remote has this pane on the split's first side, and
                    // pane.split always lands the new one second. Swapping puts
                    // it where the remote has it; the split's ratio rides along
                    // untouched, so the geometry matches exactly.
                    if place.swap {
                        let _ = deps
                            .local
                            .request(
                                "pane.swap",
                                json!({ "source_pane_id": local_id, "target_pane_id": target }),
                            )
                            .await;
                    }
                    // map + persist BEFORE the streamer exec: the pane exists
                    // as of pane.split above, and the intercept hook judges
                    // unmapped placeholder panes 250ms after pane.created
                    state.panes.insert(
                        place.pane.clone(),
                        PaneEntry {
                            local_id: local_id.clone(),
                            tombstone: None,
                            seq: 0,
                            reported: None,
                            reported_name: None,
                            remote_agent_name: None,
                            projected_rosemary_run: None,
                            identity_ineligible: false,
                            identity_cleanup_pending: false,
                        },
                    );
                    note_mapped(deps, state, std::slice::from_ref(&local_id));
                    spawn_streamer_pane(
                        &deps.local,
                        &deps.state_dir,
                        &local_id,
                        &cmd_for(&place.pane),
                        agent_hint_for(&place.pane),
                        &deps.log,
                    )
                        .await;
                }
                // A pane whose remote sibling is a multi-pane subtree can't be
                // reproduced: pane.split splits a leaf, and nothing wraps a
                // subtree in a new split. Place it by the old heuristic so the
                // mirror is never missing a pane, and say so — the tab's ratio
                // sync will report a structural mismatch from here on.
                for rp in remote_panes_in_tab.iter().filter(|p| unplaceable.contains(&p.pane_id)) {
                    if state.panes.contains_key(&rp.pane_id) {
                        continue;
                    }
                    let fallback = locate_in_layout(&exported.layout.root, &rp.pane_id)
                        .and_then(|(dir, sibs)| {
                            sibs.iter()
                                .find_map(|rid| state.panes.get(rid).map(|e| e.local_id.clone()))
                                .map(|t| (t, dir))
                        })
                        .or_else(|| {
                            remote_panes_in_tab
                                .iter()
                                .find_map(|p| state.panes.get(&p.pane_id).map(|e| e.local_id.clone()))
                                .map(|t| (t, "right".to_string()))
                        });
                    let Some((target, direction)) = fallback else { continue };
                    log.log(&format!(
                        "{}: remote pane {} sits beside a subtree — mirroring it beside {target} instead; split sizes for this tab won't track",
                        rtab.tab_id, rp.pane_id
                    ));
                    let local_id =
                        split_mirror_pane(&deps.local, &target, &direction, None, &cwd).await?;
                    state.panes.insert(
                        rp.pane_id.clone(),
                        PaneEntry {
                            local_id: local_id.clone(),
                            tombstone: None,
                            seq: 0,
                            reported: None,
                            reported_name: None,
                            remote_agent_name: None,
                            projected_rosemary_run: None,
                            identity_ineligible: false,
                            identity_cleanup_pending: false,
                        },
                    );
                    note_mapped(deps, state, std::slice::from_ref(&local_id));
                    spawn_streamer_pane(
                        &deps.local,
                        &deps.state_dir,
                        &local_id,
                        &cmd_for(&rp.pane_id),
                        agent_hint_for(&rp.pane_id),
                        &deps.log,
                    )
                        .await;
                }
            }
        }

        if tab_exists {
            let entry = tab_entry.as_ref().unwrap();
            let tab_local = &entry.local_id;
            let local_tab = local_snap.tabs.iter().find(|t| &t.tab_id == tab_local);
            // Same two-way resolution the workspace labels get above: a local
            // rename is intent for the REMOTE tab, and only a remote that moved
            // since we last stamped may overwrite the local label.
            if let Some(ltab) = local_tab {
                match resolve_label(None, &rtab.label, &ltab.label, entry.last_remote_label.as_deref()) {
                    LabelAction::PushRemote(new_remote) => {
                        log.log(&format!(
                            "local rename of tab {tab_local} → pushing \"{new_remote}\" to remote {}",
                            rtab.tab_id
                        ));
                        // record the new label only once the remote has it, so a
                        // failed push is retried by the next converge instead of
                        // being mistaken for a remote rename and stomped
                        if deps
                            .remote
                            .request("tab.rename", json!({ "tab_id": rtab.tab_id, "label": new_remote }))
                            .await
                            .is_ok()
                        {
                            if let Some(e) = state.tabs.get_mut(&rtab.tab_id) {
                                e.last_remote_label = Some(new_remote);
                            }
                        }
                    }
                    LabelAction::RestampLocal => {
                        // same discipline as the push above, for the same reason
                        // in reverse: recording a label the local tab never took
                        // makes the next converge read the stale local one as a
                        // user rename and push it over the remote's
                        if deps
                            .local
                            .request("tab.rename", json!({ "tab_id": tab_local, "label": rtab.label }))
                            .await
                            .is_ok()
                        {
                            if let Some(e) = state.tabs.get_mut(&rtab.tab_id) {
                                e.last_remote_label = Some(rtab.label.clone());
                            }
                        }
                    }
                    LabelAction::InSync => {
                        if entry.last_remote_label.as_deref() != Some(rtab.label.as_str()) {
                            if let Some(e) = state.tabs.get_mut(&rtab.tab_id) {
                                e.last_remote_label = Some(rtab.label.clone());
                            }
                        }
                    }
                }
            }

            // Placement above only sets a ratio for a split it JUST created. A
            // resize of an existing split has no topology change to hang off
            // of, so it needs its own check: `layout.updated` is subscribed on
            // both sides (see daemon.rs), and each converge diffs the two
            // exports and moves whichever side didn't change.
            //
            // A tab with fewer than two panes has no split to reconcile, which
            // is most tabs, so it costs nothing there.
            if remote_panes_in_tab.len() > 1 {
                reconcile_tab_geometry(deps, state, &rtab.tab_id, tab_local, &remote_panes_in_tab)
                    .await;
            }
        }
    }

    // remembered ratio agreements for tabs that are gone would otherwise
    // accumulate in the state file forever
    let live_tabs: HashSet<String> = state.tabs.keys().cloned().collect();
    state.ratios.retain(|k, _| k.split('|').next().is_some_and(|t| live_tabs.contains(t)));

    // 5. push authoritative agent status onto mirror panes
    push_statuses(deps, &remote_snap, &local_snap, state).await;
    Ok(())
}

/// Push one pane's authoritative status (or retract it when the remote agent
/// is gone). Mutates only its own entry (seq/reported). Reused by both the
/// full converge and the daemon's status fast-path.
const ROSEMARY_RUN_KEYS: [&str; 4] = [
    "rosemary_binding",
    "rosemary_outcome",
    "rosemary_commit",
    "rosemary_summary",
];

fn rosemary_run(tokens: &HashMap<String, String>) -> Option<RosemaryRun> {
    let binding = tokens.get(ROSEMARY_RUN_KEYS[0])?;
    if binding.trim().is_empty() {
        return None;
    }
    Some(RosemaryRun {
        binding: binding.clone(),
        outcome: tokens.get(ROSEMARY_RUN_KEYS[1]).cloned(),
        commit: tokens.get(ROSEMARY_RUN_KEYS[2]).cloned(),
        summary: tokens.get(ROSEMARY_RUN_KEYS[3]).cloned(),
    })
}

fn projected_tokens(
    state: &mut HostState,
    remote_name: Option<&str>,
    tokens: &HashMap<String, String>,
) -> (BTreeMap<String, Value>, BTreeMap<String, Value>, Option<RosemaryRun>) {
    let ordinary = ordinary_projected_tokens(tokens);
    let cleared = || {
        ROSEMARY_RUN_KEYS
            .into_iter()
            .map(|key| (key.to_string(), Value::Null))
            .collect()
    };
    let Some(remote_name) = remote_name.filter(|name| !name.is_empty()) else {
        return (ordinary, cleared(), rosemary_run(tokens));
    };
    let run = rosemary_run(tokens);
    if let (Some(suppressed), Some(current)) =
        (state.rosemary_suppressions.get(remote_name), run.as_ref())
    {
        if current.binding != suppressed.binding {
            state.rosemary_suppressions.remove(remote_name);
        } else if current == suppressed {
            return (ordinary, cleared(), None);
        }
    }
    let rosemary = ROSEMARY_RUN_KEYS
        .into_iter()
        .map(|key| {
            (
                key.to_string(),
                tokens.get(key).map(|value| json!(value)).unwrap_or(Value::Null),
            )
        })
        .collect();
    (ordinary, rosemary, run)
}

fn ordinary_projected_tokens(tokens: &HashMap<String, String>) -> BTreeMap<String, Value> {
    let mut ordinary: BTreeMap<String, Value> = tokens
        .iter()
        .map(|(key, value)| (key.clone(), json!(value)))
        .collect();
    for key in ROSEMARY_RUN_KEYS {
        // Revision 6 projected everything under mirror:<host>. Nulling these
        // keys retires that legacy ownership before rosemary-run takes over.
        ordinary.insert(key.to_string(), Value::Null);
    }
    ordinary
}

/// A local `pane.updated` is only a doorbell. Read the authoritative pane and,
/// when Rosemary removed the exact tuple we most recently projected, persist
/// suppression before the host task is allowed to reconcile again.
pub async fn capture_local_rosemary_clear(
    local: &ApiClient,
    state_dir: &std::path::Path,
    host_name: &str,
    local_pane_id: &str,
    log: &Logger,
) -> Result<bool> {
    let mut state = load_state(state_dir, host_name);
    let Some(entry) = state.panes.values().find(|entry| entry.local_id == local_pane_id) else {
        return Ok(false);
    };
    let (Some(remote_name), Some(projected)) =
        (entry.remote_agent_name.clone(), entry.projected_rosemary_run.clone())
    else {
        return Ok(false);
    };
    let snapshot = fetch_snapshot(local).await?;
    let tokens = snapshot
        .agents
        .iter()
        .find(|agent| agent.pane_id == local_pane_id)
        .map(|agent| &agent.tokens);
    let cleared = tokens.is_some_and(|tokens| {
        ROSEMARY_RUN_KEYS.iter().all(|key| !tokens.contains_key(*key))
    });
    if !cleared {
        return Ok(false);
    }
    state.rosemary_suppressions.insert(remote_name.clone(), projected);
    save_state(state_dir, host_name, &state).map_err(|error| {
        log.log(&format!("[{host_name}] could not persist Rosemary suppression for {remote_name}: {error}"));
        error
    })?;
    Ok(true)
}

async fn keep_agent_ineligible(
    deps: &PaneStatusDeps<'_>,
    remote_id: &str,
    exact_remote_name: &str,
    state: &mut HostState,
    reason: &str,
) {
    let PaneStatusDeps { local, state_dir, host_name, log, .. } = deps;
    let source = mirror_source(host_name);
    if state
        .panes
        .get(remote_id)
        .is_some_and(|entry| entry.identity_ineligible && !entry.identity_cleanup_pending)
    {
        return;
    }
    let local_id = {
        let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
        entry.identity_ineligible = true;
        entry.identity_cleanup_pending = true;
        entry.local_id.clone()
    };
    log.log(&format!(
        "[{host_name}] mirrored agent {exact_remote_name:?} is ineligible: {reason}; clearing local identity and authority"
    ));
    if let Err(error) = save_state(state_dir, host_name, state) {
        log.log(&format!(
            "[{host_name}] could not durably mark mirrored agent {exact_remote_name:?} ineligible: {error}"
        ));
    }
    loop {
        let rename = local
            .request("agent.rename", json!({ "target": local_id, "name": Value::Null }))
            .await;
        let authority = local
            .request(
                "pane.clear_agent_authority",
                json!({ "pane_id": local_id, "source": source }),
            )
            .await;
        if let Err(error) = &rename {
            log.log(&format!(
                "[{host_name}] cleanup failed for mirrored agent {exact_remote_name:?}: clear name: {error}"
            ));
        }
        if let Err(error) = &authority {
            log.log(&format!(
                "[{host_name}] cleanup failed for mirrored agent {exact_remote_name:?}: clear authority: {error}"
            ));
        }
        let local_pane_gone = if rename.is_err() || authority.is_err() {
            fetch_snapshot(local).await.ok().is_some_and(|snapshot| {
                !snapshot.panes.iter().any(|pane| pane.pane_id == local_id)
            })
        } else {
            false
        };
        if (rename.is_ok() && authority.is_ok()) || local_pane_gone {
            let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
            entry.reported_name = None;
            entry.reported = None;
            entry.remote_agent_name = None;
            entry.projected_rosemary_run = None;
            entry.identity_cleanup_pending = false;
            if let Err(error) = save_state(state_dir, host_name, state) {
                log.log(&format!(
                    "[{host_name}] could not persist completed cleanup for mirrored agent {exact_remote_name:?}: {error}"
                ));
            }
            if local_pane_gone {
                log.log(&format!(
                    "[{host_name}] local pane {local_id} vanished during identity cleanup; stale identity and authority are already absent"
                ));
            }
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

pub async fn push_pane_status(
    deps: &PaneStatusDeps<'_>,
    remote_id: &str,
    state: &mut HostState,
    agent: Option<&AgentInfo>,
    desired_name: Option<String>,
) {
    let PaneStatusDeps { local, state_dir, host_name, log, rosemary_gate } = deps;
    if state.panes.get(remote_id).is_none_or(PaneEntry::is_tombstoned) {
        return;
    }
    let remote_name = agent.and_then(|agent| agent.name.as_deref()).filter(|name| !name.is_empty());
    let projected_agent_tokens = agent
        .map(|agent| ordinary_projected_tokens(&agent.tokens))
        .unwrap_or_default();
    let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
    let source = mirror_source(host_name);
    match agent {
        Some(agent) => {
            let exact_remote_name = agent.name.as_deref().unwrap_or("");
            // A non-empty remote name whose planned local name was refused is
            // kept fail-closed. An unnamed agent may still proceed without a
            // name only if even the workspace-derived fallback was impossible.
            if !exact_remote_name.is_empty() && desired_name.is_none() {
                keep_agent_ineligible(
                    deps,
                    remote_id,
                    exact_remote_name,
                    state,
                    "the session-global name plan refused the candidate",
                )
                .await;
                return;
            }
            // A fresh mirror pane is not a local agent until report_agent
            // lands. Defer its first rename until after that report; otherwise
            // Herdr correctly rejects the unknown target and the agent enters
            // a permanent cleanup loop.
            let rename_after_report = entry.reported.is_none()
                && desired_name != entry.reported_name;
            if desired_name != entry.reported_name && !rename_after_report {
                let local_id = entry.local_id.clone();
                match local
                    .request(
                        "agent.rename",
                        json!({ "target": local_id, "name": desired_name }),
                    )
                    .await
                {
                    Ok(_) => {
                        let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
                        entry.reported_name = desired_name.clone();
                        entry.identity_ineligible = false;
                        entry.identity_cleanup_pending = false;
                    }
                    Err(error) => {
                        keep_agent_ineligible(
                            deps,
                            remote_id,
                            exact_remote_name,
                            state,
                            &format!("agent.rename refused {desired_name:?}: {error}"),
                        )
                        .await;
                        return;
                    }
                }
            }
            let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
            entry.seq += 1;
            let display = agent.display_agent.clone().or_else(|| agent.agent.clone());
            // Identity is the remote's CANONICAL id ("claude"), not the pretty
            // name: herdr canonicalizes a reported label, so this resolves the
            // real agent and the mirror row inherits its rows_by_agent layout and
            // icon instead of rendering as a nameless custom agent. The pretty
            // name still goes out below as display_agent, which is what the
            // sidebar actually shows.
            let label = agent
                .agent
                .clone()
                .or_else(|| display.clone())
                .unwrap_or_else(|| "agent".into());
            // pass through only a custom status the remote actually reports;
            // no synthetic "@host" marker (clear any stale one)
            let custom: Option<String> = agent.custom_status.as_deref().map(clamp_status);
            let status = agent.agent_status.as_deref().unwrap_or("unknown");
            let mut report = json!({
                "pane_id": entry.local_id,
                "source": source,
                "agent": label,
                "state": map_status(status),
                "seq": entry.seq,
            });
            report["agent_session_id"] = if agent.interactive_ready {
                agent.agent_session.as_ref().map(|session| json!(session.value)).unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            if let Some(c) = &custom {
                report["custom_status"] = json!(c);
            }
            if let Err(e) = local.request("pane.report_agent", report).await {
                log.log(&format!("report_agent {}: {e}", entry.local_id));
                return;
            }
            if rename_after_report {
                let local_id = entry.local_id.clone();
                match local
                    .request(
                        "agent.rename",
                        json!({ "target": local_id, "name": desired_name }),
                    )
                    .await
                {
                    Ok(_) => {
                        let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
                        entry.reported_name = desired_name.clone();
                        entry.identity_ineligible = false;
                        entry.identity_cleanup_pending = false;
                    }
                    Err(error) => {
                        keep_agent_ineligible(
                            deps,
                            remote_id,
                            exact_remote_name,
                            state,
                            &format!("agent.rename refused {desired_name:?}: {error}"),
                        )
                        .await;
                        return;
                    }
                }
            }
            let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
            // Herdr's typed agent operations validate the pane's real
            // foreground process, not only lifecycle reports. Tell the local
            // streamer which supported wrapper it represents; a changed hint
            // makes that streamer cleanly re-exec itself and leaves the remote
            // pane untouched.
            match crate::state::set_pane_agent_hint(state_dir, &entry.local_id, Some(&label)) {
                Ok(true) => {
                    if !crate::util::poke_pane_streamer(state_dir, &entry.local_id) {
                        log.log(&format!(
                            "agent hint for {} changed to {label}, but its streamer was not signalable",
                            entry.local_id
                        ));
                    }
                }
                Ok(false) => {}
                Err(e) => log.log(&format!("store agent hint for {}: {e}", entry.local_id)),
            }
            // Agent names are session-global on the local Herdr server. A
            // remote fleet commonly repeats role names such as `conductor`,
            // so namespace the remote name with the stable Mirror host id.
            // Resolve the whole snapshot before this call, and keep the result
            // inside the local server's 32-character rule. A refused or
            // residual-collision name is cleared instead of leaving an older
            // dispatch identity behind.
            // forward the remote's own tokens so a mirrored agent row carries the
            // same values a native one does, under whatever layout is configured
            // locally. Ignored by a pre-0.7.4 local server (no deny_unknown_fields).
            let mut meta = json!({
                "pane_id": entry.local_id,
                "source": source,
                "display_agent": display,
                "title": agent.effective_title(),
                "state_labels": agent.state_labels.clone().unwrap_or_default(),
                "tokens": projected_agent_tokens,
                "seq": entry.seq,
            });
            if custom.is_none() {
                meta["clear_custom_status"] = json!(true);
            }
            if let Err(error) = local.request("pane.report_metadata", meta).await {
                log.log(&format!("report_metadata {}: {error}", entry.local_id));
            }
            // A local update may have arrived while this converge waited on
            // earlier RPCs. Read the authoritative snapshot before the next
            // Rosemary write and reload any durable suppression into this
            // pass. Herdr exposes no revision/CAS: if an already-started RPC
            // overwrote a transient clear before this read, there is no clear
            // left for Mirror to claim it observed.
            if rosemary_gate.has_local_update(&entry.local_id) {
                match capture_local_rosemary_clear(local, state_dir, host_name, &entry.local_id, log).await {
                    Ok(_) => {
                        if let Some(remote_name) = remote_name {
                            let durable = load_state(state_dir, host_name);
                            match durable.rosemary_suppressions.get(remote_name) {
                                Some(run) => {
                                    state.rosemary_suppressions.insert(remote_name.to_string(), run.clone());
                                }
                                None => {
                                    state.rosemary_suppressions.remove(remote_name);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        log.log(&format!(
                            "[{host_name}] local Rosemary update is pending; projection remains blocked: {error}"
                        ));
                        return;
                    }
                }
            }
            let (.., projected_rosemary_tokens, projected_run) =
                projected_tokens(state, remote_name, &agent.tokens);
            let entry = state.panes.get_mut(remote_id).expect("pane entry checked above");
            let rosemary_meta = json!({
                "pane_id": entry.local_id,
                "source": "rosemary-run",
                "tokens": projected_rosemary_tokens,
                "seq": entry.seq,
            });
            match local.request("pane.report_metadata", rosemary_meta).await {
                Ok(_) => {
                    entry.remote_agent_name = remote_name.map(str::to_string);
                    entry.projected_rosemary_run = projected_run;
                }
                Err(error) => log.log(&format!(
                    "report Rosemary metadata {}: {error}",
                    entry.local_id
                )),
            }
            entry.reported = Some(label);
        }
        None => {
            if entry.reported_name.is_some() {
                let _ = local
                    .request(
                        "agent.rename",
                        json!({ "target": entry.local_id, "name": Value::Null }),
                    )
                    .await;
                entry.reported_name = None;
            }
            match crate::state::set_pane_agent_hint(state_dir, &entry.local_id, None) {
                Ok(true) => {
                    let _ = crate::util::poke_pane_streamer(state_dir, &entry.local_id);
                }
                Ok(false) => {}
                Err(e) => log.log(&format!("clear agent hint for {}: {e}", entry.local_id)),
            }
            let Some(reported) = entry.reported.clone() else { return };
            // remote agent exited — retract our claim so the mirror pane doesn't
            // show a phantom agent row forever
            entry.seq += 1;
            log.log(&format!("remote agent gone on {remote_id} — releasing {reported} from {}", entry.local_id));
            if let Err(e) = local
                .request(
                    "pane.release_agent",
                    json!({ "pane_id": entry.local_id, "source": source, "agent": reported, "seq": entry.seq }),
                )
                .await
            {
                log.log(&format!("release_agent {}: {e}", entry.local_id));
            }
            entry.seq += 1;
            let _ = local
                .request(
                    "pane.report_metadata",
                    json!({
                        "pane_id": entry.local_id,
                        "source": source,
                        "clear_display_agent": true,
                        "clear_custom_status": true,
                        "clear_state_labels": true,
                        "clear_title": true,
                        "seq": entry.seq,
                    }),
                )
                .await;
            let _ = local
                .request(
                    "pane.report_metadata",
                    json!({
                        "pane_id": entry.local_id,
                        "source": "rosemary-run",
                        "tokens": ROSEMARY_RUN_KEYS.into_iter()
                            .map(|key| (key, Value::Null)).collect::<BTreeMap<_, _>>(),
                        "seq": entry.seq,
                    }),
                )
                .await;
            entry.reported = None;
            entry.remote_agent_name = None;
            entry.projected_rosemary_run = None;
        }
    }
}

/// The local Herdr server's rule for an agent name: start with a lowercase
/// letter, then only lowercase letters, digits, `-` and `_`, 1-32 characters.
/// A name that breaks it is refused outright — `agent.rename: agent name must
/// start with a lowercase letter and contain only lowercase letters, digits,
/// '-' or '_' (1-32 characters)` — and converge retries the same rename on
/// every poll, so one bad name is a permanent log fire and a permanently
/// unnamed agent row.
const AGENT_NAME_MAX: usize = 32;

/// The shortest host prefix worth keeping. Below three characters the prefix
/// stops distinguishing fleets, which is the only reason it exists.
const HOST_PREFIX_MIN: usize = 3;

/// Fold one part of a name into the local server's alphabet: lowercase, and
/// anything outside `[a-z0-9_-]` becomes `-`. Only a fold — a component is not
/// a name, so nothing is dropped here for starting with a digit; the assembled
/// name is what has to start with a letter.
fn sanitize_agent_component(s: &str) -> String {
    s.trim()
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Make an assembled name legal, or say there is no name to be had: a name
/// starts with a lowercase letter, so leading digits and separators are
/// dropped; a trailing `-` left by truncation is dropped for tidiness; and the
/// whole thing is capped at `AGENT_NAME_MAX`. `None` when nothing usable is
/// left, which is better than a rename the server refuses on every poll.
fn legal_agent_name(name: &str) -> Option<String> {
    let start = name.trim_start_matches(|c: char| !c.is_ascii_lowercase());
    let capped: String = start.chars().take(AGENT_NAME_MAX).collect();
    let trimmed = capped.trim_end_matches('-');
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// The local name for a mirrored remote agent: the remote's own name,
/// namespaced by the Mirror host so two fleets can both run a `conductor`, and
/// always a name the local server will accept.
///
/// An already-legal full name is retained. Only names that need shortening get
/// a digest, derived from the exact source pair before either half is folded.
/// The complete Rosemary conductor suffix is kept because it is dispatch
/// identity, not decoration.
pub(crate) fn mirrored_agent_name(host_name: &str, remote_name: Option<&str>) -> Option<String> {
    let exact_remote = remote_name?;
    if exact_remote.trim().is_empty() {
        return None;
    }
    let remote = sanitize_agent_component(exact_remote);
    // A remote name of nothing but separators would collapse to the bare host
    // prefix, and every such agent on that host would collapse to the SAME
    // name. No name at all is better: the mirrored row keeps the remote's
    // title and the local server is never asked for an impossible rename.
    if !remote.chars().any(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    let host = sanitize_agent_component(host_name);
    if host.is_empty() {
        return None;
    }
    let full = format!("{host}-{remote}");
    if full.len() <= AGENT_NAME_MAX {
        return legal_agent_name(&full);
    }

    mirrored_agent_name_hashed(host_name, exact_remote)
}

/// Give an unnamed remote agent the workspace/host name the owner already
/// recognizes. A single unnamed agent gets the bare host name. Multiple
/// unnamed agents (or a collision with a native name) get a stable pane-based
/// digest so the plan remains deterministic across converge passes.
fn mirrored_unnamed_agent_name(
    host_name: &str,
    pane_id: &str,
    require_suffix: bool,
) -> Option<String> {
    let host = sanitize_agent_component(host_name);
    if !host.chars().any(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    if !require_suffix && host.len() <= AGENT_NAME_MAX {
        return legal_agent_name(&host);
    }

    let mut hasher = Sha256::new();
    hasher.update(host_name.as_bytes());
    hasher.update([0]);
    hasher.update(pane_id.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    let digest = &digest[..8];
    let prefix_budget = AGENT_NAME_MAX - 1 - digest.len();
    let readable = host
        .trim_start_matches(|c: char| !c.is_ascii_lowercase())
        .chars()
        .take(prefix_budget)
        .collect::<String>();
    let readable = readable.trim_end_matches(['-', '_']);
    if readable.is_empty() {
        return None;
    }
    legal_agent_name(&format!("{readable}-{digest}"))
}

fn mirrored_agent_name_hashed(host_name: &str, exact_remote: &str) -> Option<String> {
    let remote = sanitize_agent_component(exact_remote);
    if !remote.chars().any(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    let host = sanitize_agent_component(host_name);
    if host.is_empty() {
        return None;
    }
    let full = format!("{host}-{remote}");

    let mut hasher = Sha256::new();
    hasher.update(host_name.as_bytes());
    hasher.update([0]);
    hasher.update(exact_remote.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    let digest = &digest[..8];
    const ROSIE_SUFFIX: &str = "-conductor-rosie";
    let suffix = if remote.ends_with(ROSIE_SUFFIX) {
        ROSIE_SUFFIX
    } else {
        ""
    };
    let prefix_budget = AGENT_NAME_MAX - 1 - digest.len() - suffix.len();
    let readable = full
        .trim_start_matches(|c: char| !c.is_ascii_lowercase())
        .chars()
        .take(prefix_budget.max(HOST_PREFIX_MIN))
        .collect::<String>();
    let readable = readable.trim_end_matches(['-', '_']);
    if readable.is_empty() || readable.len() > prefix_budget {
        return None;
    }
    legal_agent_name(&format!("{readable}-{digest}{suffix}"))
}

/// Authoritative close path: apply explicit remote `*.closed` events by closing
/// the matching local mirror and pruning state. Ids are namespaced (ws `w1`, tab
/// `w1:t1`, pane `w1:p1`), so each is looked up wherever it lives. Closing a
/// workspace mirror cascades to its tabs/panes locally; stale child state entries
/// are pruned by the next converge.
pub async fn apply_remote_closes(
    local: &ApiClient,
    state_dir: &std::path::Path,
    host_name: &str,
    closed: &[String],
    log: &Logger,
) {
    if closed.is_empty() {
        return;
    }
    let mut state = load_state(state_dir, host_name);
    let mut changed = false;
    for rid in closed {
        if let Some(entry) = state.workspaces.remove(rid) {
            changed = true;
            if !entry.is_tombstoned() {
                log.log(&format!("remote workspace {rid} closed — closing mirror {}", entry.local_id));
                let _ = local.request("workspace.close", json!({ "workspace_id": entry.local_id })).await;
            }
        } else if let Some(entry) = state.tabs.remove(rid) {
            changed = true;
            let _ = local.request("tab.close", json!({ "tab_id": entry.local_id })).await;
        } else if let Some(entry) = state.panes.remove(rid) {
            changed = true;
            if !entry.is_tombstoned() {
                let _ = local.request("pane.close", json!({ "pane_id": entry.local_id })).await;
            }
        }
    }
    if changed {
        if let Err(e) = save_state(state_dir, host_name, &state) {
            log.log(&format!("[{host_name}] state save failed: {e}"));
        }
    }
}

pub async fn push_statuses(
    deps: &ConvergeDeps,
    remote_snap: &Snapshot,
    local_snap: &Snapshot,
    state: &mut HostState,
) {
    let agent_by_pane: HashMap<&str, &AgentInfo> =
        remote_snap.agents.iter().map(|a| (a.pane_id.as_str(), a)).collect();
    let names = deps.names.update(
        &deps.host.name,
        &remote_snap.agents,
        local_snap,
        &deps.state_dir,
        &deps.log,
    );
    let remote_ids: Vec<String> = state.panes.keys().cloned().collect();
    for remote_id in remote_ids {
        let agent = agent_by_pane.get(remote_id.as_str()).copied();
        push_pane_status(
            &PaneStatusDeps {
                local: &deps.local,
                state_dir: &deps.state_dir,
                host_name: &deps.host.name,
                log: &deps.log,
                rosemary_gate: &deps.rosemary_gate,
            },
            &remote_id,
            state,
            agent,
            names.get(&remote_id).cloned().flatten(),
        )
        .await;
    }
}

/// Mark mirrored agents unknown (ssh drop) — statuses recover on reconnect.
/// Only panes we actually reported an agent onto; inventing agent rows for
/// plain mirrored terminals pollutes the agents panel.
pub async fn mark_unknown(local: &ApiClient, state_dir: &std::path::Path, host_name: &str, reason: &str) {
    let mut state = load_state(state_dir, host_name);
    let source = mirror_source(host_name);
    let custom = clamp_status(reason);
    for entry in state.panes.values_mut() {
        let Some(reported) = entry.reported.clone() else { continue };
        if entry.is_tombstoned() {
            continue;
        }
        entry.seq += 1;
        let _ = local
            .request(
                "pane.report_agent",
                json!({
                    "pane_id": entry.local_id,
                    "source": source,
                    "agent": reported,
                    "state": "unknown",
                    "custom_status": custom,
                    "seq": entry.seq,
                }),
            )
            .await;
    }
    let _ = save_state(state_dir, host_name, &state);
}

/// Graceful teardown: close every mirror workspace this host created.
pub async fn teardown(
    local: &ApiClient,
    state_dir: &std::path::Path,
    host_name: &str,
    log: &Logger,
    closes: Option<&crate::closes::Closes>,
) -> Result<()> {
    let state = load_state(state_dir, host_name);
    // Wipe the id map BEFORE closing the local windows. teardown (and the
    // restart / zombie-heal that call it) means "stop mirroring here" — never
    // "close the remote sessions". But close_remote_on_local_close fires when a
    // converge sees a still-mapped mirror vanish locally, and it can't tell our
    // bulk close from the user pressing prefix-x. Clearing the map first leaves
    // nothing to attribute these closes to, so they cannot propagate to the
    // remote. Manual close is unaffected: there the entry is still mapped when
    // the user closes it, so the intent still reaches the remote.
    save_state(state_dir, host_name, &HostState::default())?;
    // teardown means "stop mirroring here entirely", which supersedes hide —
    // leaving the marker would make a later `start` bring back nothing with no
    // explanation of why
    let _ = crate::state::set_hidden(state_dir, host_name, false);
    for entry in state.workspaces.values() {
        log.log(&format!("closing mirror workspace {}", entry.local_id));
        // ours, not the user's: the heal re-adopts these ids, so without the mark
        // the echoing close event would later read as "user closed the mirror"
        if let Some(c) = closes {
            if let Ok(mut t) = c.lock() {
                t.mark_self_close(&entry.local_id);
            }
        }
        let _ = local.request("workspace.close", json!({ "workspace_id": entry.local_id })).await;
    }
    Ok(())
}

async fn move_ws(local: &ApiClient, ws: &str, insert_index: usize) -> bool {
    local
        .request("workspace.move", json!({ "workspace_id": ws, "insert_index": insert_index }))
        .await
        .is_ok()
}

/// rank a workspace by its label: local (no `<prefix>: `) sorts first (0), then
/// each host's mirrors by config order (i+1). First matching prefix wins.
fn ws_rank(label: &str, prefixes: &[String]) -> usize {
    for (i, p) in prefixes.iter().enumerate() {
        if label.starts_with(&format!("{p}: ")) {
            return i + 1;
        }
    }
    0
}

/// Pure planner: given the current `(workspace_id, rank)` order, return the
/// `(workspace_id, insert_index)` workspace.move calls that group the sidebar
/// (locals first, then mirror ranks ascending, preserving order within each
/// group), moving ONLY mirror rows (rank > 0). Empty when already grouped.
///
/// `insert_index` is herdr's pre-removal gap index: pulling a row up lands it at
/// `i`; pushing one to the end uses `insert_index == len`.
fn plan_regroup(current: &[(String, usize)]) -> Vec<(String, usize)> {
    let mut target = current.to_vec();
    target.sort_by_key(|(_, r)| *r); // stable: preserves order within each group
    if current == target.as_slice() {
        return Vec::new();
    }
    let mut moves = Vec::new();
    let mut working = current.to_vec();
    let n = working.len();
    let mut i = 0usize;
    let mut guard = 0usize;
    while i < target.len() {
        guard += 1;
        if guard > n * n + 8 {
            break;
        }
        if working[i].0 == target[i].0 {
            i += 1;
            continue;
        }
        if target[i].1 > 0 {
            // a mirror belongs at i and is currently later — pull it up to i
            let want = target[i].0.clone();
            let src = working.iter().position(|(id, _)| *id == want).unwrap();
            moves.push((want.clone(), i));
            let item = working.remove(src);
            working.insert(i, item);
            i += 1;
        } else if i + 1 < working.len() {
            // a local belongs at i but a mirror sits there — push that mirror to the end
            let m = working[i].0.clone();
            moves.push((m.clone(), working.len()));
            let item = working.remove(i);
            working.push(item);
        } else {
            i += 1;
        }
    }
    moves
}

/// Keep the local sidebar grouped: local (non-mirror) workspaces first, then each
/// host's mirror workspaces contiguous in config order. Classifies by the
/// `<prefix>: ` label the mirror sets, and only ever moves mirror rows — local
/// workspaces are never reordered (they group as a side effect of mirror rows
/// being pushed below them). Idempotent: issues no moves when already grouped.
pub async fn regroup_sidebar(local: &ApiClient, prefixes: &[String], log: &Logger) {
    let Ok(snap) = fetch_snapshot(local).await else { return };
    let current: Vec<(String, usize)> =
        snap.workspaces.iter().map(|w| (w.workspace_id.clone(), ws_rank(&w.label, prefixes))).collect();
    for (ws, insert_index) in plan_regroup(&current) {
        if !move_ws(local, &ws, insert_index).await {
            log.log(&format!("regroup: move {ws} failed"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{hidden_close_plan, missing_local_action, MissingLocalAction};

    fn ws_entry(local: &str, tomb: bool) -> WsEntry {
        WsEntry {
            local_id: local.into(),
            tombstone: tomb.then_some(true),
            root_tab_local_id: None,
            last_remote_label: None,
        }
    }

    fn live(ids: &[&str]) -> std::collections::HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    /// The bug that made the daemon close the mirrors it had just created, on
    /// every pass, for every host. It shipped past a full-green suite because
    /// nothing exercised the gate.
    #[test]
    fn a_host_that_is_not_hidden_is_never_touched() {
        let mut state = HostState::default();
        state.workspaces.insert("R1".into(), ws_entry("w7", false));
        let doomed = hidden_close_plan(false, &mut state, &live(&["w7"]));
        assert!(doomed.is_empty(), "closed a mirror on a host nobody hid");
        assert_eq!(state.workspaces.len(), 1, "map must survive untouched");
    }

    #[test]
    fn hiding_closes_live_mirrors_and_keeps_tombstones() {
        let mut state = HostState::default();
        state.workspaces.insert("R1".into(), ws_entry("w7", false));
        state.workspaces.insert("R2".into(), ws_entry("w8", true)); // user closed it
        state.workspaces.insert("R3".into(), ws_entry("w9", false)); // already gone
        let doomed = hidden_close_plan(true, &mut state, &live(&["w7", "w8"]));
        assert_eq!(doomed, vec!["w7".to_string()], "only the live, non-tombstoned one");
        // the tombstone survives, or `show` resurrects a mirror the user closed
        assert!(state.workspaces.contains_key("R2"));
        assert!(!state.workspaces.contains_key("R1"));
        assert!(!state.workspaces.contains_key("R3"));
    }

    #[test]
    fn only_an_observed_close_becomes_a_tombstone() {
        assert_eq!(missing_local_action(true), MissingLocalAction::Tombstone);
        assert_eq!(missing_local_action(false), MissingLocalAction::Rebuild);
    }
    use super::*;

    struct FakePeer {
        path: PathBuf,
        requests: std::sync::Arc<std::sync::Mutex<Vec<Value>>>,
        failures: std::sync::Arc<std::sync::Mutex<HashMap<String, usize>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl FakePeer {
        async fn start(label: &str, snapshot: Value) -> Self {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            use tokio::net::UnixListener;

            let path = std::env::temp_dir().join(format!(
                "herdr-mirror-rosemary-{}-{label}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&path);
            let listener = UnixListener::bind(&path).unwrap();
            let snapshot = std::sync::Arc::new(std::sync::Mutex::new(snapshot));
            let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let failures = std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));
            let snapshots = snapshot.clone();
            let captured = requests.clone();
            let rejected = failures.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else { break };
                    let snapshots = snapshots.clone();
                    let captured = captured.clone();
                    let rejected = rejected.clone();
                    tokio::spawn(async move {
                        let (read, mut write) = stream.into_split();
                        let mut lines = BufReader::new(read).lines();
                        let Ok(Some(line)) = lines.next_line().await else { return };
                        let request: Value = serde_json::from_str(&line).unwrap();
                        captured.lock().unwrap().push(request.clone());
                        let method = request["method"].as_str().unwrap_or("");
                        let refused = {
                            let mut rejected = rejected.lock().unwrap();
                            rejected.get_mut(method).is_some_and(|remaining| {
                                if *remaining == 0 {
                                    false
                                } else {
                                    *remaining -= 1;
                                    true
                                }
                            })
                        };
                        let response = if refused {
                            json!({"id": request["id"], "error": {"message": "injected refusal"}})
                        } else {
                            let result = if request["method"] == "session.snapshot" {
                                json!({"snapshot": snapshots.lock().unwrap().clone()})
                            } else {
                                json!({"type": "ok"})
                            };
                            json!({"id": request["id"], "result": result})
                        };
                        write.write_all(format!("{response}\n").as_bytes()).await.unwrap();
                    });
                }
            });
            Self { path, requests, failures, task }
        }

        fn requests(&self) -> Vec<Value> {
            self.requests.lock().unwrap().clone()
        }

        fn fail(&self, method: &str, times: usize) {
            self.failures.lock().unwrap().insert(method.to_string(), times);
        }
    }

    impl Drop for FakePeer {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn local_rosemary_snapshot(local_pane: &str, include_run: bool) -> Value {
        let tokens = if include_run {
            json!({"rosemary_binding": "run-1", "rosemary_outcome": "complete",
                "rosemary_commit": "abc123", "rosemary_summary": "done"})
        } else {
            json!({})
        };
        json!({
            "workspaces": [
                {"workspace_id": "lw", "label": "vps: feature", "tab_count": 1, "pane_count": 1, "active_tab_id": "lt"},
                {"workspace_id": "native-w", "label": "native", "tab_count": 1, "pane_count": 1, "active_tab_id": "native-t"}],
            "tabs": [{"tab_id": "lt", "workspace_id": "lw", "label": "main"},
                {"tab_id": "native-t", "workspace_id": "native-w", "label": "main"}],
            "panes": [{"pane_id": local_pane, "tab_id": "lt", "workspace_id": "lw", "label": null,
                    "cwd": "/tmp", "foreground_cwd": "/tmp"},
                {"pane_id": "native-p", "tab_id": "native-t", "workspace_id": "native-w", "label": null,
                    "cwd": "/native", "foreground_cwd": "/native"}],
            "agents": [{"pane_id": local_pane, "agent": "codex", "name": "vps-conductor-rosie",
                    "agent_status": "idle", "tokens": tokens},
                {"pane_id": "native-p", "agent": "codex", "name": "native-agent", "agent_status": "idle",
                    "interactive_ready": true, "agent_session": {"value": "native-session"}, "tokens": {}}],
            "layouts": []
        })
    }

    fn ssh_host() -> HostConfig {
        HostConfig {
            name: "vps".into(),
            target: "vps".into(),
            kind: crate::config::HostKind::Ssh,
            docker_bin: "docker".into(),
            prefix: "vps".into(),
            remote_bin: None,
            session: None,
            always_control: true,
            max_cols: None,
            max_rows: None,
            api_transport: crate::config::ApiTransport::Auto,
        }
    }

    #[test]
    fn streamer_supervisor_is_not_itself_an_agent() {
        let argv = vec!["herdr-mirror".into(), "pane".into(), "host".into(), "w1:p1".into()];
        assert_eq!(
            streamer_exec_line(&argv, std::path::Path::new("/state")),
            "exec env -u HERDR_AGENT HERDR_MIRROR_STATE_DIR='/state' 'herdr-mirror' 'pane' 'host' 'w1:p1'\n"
        );
    }

    /// One focused table covers the public naming contract. Generated outputs
    /// are deliberately absent: they are not source identities.
    #[test]
    fn mirrored_name_table_is_legal_stable_and_collision_resistant() {
        let legal = |name: &String| {
            (1..=AGENT_NAME_MAX).contains(&name.len())
                && name.starts_with(|c: char| c.is_ascii_lowercase())
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        };
        let cases = [
            ("greenroom", "conductor", "greenroom-conductor"),
            (
                "caddypayio-vm",
                "remote-conductor-rosie",
                "caddypa-bf6d3195-conductor-rosie",
            ),
            (
                "a-very-long-mirror-host-name-indeed",
                "conductor-for-the-first-workspace",
                "a-very-long-mirror-host-3761cc1f",
            ),
            (
                "a-very-long-mirror-host-name-indeed",
                "conductor-for-the-first-workspace-two",
                "a-very-long-mirror-host-269339fa",
            ),
        ];

        for (host, remote, expected) in cases {
            let name = mirrored_agent_name(host, Some(remote))
                .unwrap_or_else(|| panic!("{host} + {remote} produced no name"));
            assert_eq!(name, expected);
            assert!(legal(&name), "{host} + {remote} produced {name:?}");
            assert_eq!(
                mirrored_agent_name(host, Some(remote)),
                Some(name.clone()),
                "{host} + {remote} is not deterministic"
            );
        }
        assert!(cases[1].2.ends_with("-conductor-rosie"));
        assert_ne!(cases[2].2, cases[3].2, "the formerly colliding pairs must differ");

    }

    #[test]
    fn session_name_plan_handles_cross_host_native_and_residual_collisions() {
        let state_dir = std::env::temp_dir().join(format!("hm-name-plan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&state_dir);
        let empty_local: Snapshot = serde_json::from_value(json!({})).unwrap();
        let alpha = vec![AgentInfo {
            pane_id: "alpha-pane".into(),
            name: Some("beta-conductor-rosie".into()),
            ..AgentInfo::default()
        }];
        let alpha_beta = vec![AgentInfo {
            pane_id: "alpha-beta-pane".into(),
            name: Some("conductor-rosie".into()),
            ..AgentInfo::default()
        }];
        let planner = SessionNamePlanner::new(["alpha".to_string(), "alpha-beta".to_string()]);
        let log = Logger::new(&state_dir, false);
        assert!(planner.update("alpha", &alpha, &empty_local, &state_dir, &log).is_empty());
        let second = planner.update("alpha-beta", &alpha_beta, &empty_local, &state_dir, &log);
        let first = planner.update("alpha", &alpha, &empty_local, &state_dir, &log);
        assert_ne!(first["alpha-pane"], second["alpha-beta-pane"]);
        assert!(first["alpha-pane"].is_some());
        assert!(second["alpha-beta-pane"].is_some());

        let native_local: Snapshot = serde_json::from_value(json!({
            "agents": [{"pane_id": "native", "name": "greenroom-conductor"}]
        }))
        .unwrap();
        let native_collision = SessionNamePlanner::new(["greenroom".to_string()]);
        let remote = vec![AgentInfo {
            pane_id: "remote".into(),
            name: Some("conductor".into()),
            ..AgentInfo::default()
        }];
        assert_eq!(
            native_collision.update("greenroom", &remote, &native_local, &state_dir, &log)["remote"],
            None
        );

        let duplicate = vec![
            AgentInfo { pane_id: "p1".into(), name: Some("same".into()), ..AgentInfo::default() },
            AgentInfo { pane_id: "p2".into(), name: Some("same".into()), ..AgentInfo::default() },
        ];
        let residual = SessionNamePlanner::new(["host".to_string()]);
        let names = residual.update("host", &duplicate, &empty_local, &state_dir, &log);
        assert_eq!(names["p1"], None);
        assert_eq!(names["p2"], None);

        let unnamed = SessionNamePlanner::new(["cargocaddy-studio".to_string()]);
        let one = vec![AgentInfo {
            pane_id: "only-pane".into(),
            agent: Some("claude".into()),
            ..AgentInfo::default()
        }];
        assert_eq!(
            unnamed.update("cargocaddy-studio", &one, &empty_local, &state_dir, &log)
                ["only-pane"],
            Some("cargocaddy-studio".into())
        );

        let several = vec![
            AgentInfo { pane_id: "p1".into(), agent: Some("claude".into()), ..AgentInfo::default() },
            AgentInfo { pane_id: "p2".into(), agent: Some("codex".into()), ..AgentInfo::default() },
        ];
        let several_names = unnamed.update(
            "cargocaddy-studio",
            &several,
            &empty_local,
            &state_dir,
            &log,
        );
        assert_ne!(several_names["p1"], several_names["p2"]);
        assert!(several_names.values().all(|name| name
            .as_deref()
            .is_some_and(|name| name.starts_with("cargocaddy-studio-"))));
        let _ = std::fs::remove_dir_all(state_dir);
    }

    /// A remote name with nothing usable in it is no name at all — better an
    /// unnamed mirrored agent than a rename refused on every poll.
    #[test]
    fn an_unusable_remote_name_yields_nothing() {
        // nothing here survives the fold and the trim: `!!!` folds to `---`,
        // and neither it nor a bare `--` leaves a character a name may carry
        assert_eq!(mirrored_agent_name("greenroom", Some("!!!")), None);
        assert_eq!(mirrored_agent_name("greenroom", Some("--")), None);
        assert_eq!(mirrored_agent_name("greenroom", Some("_")), None);
        assert_eq!(mirrored_agent_name("greenroom", Some("")), None);
        assert_eq!(mirrored_agent_name("greenroom", Some("   ")), None);
        // a digit-led remote name keeps its digits: the host prefix supplies
        // the letter the local rule wants at the front
        assert_eq!(
            mirrored_agent_name("greenroom", Some("9lives")),
            Some("greenroom-9lives".into())
        );
    }

    #[test]
    fn rosemary_suppression_is_exact_durable_and_retires_only_for_a_new_binding() {
        let dir = std::env::temp_dir().join(format!("hm-rosemary-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut state = HostState::default();
        let first = HashMap::from([
            ("rosemary_binding".into(), " run-1 ".into()),
            ("rosemary_outcome".into(), "complete".into()),
            ("rosemary_commit".into(), "abc123".into()),
            ("rosemary_summary".into(), "done".into()),
            ("model".into(), "gpt".into()),
        ]);
        let (projected, rosemary, remembered) =
            projected_tokens(&mut state, Some("conductor"), &first);
        assert_eq!(projected["model"], "gpt");
        assert!(ROSEMARY_RUN_KEYS.iter().all(|key| projected[*key].is_null()));
        assert_eq!(rosemary["rosemary_binding"], " run-1 ");
        let remembered = remembered.unwrap();
        assert_eq!(remembered.binding, " run-1 ");
        state.rosemary_suppressions.insert("conductor".into(), remembered.clone());
        save_state(&dir, "host", &state).unwrap();

        let mut restarted = load_state(&dir, "host");
        let (suppressed, cleared, projected_run) =
            projected_tokens(&mut restarted, Some("conductor"), &first);
        assert_eq!(suppressed["model"], "gpt");
        assert!(ROSEMARY_RUN_KEYS.iter().all(|key| cleared[*key].is_null()));
        assert_eq!(projected_run, None);

        let (missing, cleared, _) =
            projected_tokens(&mut restarted, Some("conductor"), &HashMap::new());
        assert!(ROSEMARY_RUN_KEYS.iter().all(|key| missing[*key].is_null()));
        assert!(ROSEMARY_RUN_KEYS.iter().all(|key| cleared[*key].is_null()));
        assert_eq!(restarted.rosemary_suppressions["conductor"], remembered);

        let mut changed = first.clone();
        changed.insert("rosemary_binding".into(), "run-1".into());
        let (projected, rosemary, run) =
            projected_tokens(&mut restarted, Some("conductor"), &changed);
        assert_eq!(projected["model"], "gpt");
        assert!(ROSEMARY_RUN_KEYS.iter().all(|key| projected[*key].is_null()));
        assert_eq!(rosemary["rosemary_binding"], "run-1");
        assert_eq!(run.unwrap().binding, "run-1");
        assert!(!restarted.rosemary_suppressions.contains_key("conductor"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn remote_readiness_requires_remote_session_evidence() {
        let ready: AgentInfo = serde_json::from_value(json!({
            "pane_id": "p1",
            "agent": "codex",
            "agent_status": "idle",
            "interactive_ready": true,
            "agent_session": {"agent": "codex", "kind": "id", "source": "herdr:codex", "value": "session-1"}
        })).unwrap();
        assert!(ready.interactive_ready);
        assert_eq!(ready.agent_session.unwrap().value, "session-1");

        let absent: AgentInfo = serde_json::from_value(json!({
            "pane_id": "p2", "agent": "codex", "agent_status": "idle"
        })).unwrap();
        assert!(!absent.interactive_ready);
        assert!(absent.agent_session.is_none());
    }

    #[tokio::test]
    async fn refused_name_and_failed_cleanup_remain_durably_ineligible_and_are_logged() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-rosemary-cleanup-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        let local = FakePeer::start(
            "cleanup",
            json!({
                "panes": [{
                    "pane_id": "local-pane",
                    "tab_id": "local-tab",
                    "workspace_id": "local-workspace"
                }]
            }),
        )
        .await;
        local.fail("agent.rename", 2);
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let mut state = HostState::default();
        state.panes.insert(
            "remote-pane".into(),
            PaneEntry {
                local_id: "local-pane".into(),
                reported: Some("codex".into()),
                reported_name: Some("old-dispatch-name".into()),
                remote_agent_name: Some("exact remote name".into()),
                ..PaneEntry::default()
            },
        );
        save_state(&state_dir, "configured-host", &state).unwrap();
        let agent = AgentInfo {
            pane_id: "remote-pane".into(),
            agent: Some("codex".into()),
            name: Some("exact remote name".into()),
            agent_status: Some("idle".into()),
            ..AgentInfo::default()
        };
        let log = Logger::new(&state_dir, false);
        let rosemary_gate = RosemaryProjectionGate::default();
        push_pane_status(
            &PaneStatusDeps {
                local: &local_api,
                state_dir: &state_dir,
                host_name: "configured-host",
                log: &log,
                rosemary_gate: &rosemary_gate,
            },
            "remote-pane",
            &mut state,
            Some(&agent),
            Some("new-dispatch-name".into()),
        )
        .await;

        let durable = load_state(&state_dir, "configured-host");
        let entry = &durable.panes["remote-pane"];
        assert!(entry.identity_ineligible);
        assert!(!entry.identity_cleanup_pending);
        assert_eq!(entry.reported_name, None);
        let log_text = std::fs::read_to_string(state_dir.join("daemon.log")).unwrap();
        assert!(log_text.contains("[configured-host]"));
        assert!(log_text.contains("exact remote name"));
        assert!(log_text.contains("cleanup failed"));
        assert!(local.requests().iter().filter(|request| request["method"] == "agent.rename").count() >= 3);
        assert!(!local.requests().iter().any(|request| request["method"] == "pane.report_agent"));
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn already_clean_ineligible_agent_does_not_repeat_cleanup() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-rosemary-already-clean-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        let local = FakePeer::start("already-clean", json!({})).await;
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let mut state = HostState::default();
        state.panes.insert(
            "remote-pane".into(),
            PaneEntry {
                local_id: "local-pane".into(),
                identity_ineligible: true,
                identity_cleanup_pending: false,
                ..PaneEntry::default()
            },
        );
        let agent = AgentInfo {
            pane_id: "remote-pane".into(),
            agent: Some("codex".into()),
            name: Some("colliding name".into()),
            agent_status: Some("idle".into()),
            ..AgentInfo::default()
        };
        push_pane_status(
            &PaneStatusDeps {
                local: &local_api,
                state_dir: &state_dir,
                host_name: "configured-host",
                log: &Logger::new(&state_dir, false),
                rosemary_gate: &RosemaryProjectionGate::default(),
            },
            "remote-pane",
            &mut state,
            Some(&agent),
            None,
        )
        .await;

        assert!(!local.requests().iter().any(|request| {
            matches!(
                request["method"].as_str(),
                Some("agent.rename" | "pane.clear_agent_authority")
            )
        }));
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn unnamed_remote_agent_is_reported_before_workspace_default_name() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-unnamed-agent-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        let local = FakePeer::start(
            "unnamed-agent",
            json!({
                "panes": [{
                    "pane_id": "local-pane",
                    "tab_id": "local-tab",
                    "workspace_id": "local-workspace"
                }]
            }),
        )
        .await;
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let mut state = HostState::default();
        state.panes.insert(
            "remote-pane".into(),
            PaneEntry {
                local_id: "local-pane".into(),
                ..PaneEntry::default()
            },
        );
        let agent = AgentInfo {
            pane_id: "remote-pane".into(),
            agent: Some("claude".into()),
            name: None,
            agent_status: Some("idle".into()),
            interactive_ready: true,
            ..AgentInfo::default()
        };

        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            push_pane_status(
                &PaneStatusDeps {
                    local: &local_api,
                    state_dir: &state_dir,
                    host_name: "configured-host",
                    log: &Logger::new(&state_dir, false),
                    rosemary_gate: &RosemaryProjectionGate::default(),
                },
                "remote-pane",
                &mut state,
                Some(&agent),
                Some("configured-host".into()),
            ),
        )
        .await
        .expect("an unnamed remote agent entered identity cleanup");

        assert!(local
            .requests()
            .iter()
            .any(|request| request["method"] == "pane.report_agent"));
        let requests = local.requests();
        let report_index = requests
            .iter()
            .position(|request| request["method"] == "pane.report_agent")
            .expect("fresh mirror never reported its agent identity");
        let rename_index = requests
            .iter()
            .position(|request| request["method"] == "agent.rename")
            .expect("unnamed mirror never received its workspace-derived name");
        assert!(report_index < rename_index);
        assert_eq!(requests[rename_index]["params"]["name"], "configured-host");
        assert!(!requests.iter().any(|request| {
            request["method"] == "pane.clear_agent_authority"
        }));
        assert!(!state.panes["remote-pane"].identity_ineligible);
        assert_eq!(state.panes["remote-pane"].reported_name.as_deref(), Some("configured-host"));
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn cleanup_of_a_vanished_local_pane_finishes_instead_of_retrying_forever() {
        let state_dir = std::env::temp_dir().join(format!(
            "hm-rosemary-vanished-pane-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        let local = FakePeer::start("vanished-pane", json!({})).await;
        local.fail("agent.rename", 10);
        local.fail("pane.clear_agent_authority", 10);
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let mut state = HostState::default();
        state.panes.insert(
            "remote-pane".into(),
            PaneEntry {
                local_id: "local-pane".into(),
                reported: Some("codex".into()),
                reported_name: Some("old-name".into()),
                remote_agent_name: Some("remote-name".into()),
                ..PaneEntry::default()
            },
        );
        let agent = AgentInfo {
            pane_id: "remote-pane".into(),
            agent: Some("codex".into()),
            name: Some("remote-name".into()),
            agent_status: Some("idle".into()),
            ..AgentInfo::default()
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            push_pane_status(
                &PaneStatusDeps {
                    local: &local_api,
                    state_dir: &state_dir,
                    host_name: "configured-host",
                    log: &Logger::new(&state_dir, false),
                    rosemary_gate: &RosemaryProjectionGate::default(),
                },
                "remote-pane",
                &mut state,
                Some(&agent),
                None,
            ),
        )
        .await
        .expect("cleanup loop did not stop after the local pane vanished");

        let entry = &state.panes["remote-pane"];
        assert!(entry.identity_ineligible);
        assert!(!entry.identity_cleanup_pending);
        assert_eq!(entry.reported_name, None);
        assert_eq!(
            local
                .requests()
                .iter()
                .filter(|request| request["method"] == "agent.rename")
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(state_dir);
    }

    #[tokio::test]
    async fn local_clear_is_not_acknowledged_when_the_suppression_cannot_be_saved() {
        use std::os::unix::fs::PermissionsExt;

        let state_dir = std::env::temp_dir().join(format!(
            "hm-rosemary-save-failure-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&state_dir);
        std::fs::create_dir_all(&state_dir).unwrap();
        let local = FakePeer::start("save-failure", local_rosemary_snapshot("lp", false)).await;
        let local_api = ApiClient::connect(&local.path).await.unwrap();
        let mut state = HostState::default();
        state.panes.insert(
            "rp".into(),
            PaneEntry {
                local_id: "lp".into(),
                remote_agent_name: Some("conductor-rosie".into()),
                projected_rosemary_run: Some(RosemaryRun {
                    binding: "run-1".into(),
                    outcome: Some("complete".into()),
                    commit: Some("abc123".into()),
                    summary: Some("done".into()),
                }),
                ..PaneEntry::default()
            },
        );
        save_state(&state_dir, "vps", &state).unwrap();
        let path = crate::state::state_path(&state_dir, "vps");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
        let result = capture_local_rosemary_clear(
            &local_api,
            &state_dir,
            "vps",
            "lp",
            &Logger::new(&state_dir, false),
        )
        .await;
        assert!(result.is_err(), "a failed durable write must not acknowledge the clear");
        assert!(load_state(&state_dir, "vps").rosemary_suppressions.is_empty());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let _ = std::fs::remove_dir_all(state_dir);
    }

    fn leaf(pane_id: &str) -> LayoutNode {
        LayoutNode::Pane { pane_id: Some(pane_id.into()), label: None }
    }

    fn split(direction: &str, ratio: f64, first: LayoutNode, second: LayoutNode) -> LayoutNode {
        LayoutNode::Split { direction: direction.into(), ratio, first: Box::new(first), second: Box::new(second) }
    }

    /// The fallback path, used only for a pane `layout_sync::plan_placements`
    /// won't place (its remote sibling is a whole subtree, which `pane.split`
    /// can't wrap). Ratio deliberately not reported: the shape-preserving
    /// placements carry it, and here it would describe a different split.
    #[test]
    fn locate_in_layout_reports_direction_and_siblings() {
        let tree = split("right", 0.3, leaf("p1"), leaf("p2"));
        let (dir, sibs) = locate_in_layout(&tree, "p2").unwrap();
        assert_eq!(dir, "right");
        assert_eq!(sibs, vec!["p1".to_string()]);

        // nested: p3 is under a "down" split inside the "right" split's second
        // branch, so the nearest sibling is p2, not p1
        let tree = split("right", 0.3, leaf("p1"), split("down", 0.4, leaf("p2"), leaf("p3")));
        let (dir, sibs) = locate_in_layout(&tree, "p3").unwrap();
        assert_eq!(dir, "down");
        assert_eq!(sibs, vec!["p2".to_string()]);

        assert!(locate_in_layout(&tree, "nope").is_none());
    }

    fn tombstoned(local_id: &str) -> PaneEntry {
        PaneEntry {
            local_id: local_id.into(),
            tombstone: Some(true),
            seq: 0,
            reported: None,
            reported_name: None,
            remote_agent_name: None,
            projected_rosemary_run: None,
            identity_ineligible: false,
            identity_cleanup_pending: false,
        }
    }

    /// A locally-closed (tombstoned) pane must not survive into the tree a tab
    /// rebuild applies: layout.apply creates a real local pane per leaf, and a
    /// tombstoned one would be left as a dead shell no streamer ever claims.
    #[test]
    fn prune_closed_drops_tombstoned_panes_and_collapses_splits() {
        let tree = split("right", 0.3, leaf("p1"), split("down", 0.4, leaf("p2"), leaf("p3")));

        // untracked and live panes survive untouched
        let mut panes: BTreeMap<String, PaneEntry> = BTreeMap::new();
        panes.insert(
            "p1".into(),
            PaneEntry {
                local_id: "l1".into(),
                tombstone: None,
                seq: 0,
                reported: None,
                reported_name: None,
                remote_agent_name: None,
                projected_rosemary_run: None,
                identity_ineligible: false,
                identity_cleanup_pending: false,
            },
        );
        let mut ids = Vec::new();
        walk_pane_ids(&prune_closed(&tree, &panes).unwrap(), &mut ids);
        assert_eq!(ids, vec!["p1".to_string(), "p2".to_string(), "p3".to_string()]);

        // a tombstoned leaf disappears and its split collapses to the sibling
        panes.insert("p2".into(), tombstoned("l2"));
        let pruned = prune_closed(&tree, &panes).unwrap();
        let mut ids = Vec::new();
        walk_pane_ids(&pruned, &mut ids);
        assert_eq!(ids, vec!["p1".to_string(), "p3".to_string()]);
        // the surviving outer split keeps its geometry
        let LayoutNode::Split { direction, ratio, .. } = &pruned else {
            panic!("outer split should survive");
        };
        assert_eq!(direction, "right");
        assert_eq!(*ratio, 0.3);

        // every pane tombstoned → None: the whole tab's mirror was closed
        panes.insert("p1".into(), tombstoned("l1"));
        panes.insert("p3".into(), tombstoned("l3"));
        assert!(prune_closed(&tree, &panes).is_none());
    }

    /// Characterization test: the ssh pane argv is a cross-process contract.
    ///
    /// The daemon spawns `herdr-mirror pane ...` as a separate process, and
    /// `count_streamers` (daemon.rs) identifies a host's live streamers by
    /// string-matching `--ctl-path` in that argv. Nothing else pins the shape,
    /// so a change here silently breaks mirror healing on upgrade: streamers
    /// started by the old binary carry the old argv, the new daemon fails to
    /// match them, concludes they died, and re-execs over live panes.
    ///
    /// If this test fails, that is the question to answer — not a prompt to
    /// update the expected value.
    #[test]
    fn ssh_pane_argv_is_stable() {
        let state_dir = std::path::Path::new("/state");
        let cmd = cmd_for_pane(&ssh_host(), state_dir, &HashMap::new());
        let argv = cmd("w1:p1");
        assert_eq!(
            argv[1..],
            [
                "pane",
                "vps",
                "w1:p1",
                // no --remote-bin: auto (PATH then ~/.local/bin/herdr)
                "--always-control",
                "--ctl-path",
                "/state/vps.ctl",
            ]
        );
        // argv[0] is the resolved exe path, which varies by install
        assert!(argv[0].ends_with("herdr-mirror") || argv[0].contains("herdr_mirror"), "{}", argv[0]);
    }

    /// When remote_bin is set, it must appear on the argv (cross-process contract
    /// with the pane parser) rather than being re-resolved by the streamer.
    #[test]
    fn ssh_pane_argv_carries_explicit_remote_bin() {
        let mut host = ssh_host();
        host.remote_bin = Some("/opt/herdr".into());
        let cmd = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new());
        let argv = cmd("w1:p1");
        assert_eq!(
            argv[1..],
            [
                "pane",
                "vps",
                "w1:p1",
                "--remote-bin",
                "/opt/herdr",
                "--always-control",
                "--ctl-path",
                "/state/vps.ctl",
            ]
        );
    }

    #[test]
    fn ssh_pane_argv_carries_remote_session() {
        let mut host = ssh_host();
        host.session = Some("work".into());
        let cmd = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new());
        let argv = cmd("w1:p1");
        assert_eq!(
            argv[1..],
            [
                "pane",
                "vps",
                "w1:p1",
                "--session",
                "work",
                "--always-control",
                "--ctl-path",
                "/state/vps.ctl",
            ]
        );
        let parsed = crate::pane::parse_args(&argv[2..]).expect("pane must parse daemon argv");
        assert_eq!(parsed.session.as_deref(), Some("work"));
    }

    /// Docker hosts append their flags *after* the ssh-shaped prefix, so the
    /// two argv layouts share a stable head and only diverge at the tail.
    #[test]
    fn docker_pane_argv_carries_container_and_no_identity_token() {
        let mut host = ssh_host();
        host.name = "token".into();
        host.target = "/Users/n/proj".into();
        host.kind = crate::config::HostKind::DockerFolder("/Users/n/proj".into());
        host.docker_bin = "/usr/local/bin/docker".into();
        let cmd = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new());
        let argv = cmd("w1:p1");
        assert_eq!(
            argv[1..],
            [
                "pane",
                "/Users/n/proj",
                "w1:p1",
                "--always-control",
                // no identity token at all: healing asks herdr per pane
                "--container-folder",
                "/Users/n/proj",
                "--docker-bin",
                "/usr/local/bin/docker",
            ]
        );
    }

    /// The argv the daemon emits must round-trip through the pane process's
    /// own parser — they are separate processes, so nothing else checks this.
    #[test]
    fn docker_argv_round_trips_through_pane_parser() {
        let mut host = ssh_host();
        host.kind = crate::config::HostKind::DockerContainer("crazy_ride".into());
        // deliberately NOT the default "docker": parse_args defaults to the
        // same value, so a fixture using the default would still pass if
        // cmd_for_pane stopped emitting --docker-bin. Users who need an
        // absolute path (GUI-launched daemons without /usr/local/bin on PATH)
        // would then silently get "cannot run docker".
        host.docker_bin = "/usr/local/bin/docker".into();
        let cmd = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new());
        let argv = cmd("w1:p1");
        let parsed = crate::pane::parse_args(&argv[2..]).expect("pane must parse daemon argv");
        assert_eq!(parsed.pane_target, "w1:p1");
        assert_eq!(parsed.ctl_path, None, "docker panes carry no ctl path");
        let ct = parsed.container.expect("container must survive the argv round trip");
        assert_eq!(ct.kind, crate::config::HostKind::DockerContainer("crazy_ride".into()));
        assert_eq!(ct.docker_bin, "/usr/local/bin/docker", "--docker-bin must round-trip");
    }

    /// always_control is the only conditional flag; its absence must not
    /// disturb the position of --ctl-path.
    #[test]
    fn ssh_pane_argv_without_always_control() {
        let mut host = ssh_host();
        host.always_control = false;
        let cmd = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new());
        let argv = cmd("w1:p1");
        assert_eq!(
            argv[1..],
            ["pane", "vps", "w1:p1", "--ctl-path", "/state/vps.ctl"]
        );
    }

    /// An uncapped host's argv must not grow, and a capped one must round-trip
    /// through the same parser the daemon's child uses.
    #[test]
    fn size_caps_reach_the_streamer_argv() {
        let mut host = ssh_host();
        host.always_control = false;
        let uncapped = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new())("w1:p1");
        assert!(!uncapped.iter().any(|a| a == "--max-cols" || a == "--max-rows"));

        host.max_cols = Some(212);
        host.max_rows = Some(58);
        let argv = cmd_for_pane(&host, std::path::Path::new("/state"), &HashMap::new())("w1:p1");
        let parsed = crate::pane::parse_args(&argv[2..]).expect("pane must parse daemon argv");
        assert_eq!(parsed.max_cols, Some(212));
        assert_eq!(parsed.max_rows, Some(58));
        // the caps are a ceiling on control only — the observe request is
        // still whatever --cols/--rows said (here: unset, so the defaults,
        // which #42 made a floor rather than an exact size)
        assert_eq!((parsed.cols, parsed.rows), (240, 72));
    }

    #[test]
    fn ws_label_two_way_rename() {
        // in sync → nothing
        assert_eq!(resolve_label(Some("pm"), "scratch", "pm: scratch", Some("scratch")), LabelAction::InSync);
        // remote renamed (history differs) → remote wins
        assert_eq!(resolve_label(Some("pm"), "runs", "pm: scratch", Some("scratch")), LabelAction::RestampLocal);
        // no history (pre-upgrade state file) → remote wins once
        assert_eq!(resolve_label(Some("pm"), "scratch", "pm: LLMs", None), LabelAction::RestampLocal);
        // user renamed locally, kept the prefix → push stripped name to remote
        assert_eq!(
            resolve_label(Some("pm"), "scratch", "pm: LLMs", Some("scratch")),
            LabelAction::PushRemote("LLMs".into())
        );
        // user renamed locally without prefix → push as-is
        assert_eq!(
            resolve_label(Some("pm"), "scratch", "LLM runs", Some("scratch")),
            LabelAction::PushRemote("LLM runs".into())
        );
        // degenerate: renamed to just the prefix-colon or whitespace → restamp
        assert_eq!(resolve_label(Some("pm"), "scratch", "pm:  ", Some("scratch")), LabelAction::RestampLocal);
    }

    /// Tabs carry the remote label verbatim, so the same resolution runs with no
    /// prefix. The third case is the bug this exists to prevent: a local rename
    /// used to be invisible, so converge restamped it back from the remote.
    #[test]
    fn tab_label_two_way_rename() {
        // in sync → nothing
        assert_eq!(resolve_label(None, "logs", "logs", Some("logs")), LabelAction::InSync);
        // remote renamed since we last stamped → remote wins
        assert_eq!(resolve_label(None, "build", "logs", Some("logs")), LabelAction::RestampLocal);
        // user renamed the mirror tab, remote unchanged → push it to the remote
        assert_eq!(
            resolve_label(None, "logs", "deploys", Some("logs")),
            LabelAction::PushRemote("deploys".into())
        );
        // no history (tab mapped by an older mirror) → remote wins once
        assert_eq!(resolve_label(None, "logs", "deploys", None), LabelAction::RestampLocal);
        // renamed to whitespace → restamp rather than push an empty label
        assert_eq!(resolve_label(None, "logs", "   ", Some("logs")), LabelAction::RestampLocal);
        // a never-named remote tab reports its position as its label, so a local
        // rename of one still has to push rather than restamp
        assert_eq!(
            resolve_label(None, "2", "notes", Some("2")),
            LabelAction::PushRemote("notes".into())
        );
    }

    /// The `pane_agent_status_changed` event (herdr app/api.rs) must deserialize
    /// into AgentInfo cleanly, or flush_status would fall back to a default (no
    /// agent) and wrongly retract the mirror's agent. Note the event carries the
    /// title as `title`, which lands in its own field and still reaches the
    /// reported title slot through `effective_title`.
    #[test]
    fn agent_status_event_parses_and_keeps_title() {
        let data = json!({
            "pane_id": "w1:p1",
            "workspace_id": "w1",
            "agent_status": "working",
            "agent": "claude",
            "title": "fix the bug",
            "display_agent": "Claude",
            "custom_status": null,
            "state_labels": { "branch": "main" }
        });
        let info: AgentInfo = serde_json::from_value(data).unwrap();
        assert_eq!(info.agent.as_deref(), Some("claude"));
        assert_eq!(info.agent_status.as_deref(), Some("working"));
        assert_eq!(info.display_agent.as_deref(), Some("Claude"));
        assert_eq!(info.title.as_deref(), Some("fix the bug"));
        assert_eq!(info.effective_title(), Some("fix the bug"));
        assert!(info.has_agent());
    }

    /// A named agent that also carries a pane title must parse: with `title`
    /// aliased onto `name` it was a duplicate-field error, which fails the
    /// whole snapshot parse (`agents` is a Vec) and wedges the host.
    #[test]
    fn agent_with_both_name_and_title_parses() {
        let agents: Vec<AgentInfo> = serde_json::from_value(json!([
            { "pane_id": "w1:p1", "agent": "claude", "name": "l2-r3", "title": "fix the bug" },
            { "pane_id": "w1:p2", "agent": "codex" },
        ]))
        .expect("a named agent with a pane title must not fail the snapshot parse");
        assert_eq!(agents.len(), 2);
        // an explicit name still wins over the pane title
        assert_eq!(agents[0].effective_title(), Some("l2-r3"));
        assert_eq!(agents[0].title.as_deref(), Some("fix the bug"));
        assert_eq!(agents[1].effective_title(), None);
    }

    /// A remote agent with a user-given name keeps showing it; only an
    /// unnamed agent falls back to the remote's live terminal title, so a
    /// mirrored agent's current task is visible instead of always blank
    /// (the reported gap: oldmac reports `terminal_title_stripped` on every
    /// agent, but the mirror only ever forwarded `name`, which most agents
    /// never set).
    #[test]
    fn effective_title_prefers_name_falls_back_to_terminal_title() {
        let named = AgentInfo {
            name: Some("l2-r3".into()),
            terminal_title_stripped: Some("实现论文引用图数据层".into()),
            ..Default::default()
        };
        assert_eq!(named.effective_title(), Some("l2-r3"));

        let unnamed = AgentInfo {
            name: None,
            terminal_title_stripped: Some("实现论文引用图数据层".into()),
            terminal_title: Some("✳ 实现论文引用图数据层".into()),
            ..Default::default()
        };
        assert_eq!(unnamed.effective_title(), Some("实现论文引用图数据层"));

        let stripped_missing = AgentInfo {
            name: None,
            terminal_title_stripped: None,
            terminal_title: Some("✳ working".into()),
            ..Default::default()
        };
        assert_eq!(stripped_missing.effective_title(), Some("✳ working"));

        let bare = AgentInfo { name: None, ..Default::default() };
        assert_eq!(bare.effective_title(), None);
    }

    // simulate herdr's move_workspace(source, insert_index) on an id list
    fn apply_move(order: &mut Vec<String>, ws: &str, insert_index: usize) {
        let src = order.iter().position(|w| w == ws).unwrap();
        let target_idx = if src < insert_index { insert_index - 1 } else { insert_index };
        let item = order.remove(src);
        order.insert(target_idx, item);
    }

    fn ranked(items: &[(&str, usize)]) -> Vec<(String, usize)> {
        items.iter().map(|(s, r)| (s.to_string(), *r)).collect()
    }

    #[test]
    fn regroup_groups_and_only_moves_mirrors() {
        // rank 0 = local, 1 = work, 2 = vps; interleaved current order
        let current = ranked(&[("L1", 0), ("W1", 1), ("V1", 2), ("L2", 0), ("W2", 1)]);
        let moves = plan_regroup(&current);
        // never move a local
        let rank_of = |id: &str| current.iter().find(|(i, _)| i == id).unwrap().1;
        for (id, _) in &moves {
            assert!(rank_of(id) > 0, "planner moved a local row: {id}");
        }
        // applying the plan yields the grouped order
        let mut order: Vec<String> = current.iter().map(|(id, _)| id.clone()).collect();
        for (ws, idx) in &moves {
            apply_move(&mut order, ws, *idx);
        }
        assert_eq!(order, vec!["L1", "L2", "W1", "W2", "V1"]);
    }

    #[test]
    fn regroup_is_noop_when_already_grouped() {
        let current = ranked(&[("L1", 0), ("L2", 0), ("W1", 1), ("W2", 1), ("V1", 2)]);
        assert!(plan_regroup(&current).is_empty());
    }

    #[test]
    fn regroup_new_mirror_slots_into_its_block() {
        // a new work workspace appended at the bottom (the reported bug)
        let current = ranked(&[("L1", 0), ("W1", 1), ("V1", 2), ("W2", 1)]);
        let mut order: Vec<String> = current.iter().map(|(id, _)| id.clone()).collect();
        for (ws, idx) in plan_regroup(&current) {
            apply_move(&mut order, &ws, idx);
        }
        assert_eq!(order, vec!["L1", "W1", "W2", "V1"]); // W2 rises above V1
    }

    #[test]
    fn ws_rank_classifies_by_prefix() {
        let prefixes = vec!["work".to_string(), "vps".to_string()];
        assert_eq!(ws_rank("work: slice", &prefixes), 1);
        assert_eq!(ws_rank("vps: ~", &prefixes), 2);
        assert_eq!(ws_rank("utopia", &prefixes), 0); // local
    }

    /// An agent-exit event carries no agent + "unknown" status → has_agent()
    /// false, so push_pane_status retracts (the intended release path).
    #[test]
    fn agent_exit_event_reads_as_no_agent() {
        let data = json!({
            "pane_id": "w1:p1",
            "workspace_id": "w1",
            "agent_status": "unknown",
            "agent": null,
            "display_agent": null,
            "custom_status": null,
            "state_labels": null
        });
        let info: AgentInfo = serde_json::from_value(data).unwrap();
        assert!(!info.has_agent());
    }
    fn argv(parts: &[&str]) -> Vec<Value> {
        parts.iter().map(|s| json!(s)).collect()
    }

    /// Real argv, captured from `pane.process_info` on a live ssh mirror pane.
    #[test]
    fn recognises_a_live_streamer() {
        let streamer = argv(&[
            "/Users/niko/Documents/coding/herdr-mirror/target/release/herdr-mirror",
            "pane",
            "vps",
            "wC:p1",
            "--remote-bin",
            "~/.local/bin/herdr",
        ]);
        assert!(is_streamer_argv(&streamer));

        // the ssh child sharing the same pane is not itself a streamer
        let ssh_child = argv(&["ssh", "-o", "BatchMode=yes", "vps", "exec ~/.local/bin/herdr ..."]);
        assert!(!is_streamer_argv(&ssh_child));
    }

    /// A docker pane's wrapper looks the same to this check — the whole point
    /// of asking herdr per pane instead of matching transport-specific flags.
    #[test]
    fn transport_and_flags_are_irrelevant() {
        assert!(is_streamer_argv(&argv(&[
            "/plugins/github/mirror-0015/target/release/herdr-mirror",
            "pane",
            "/Users/n/proj",
            "w1:p1",
            "--container-folder",
            "/Users/n/proj",
        ])));
        // and a pre-v0.1.7 streamer, which carried no identity flag at all
        assert!(is_streamer_argv(&argv(&["/usr/local/bin/herdr-mirror", "pane", "vps", "w1:p1"])));
    }

    /// A shell left behind by session-restore is what healing must act on.
    #[test]
    fn plain_shell_is_not_a_streamer() {
        assert!(!is_streamer_argv(&argv(&["-zsh"])));
        assert!(!is_streamer_argv(&argv(&["/bin/bash"])));
        assert!(!is_streamer_argv(&argv(&[])));
    }

    /// Another subcommand in the pane must not read as a live stream.
    #[test]
    fn other_subcommands_are_not_streamers() {
        assert!(!is_streamer_argv(&argv(&["/usr/local/bin/herdr-mirror", "status"])));
        assert!(!is_streamer_argv(&argv(&["/usr/local/bin/herdr-mirror"])));
    }

    /// The live failure reported no foreground streamer while its pidfile
    /// already named the active wrapper. Recovery must trust either signal.
    #[test]
    fn a_live_pidfile_blocks_false_recovery() {
        assert!(!streamer_exec_needed(Some(false), true));
        assert!(!streamer_exec_needed(Some(true), false));
        assert!(!streamer_exec_needed(None, false));
        assert!(streamer_exec_needed(Some(false), false));
    }

    /// The startup retype asks the same question the zombie heal does, so the
    /// only line that may be typed into a pane is one herdr says is a shell.
    /// Live 2026-08-29: a Daytona mirror pane whose owner shell was still in
    /// startup got a second copy of the exec line, the streamer forwarded it to
    /// the remote shell, `exec herdr-mirror` failed with 127, and the remote
    /// workspace's only pane died with it.
    #[test]
    fn retype_needs_both_a_dead_pidfile_and_a_shell() {
        // a live pidfile is proof enough on its own — never type
        assert!(!streamer_exec_needed(Some(false), true));
        assert!(!streamer_exec_needed(Some(true), true));
        // herdr sees our wrapper running: the pid is simply not published yet
        assert!(!streamer_exec_needed(Some(true), false));
        // herdr could not answer: unknown is not permission
        assert!(!streamer_exec_needed(None, false));
        assert!(!streamer_exec_needed(None, true));
        // the only safe case: no pid of ours, and herdr sees a plain shell
        assert!(streamer_exec_needed(Some(false), false));
    }
}
