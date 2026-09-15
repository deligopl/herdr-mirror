// Foreground-process detection for the mirror streamer.
//
// herdr strips the mouse-mode DECSET from the frames the plugin observes, so the
// streamer can't tell whether the remote pane's app wants the mouse. As a proxy,
// query the remote pane's foreground process (`herdr pane process-info`) and
// classify it: a plain shell at a prompt never enables mouse reporting, so mouse
// events should stay local (no garbage in the prompt); anything else is treated
// as a possible mouse-aware TUI and clicks are forwarded. This is a heuristic
// stand-in until herdr exposes the pane's mouse-reporting state through the API.

use std::path::Path;

use crate::pane::sh_quote;

/// Interactive shells: at a prompt these don't enable mouse reporting, so mouse
/// events over them should stay local rather than being forwarded to the pty.
const SHELLS: &[&str] = &[
    "bash", "zsh", "fish", "sh", "dash", "ksh", "ksh93", "mksh", "ash", "tcsh",
    "csh", "nu", "elvish", "xonsh", "osh", "ysh", "oil", "ion", "murex", "ngs",
    "pwsh", "powershell", "cmd",
];

/// Is `name` one of the known interactive shells? Normalizes a login-shell dash
/// (`-bash`), a leading path, and a Windows `.exe` suffix before matching.
pub fn is_shell(name: &str) -> bool {
    let base = name.trim_start_matches('-').rsplit(['/', '\\']).next().unwrap_or(name);
    let n = base.trim_end_matches(".exe").to_ascii_lowercase();
    SHELLS.contains(&n.as_str())
}

/// What the remote pane's foreground implies for local input handling.
///
/// Three states because two different questions hide in "is it a TUI?": which
/// cursor-key encoding to use, and who should get the mouse. An agent CLI is not
/// a shell (it sets DECCKM, so arrows must be application mode) and still does
/// not read mouse reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fg {
    /// interactive shell at a prompt: sets no mouse modes. The local grab
    /// stays held so the wheel can scroll; left-button drags use the plugin
    /// selector; raw reports are not forwarded (they'd garbage the prompt).
    Shell,
    /// an agent CLI. herdr identified it, so this is not a guess.
    Agent,
    /// anything else: assume it wants the mouse, which is the safe default
    /// because being wrong only costs a selection, never an app's clicks
    Mouse,
}

/// Classify from the remote pane's `agent` field and its foreground job.
///
/// The agent question is answered by HERDR, not by us: `PaneInfo.agent` comes
/// from its `identify_agent_in_job`, which scans the whole foreground job across
/// its own canonical agent table and resolves CLIs shipped behind `node`, `bun`
/// or `python` wrappers using argv0/argv/cmdline. A hardcoded list here would be
/// a second, worse copy of data herdr already maintains and already serves over
/// the API we are calling anyway — and it would drift the day a new agent ships.
///
/// It also fixes the leaf problem for free: `process-info` returns the whole
/// foreground process GROUP, so an agent's leaf is whatever tool it just spawned
/// (`node`, `rg`, `bash`) and moves every few seconds. `agent` does not move.
pub fn classify(pane_json: &str, proc_json: &str) -> Option<Fg> {
    if agent_pane(pane_json)? {
        return Some(Fg::Agent);
    }
    let v: serde_json::Value = serde_json::from_str(proc_json).ok()?;
    let fg = v.get("result")?.get("process_info")?.get("foreground_processes")?.as_array()?;
    // the last foreground process is the actually-running leaf, so `sudo vim`
    // classifies on `vim`, not `sudo`
    let name = fg.last()?.get("name")?.as_str()?;
    Some(if is_shell(name) { Fg::Shell } else { Fg::Mouse })
}

/// Has herdr identified an agent CLI in this pane? Read out of the `pane get`
/// answer on its own, which is what lets a transport that pays per call decide
/// whether the second call is worth making. `None` when the answer does not
/// parse or does not describe a pane — never `Some(false)`, because "I could
/// not tell" and "no agent" lead to different next steps.
fn agent_pane(pane_json: &str) -> Option<bool> {
    let pane: serde_json::Value = serde_json::from_str(pane_json).ok()?;
    let pane = pane.get("result")?.get("pane")?;
    Some(pane.get("agent").and_then(|v| v.as_str()).is_some())
}

/// The remote pane's own content revision, from the same `pane get` answer the
/// foreground classification is read out of.
///
/// herdr bumps it whenever the pane's screen changes, so it is the cheapest
/// available answer to "did the remote produce output?" — the question that
/// separates a mirror with nothing to show from a mirror that is no longer
/// being shown anything. Free here: the poll already makes this call.
pub fn revision(pane_json: &str) -> Option<u64> {
    let pane: serde_json::Value = serde_json::from_str(pane_json).ok()?;
    pane.get("result")?.get("pane")?.get("revision")?.as_u64()
}

/// One in-flight metadata poll per pane, at most.
///
/// The deadline alone does not bound how many polls a pane can have in the air.
/// Spawning is throttled to `FG_POLL_INTERVAL`, but a forced poll — the one an
/// input burst asks for, so the classification is right the instant a TUI exits
/// — bypasses that throttle entirely. Under input, polls can therefore be
/// started far faster than a slow remote retires them.
///
/// So the gate, not arithmetic, is what bounds it. A caller that finds a poll
/// already running does not start a second one and does not queue: it records
/// that a refresh is wanted, and the running poll does one more pass when it
/// finishes. Only the newest answer is ever worth having — each one simply
/// overwrites the pane's last known value — so coalescing loses nothing, and a
/// burst of a hundred forced triggers costs one extra pass, not a hundred.
#[derive(Clone, Default)]
pub struct PollGate {
    state: std::sync::Arc<std::sync::Mutex<GateState>>,
}

#[derive(Default)]
struct GateState {
    running: bool,
    refresh_wanted: bool,
}

/// Held for as long as a poll owns the gate. Releasing it on drop is what makes
/// the guard safe against cancellation as well as completion: if the pane goes
/// away mid-poll, the gate does not stay shut on a poll that will never finish.
pub struct PollPermit {
    gate: PollGate,
}

impl PollGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// `Some` when the caller should run the poll, `None` when one is already
    /// in flight — in which case a refresh is remembered, without a queue.
    pub fn begin(&self) -> Option<PollPermit> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.running {
            state.refresh_wanted = true;
            return None;
        }
        state.running = true;
        state.refresh_wanted = false;
        drop(state);
        Some(PollPermit { gate: self.clone() })
    }
}

impl PollPermit {
    /// After a pass: whether someone asked for a refresh while it ran. Taking
    /// it and deciding to continue happen under the one lock, so a request
    /// arriving at that moment cannot be dropped on the floor.
    pub fn another_pass_wanted(&self) -> bool {
        let mut state = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut state.refresh_wanted)
    }
}

impl Drop for PollPermit {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
        state.running = false;
        state.refresh_wanted = false;
    }
}

/// One pane's poller, shared between the pane loop and the tasks it spawns.
///
/// A mutex rather than a channel because the serialization IS the feature: one
/// shell, one script at a time, and a poll that arrives while another is in
/// flight is dropped rather than queued — it would ask the same question and
/// get the same answer a moment later.
pub type Shared = std::sync::Arc<tokio::sync::Mutex<crate::poll_channel::Poller>>;

pub fn shared(transport: crate::poll_channel::Transport) -> Shared {
    std::sync::Arc::new(tokio::sync::Mutex::new(crate::poll_channel::Poller::new(transport)))
}

/// Query the remote pane down the pane's existing channel: its foreground
/// classification, and its content revision. `None` on any failure
/// (transport/parse) so the caller keeps its last known value.
///
/// Both answers still come back in ONE round trip, and since 2026-09-15 that
/// round trip no longer costs a session: the script is written into the shell
/// this pane already has open. On a guest behind the loopback bridge that is
/// the difference between one `exec` per poll and one per streamer.
///
/// A host reached over the daemon's API forward (`api_socket`) takes neither:
/// see `api_poll`.
/// Pane metadata over the daemon's API forward, for a host that has one.
///
/// Why this is not the reused channel below. The channel works because a shell
/// can be held open and written to again; the Herdr API cannot be used that
/// way. Its server answers ONE request per connection and closes (see
/// `api.rs`), so "one long-lived connection per pane" is not a thing this
/// protocol offers — the only held connection it has is `events.subscribe`,
/// which pushes events rather than answering questions. Every request is
/// therefore its own connection, and over the `-L` forward of a sandbox guest
/// every connection is its own direct-tcpip channel and its own guest `exec`.
///
/// What is left is to ask less often and to ask for less. The cadence is the
/// caller's job (`pane::fg_poll_interval`). Asking for less is this function's:
/// `pane.get` alone settles the classification whenever herdr has identified an
/// agent in the pane — which on these hosts is the ordinary case, since they
/// exist to mirror agent panes — so the second call is made only when the first
/// one left the question open. An idle agent pane costs one connection per
/// poll, not two.
async fn api_poll(socket: &str, pane: &str) -> (Option<Fg>, Option<u64>) {
    let api = crate::api::ApiClient::at(Path::new(socket));
    let Ok(pane_value) = api
        .request("pane.get", serde_json::json!({ "pane_id": pane }))
        .await
    else {
        return (None, None);
    };
    let pane_json = serde_json::json!({ "result": pane_value }).to_string();
    let revision = revision(&pane_json);
    match agent_pane(&pane_json) {
        Some(true) => return (Some(Fg::Agent), revision),
        // unparseable: a second call cannot rescue a first answer we could not
        // read, and the caller keeps its last classification either way
        None => return (None, revision),
        Some(false) => {}
    }
    let Ok(process_value) = api
        .request("pane.process_info", serde_json::json!({ "pane_id": pane }))
        .await
    else {
        return (None, revision);
    };
    let process_json = serde_json::json!({ "result": process_value }).to_string();
    (classify(&pane_json, &process_json), revision)
}

pub async fn poll(
    poller: &Shared,
    remote_bin: Option<&str>,
    session: Option<&str>,
    pane: &str,
    api_socket: Option<&str>,
) -> (Option<Fg>, Option<u64>) {
    // A host with a selected API forward answers there and nowhere else: the
    // forward already reaches this exact Herdr server, and going over an ssh
    // `session` channel instead would invoke the sandbox's lifecycle-gated
    // exec path for metadata it is already serving.
    if let Some(socket) = api_socket {
        return api_poll(socket, pane).await;
    }
    // same expression as the observe session (configured path or PATH auto)
    let bin = crate::config::remote_herdr_expr(remote_bin, session);
    // no `exec` on the second command any more: it would replace the shell we
    // are keeping, turning every poll back into a new channel
    let script = format!(
        "{b} pane get {p}; echo '<<>>'; {b} pane process-info --pane {p}",
        b = bin,
        p = sh_quote(pane)
    );
    // Busy means a poll is already asking this exact question. Skip rather than
    // wait: the caller keeps its last value and the next tick gets the fresh one.
    let Ok(mut poller) = poller.try_lock() else {
        return (None, None);
    };
    let Some(text) = poller.ask(&script).await else {
        return (None, None);
    };
    let Some((pane_json, proc_json)) = text.split_once("<<>>") else {
        return (None, None);
    };
    (classify(pane_json, proc_json), revision(pane_json))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pane_with_agent(a: Option<&str>) -> String {
        match a {
            Some(a) => format!(r#"{{"result":{{"pane":{{"agent":"{a}"}}}}}}"#),
            None => r#"{"result":{"pane":{}}}"#.to_string(),
        }
    }

    fn proc_with(leaf: &str) -> String {
        format!(r#"{{"result":{{"process_info":{{"foreground_processes":[{{"name":"{leaf}"}}]}}}}}}"#)
    }

    #[test]
    fn shells_recognized_including_login_and_path() {
        assert!(is_shell("zsh"));
        assert!(is_shell("bash"));
        assert!(is_shell("-bash")); // login shell
        assert!(is_shell("/usr/bin/fish")); // full path
        assert!(is_shell("pwsh.exe")); // windows
        assert!(!is_shell("vim"));
        assert!(!is_shell("htop"));
        assert!(!is_shell("nvim"));
        assert!(!is_shell("lazygit"));
    }

    /// The revision is what tells an idle mirror apart from a stalled one, so
    /// its absence must read as "unknown", never as "nothing happened".
    #[test]
    fn revision_is_read_when_present_and_absent_otherwise() {
        assert_eq!(
            revision(r#"{"result":{"pane":{"pane_id":"w1:p6","revision":4218}}}"#),
            Some(4218)
        );
        assert_eq!(revision(&pane_with_agent(None)), None);
        assert_eq!(revision("not json"), None);
        assert_eq!(revision(r#"{"result":{}}"#), None);
    }

    #[test]
    fn classify_indeterminate_on_empty_or_garbage() {
        let none = pane_with_agent(None);
        assert_eq!(
            classify(&none, r#"{"result":{"process_info":{"foreground_processes":[]}}}"#),
            None
        );
        assert_eq!(classify(&none, "not json"), None);
        assert_eq!(classify("not json", &proc_with("zsh")), None);
    }

    /// A Herdr API server that answers exactly like the guest's does: one
    /// request per connection, then close. Every accepted connection is
    /// recorded, because on a sandbox guest behind the daemon's `-L` forward a
    /// connection is a direct-tcpip channel and a guest `exec` — so the count
    /// of accepts IS the cost this ticket is about.
    struct FakeApi {
        _dir: std::path::PathBuf,
        socket: String,
        methods: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        server: tokio::task::JoinHandle<()>,
    }

    impl FakeApi {
        fn start(tag: &str, agent: Option<&'static str>) -> FakeApi {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

            let dir = std::env::temp_dir()
                .join(format!("fg-api-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let socket = dir.join("api.sock");
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let methods = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = methods.clone();
            let server = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else { return };
                    let (read, mut write) = stream.into_split();
                    let mut lines = BufReader::new(read).lines();
                    let Ok(Some(line)) = lines.next_line().await else { continue };
                    let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                    let method = request["method"].as_str().unwrap().to_string();
                    let result = match method.as_str() {
                        "pane.get" => match agent {
                            Some(a) => serde_json::json!({
                                "pane": { "pane_id": "w1:p1", "agent": a, "revision": 42 }
                            }),
                            None => serde_json::json!({
                                "pane": { "pane_id": "w1:p1", "revision": 42 }
                            }),
                        },
                        "pane.process_info" => serde_json::json!({
                            "process_info": { "foreground_processes": [{ "name": "vim" }] }
                        }),
                        other => panic!("unexpected method {other}"),
                    };
                    seen.lock().unwrap().push(method);
                    let response =
                        serde_json::json!({ "id": request["id"], "result": result }).to_string()
                            + "\n";
                    let _ = write.write_all(response.as_bytes()).await;
                    // one request per connection, then close — the real server's
                    // contract, and the reason a connection cannot be reused
                }
            });
            FakeApi {
                socket: socket.to_string_lossy().into_owned(),
                methods,
                server,
                _dir: dir,
            }
        }

        fn calls(&self) -> Vec<String> {
            self.methods.lock().unwrap().clone()
        }
    }

    impl Drop for FakeApi {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_dir_all(&self._dir);
        }
    }

    /// The poller a host with an API forward is given. If the poll ever falls
    /// through to it the test fails loudly rather than quietly opening a
    /// session channel to the guest — the thing `.26` did on these hosts.
    fn poller_that_must_not_be_used() -> Shared {
        shared(crate::poll_channel::Transport::Local {
            program: "/nonexistent/ssh-must-not-run".into(),
        })
    }

    /// The socket transport: metadata rides the daemon's API forward, and the
    /// pane's shell channel is never opened.
    #[tokio::test(flavor = "current_thread")]
    async fn selected_api_metadata_never_falls_through_to_the_pane_channel() {
        let api = FakeApi::start("agent", Some("codex"));
        let (fg, revision) =
            poll(&poller_that_must_not_be_used(), None, None, "w1:p1", Some(&api.socket)).await;
        assert_eq!(fg, Some(Fg::Agent));
        assert_eq!(revision, Some(42));
    }

    /// The bound this ticket turns on: an agent pane — what these hosts exist
    /// to mirror — costs ONE connection per poll, because `pane.get` already
    /// carries the answer `pane.process_info` would be asked for. Twenty idle
    /// polls are twenty connections, not forty.
    #[tokio::test(flavor = "current_thread")]
    async fn an_agent_pane_costs_one_bridge_connection_per_poll() {
        let api = FakeApi::start("one-call", Some("codex"));
        let poller = poller_that_must_not_be_used();
        for _ in 0..20 {
            let (fg, revision) = poll(&poller, None, None, "w1:p1", Some(&api.socket)).await;
            assert_eq!(fg, Some(Fg::Agent));
            assert_eq!(revision, Some(42));
        }
        let calls = api.calls();
        assert_eq!(calls.len(), 20, "one connection per poll, not two: {calls:?}");
        assert!(
            calls.iter().all(|m| m == "pane.get"),
            "process_info was asked for anyway: {calls:?}"
        );
    }

    /// A pane herdr has NOT identified an agent in still needs the second
    /// question, and still gets it — the saving is a skipped question, never a
    /// guessed answer.
    #[tokio::test(flavor = "current_thread")]
    async fn a_pane_without_an_agent_still_pays_for_its_process_group() {
        let api = FakeApi::start("two-calls", None);
        let (fg, revision) =
            poll(&poller_that_must_not_be_used(), None, None, "w1:p1", Some(&api.socket)).await;
        assert_eq!(fg, Some(Fg::Mouse)); // the stub's leaf is `vim`
        assert_eq!(revision, Some(42));
        assert_eq!(api.calls(), vec!["pane.get", "pane.process_info"]);
    }

    /// A bridge that has gone away costs one failed connection, and the pane
    /// keeps its last classification rather than being told something false.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unreachable_bridge_answers_nothing_and_opens_no_session() {
        let (fg, revision) = poll(
            &poller_that_must_not_be_used(),
            None,
            None,
            "w1:p1",
            Some("/nonexistent/api.sock"),
        )
        .await;
        assert_eq!((fg, revision), (None, None));
    }
}

#[cfg(test)]
mod hung_poll_is_bounded {
    //! The poll that leaked. A remote `herdr pane get` that never answers used
    //! to leave its client running for as long as the pane lived, one more per
    //! throttle interval, because the spawn had no deadline and no owned
    //! process group to reap. These two properties are what stop that.

    use super::*;
    use std::io::Write;
    use std::time::{Duration, Instant};

    /// A stand-in for `docker`: answers `ps` with one id, then hangs on `exec`
    /// exactly as an unresponsive remote does. Writing its own pid out lets the
    /// test ask afterwards whether the process actually went away.
    fn hanging_docker(dir: &std::path::Path) -> String {
        let pidfile = dir.join("child.pid");
        let bin = dir.join("docker-stub");
        let mut fh = std::fs::File::create(&bin).unwrap();
        write!(
            fh,
            "#!/bin/sh\n\
             if [ \"$1\" = ps ]; then echo deadbeefcafe; exit 0; fi\n\
             echo $$ > {pid}\n\
             exec sleep 600\n",
            pid = pidfile.display()
        )
        .unwrap();
        drop(fh);
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin.to_string_lossy().into_owned()
    }

    fn alive(pid: i32) -> bool {
        unsafe { libc::kill(pid, 0) == 0 }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_poll_that_never_answers_is_stopped_and_leaves_no_client_behind() {
        let dir = std::env::temp_dir().join(format!("fg-poll-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let docker_bin = hanging_docker(&dir);
        let container = crate::pane::ContainerArg {
            kind: crate::config::HostKind::DockerContainer("whatever".into()),
            docker_bin,
        };

        let poller = shared(crate::poll_channel::Transport::Docker {
            docker_bin: container.docker_bin.clone(),
            kind: container.kind.clone(),
        });

        let started = Instant::now();
        let (fg, revision) = poll(&poller, None, None, "w1:p1", None).await;
        let took = started.elapsed();

        // the deadline held: it returned, and near the ceiling rather than at
        // the stub's own 600s. One ceiling, not two — a timed-out ask is not
        // retried on a fresh channel (see `poll_channel::AskFailure`).
        let bound = crate::poll_channel::ASK_TIMEOUT;
        assert!(
            took < bound + Duration::from_secs(4),
            "poll ran {took:?}, so nothing bounded it"
        );
        assert!(took >= bound - Duration::from_secs(1));
        // a failed poll says nothing, so the caller keeps its last known value
        assert!(fg.is_none() && revision.is_none());

        // and the client it owned is gone, not merely abandoned
        let pid: i32 = std::fs::read_to_string(dir.join("child.pid"))
            .expect("stub never recorded a pid")
            .trim()
            .parse()
            .unwrap();
        for _ in 0..50 {
            if !alive(pid) {
                std::fs::remove_dir_all(&dir).ok();
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the poll's own client {pid} outlived it");
    }
}

#[cfg(test)]
mod one_poll_per_pane {
    //! Rapid forced triggers must not put a second client on the wire.
    //!
    //! `spawn_foreground_poll(force = true)` skips the interval throttle, so
    //! before the gate a burst of input could start polls as fast as the events
    //! arrived while a slow remote retired none of them.

    use super::*;
    use std::io::Write;
    use std::time::Duration;

    /// Records every invocation's pid, then hangs, so the test can count how
    /// many clients a burst actually put on the wire.
    fn recording_docker(dir: &std::path::Path, hang: &str) -> String {
        let bin = dir.join("docker-stub");
        let mut fh = std::fs::File::create(&bin).unwrap();
        write!(
            fh,
            "#!/bin/sh\n\
             if [ \"$1\" = ps ]; then echo deadbeefcafe; exit 0; fi\n\
             echo $$ >> {log}\n\
             exec sleep {hang}\n",
            log = dir.join("invocations").display(),
            hang = hang
        )
        .unwrap();
        drop(fh);
        std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        bin.to_string_lossy().into_owned()
    }

    fn invocations(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_to_string(dir.join("invocations"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn docker_poller(docker_bin: String) -> Shared {
        shared(crate::poll_channel::Transport::Docker {
            docker_bin,
            kind: crate::config::HostKind::DockerContainer("whatever".into()),
        })
    }

    /// What the pane does: run under a permit, and do one more pass if a
    /// refresh was asked for while this one ran.
    fn spawn_under_gate(
        gate: &PollGate,
        poller: Shared,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let permit = gate.begin()?;
        Some(tokio::spawn(async move {
            let permit = permit;
            loop {
                let _ = poll(&poller, None, None, "w1:p1", None).await;
                if !permit.another_pass_wanted() {
                    break;
                }
            }
        }))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_burst_of_forced_polls_puts_exactly_one_client_on_the_wire() {
        let dir = std::env::temp_dir().join(format!("fg-gate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ct = docker_poller(recording_docker(&dir, "600"));
        let gate = PollGate::new();

        // one slow poll in flight, then fifty forced triggers on top of it
        let first = spawn_under_gate(&gate, ct.clone()).expect("gate was free");
        tokio::time::sleep(Duration::from_millis(700)).await;
        let mut extra = 0;
        for _ in 0..50 {
            if spawn_under_gate(&gate, ct.clone()).is_some() {
                extra += 1;
            }
        }
        assert_eq!(extra, 0, "the gate let {extra} more polls start");

        // and the wire agrees: one client, not fifty-one
        tokio::time::sleep(Duration::from_millis(500)).await;
        let seen = invocations(&dir);
        assert_eq!(seen.len(), 1, "clients on the wire: {seen:?}");

        // the hung poll is stopped by the deadline, its coalesced pass runs
        // (the refresh those triggers asked for), and then the gate reopens
        first.await.unwrap();
        assert!(gate.begin().is_some(), "the gate stayed shut after completion");
        let after = invocations(&dir);
        assert_eq!(after.len(), 2, "expected one coalesced extra pass: {after:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_a_poll_reopens_the_gate_for_the_next_one() {
        let dir = std::env::temp_dir().join(format!("fg-cancel-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ct = docker_poller(recording_docker(&dir, "600"));
        let gate = PollGate::new();

        let running = spawn_under_gate(&gate, ct.clone()).expect("gate was free");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(gate.begin().is_none(), "gate should be shut while one runs");

        // the pane going away mid-poll must not leave the gate shut forever
        running.abort();
        let _ = running.await;
        for _ in 0..50 {
            if gate.begin().is_some() {
                std::fs::remove_dir_all(&dir).ok();
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the gate stayed shut after the poll was cancelled");
    }
}
