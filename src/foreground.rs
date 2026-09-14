// Foreground-process detection for the mirror streamer.
//
// herdr strips the mouse-mode DECSET from the frames the plugin observes, so the
// streamer can't tell whether the remote pane's app wants the mouse. As a proxy,
// query the remote pane's foreground process (`herdr pane process-info`) and
// classify it: a plain shell at a prompt never enables mouse reporting, so mouse
// events should stay local (no garbage in the prompt); anything else is treated
// as a possible mouse-aware TUI and clicks are forwarded. This is a heuristic
// stand-in until herdr exposes the pane's mouse-reporting state through the API.

use std::ffi::OsStr;
use std::path::Path;

use crate::pane::sh_quote;
use crate::remote::{ssh_with_program, SSH_COMMON_OPTS};

/// How long one metadata poll may take before it is stopped and reaped.
///
/// A healthy poll is one ssh round trip over an existing ControlMaster, tens
/// of milliseconds. This ceiling exists for the unhealthy case: a remote
/// `herdr pane get` that never answers. Spawning is throttled to
/// `FG_POLL_INTERVAL`, so bounding each poll here is what keeps the number of
/// outstanding ones finite rather than growing for as long as the remote stays
/// unresponsive.
const FG_POLL_TIMEOUT_MS: u64 = 8_000;

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
    let pane: serde_json::Value = serde_json::from_str(pane_json).ok()?;
    if pane.get("result")?.get("pane")?.get("agent").and_then(|v| v.as_str()).is_some() {
        return Some(Fg::Agent);
    }
    let v: serde_json::Value = serde_json::from_str(proc_json).ok()?;
    let fg = v.get("result")?.get("process_info")?.get("foreground_processes")?.as_array()?;
    // the last foreground process is the actually-running leaf, so `sudo vim`
    // classifies on `vim`, not `sudo`
    let name = fg.last()?.get("name")?.as_str()?;
    Some(if is_shell(name) { Fg::Shell } else { Fg::Mouse })
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

/// Query the remote pane over ssh: its foreground classification, and its
/// content revision. `None` on any failure (ssh/network/parse) so the caller
/// keeps its last known value.
pub async fn poll(
    ssh_target: &str,
    remote_bin: Option<&str>,
    session: Option<&str>,
    pane: &str,
    ctl_path: Option<&str>,
    api_socket: Option<&str>,
    container: Option<&crate::pane::ContainerArg>,
) -> (Option<Fg>, Option<u64>) {
    // A selected API forward already reaches this exact Herdr server.  Keep
    // foreground classification and revision on that transport as well: each
    // API call is a fresh direct-tcpip channel, but neither is an ssh `session`
    // channel and neither invokes the sandbox lifecycle-gated exec path.
    if let Some(socket) = api_socket {
        let api = crate::api::ApiClient::at(Path::new(socket));
        let pane_value = match api
            .request("pane.get", serde_json::json!({ "pane_id": pane }))
            .await
        {
            Ok(value) => value,
            Err(_) => return (None, None),
        };
        let process_value = match api
            .request("pane.process_info", serde_json::json!({ "pane_id": pane }))
            .await
        {
            Ok(value) => value,
            Err(_) => return (None, None),
        };
        let pane_json = serde_json::json!({ "result": pane_value }).to_string();
        let process_json = serde_json::json!({ "result": process_value }).to_string();
        return (classify(&pane_json, &process_json), revision(&pane_json));
    }
    // same expression as the observe session (configured path or PATH auto)
    let bin = crate::config::remote_herdr_expr(remote_bin, session);
    // both answers in ONE hop: same ssh round trip cost as the old single query
    let cmd = format!(
        "{b} pane get {p}; echo '<<>>'; exec {b} pane process-info --pane {p}",
        b = bin,
        p = sh_quote(pane)
    );
    // One guarded spawn for both transports. `remote::ssh_with_program` owns
    // the complete process group, enforces the deadline and reaps on timeout,
    // so a poll that never answers is stopped instead of being left behind —
    // the behaviour a bare `output().await` here did not have.
    let (program, args) = match container {
        Some(ct) => {
            // async resolve, not the blocking one: this runs on the pane's
            // single-threaded runtime and fires on every keystroke burst, so a
            // blocking `docker ps` would stall input and rendering (and hang
            // the pane outright if the Docker daemon wedges).
            //
            // No ControlMaster equivalent is needed — docker exec is local, so
            // there is no handshake to amortize.
            let Some(id) = crate::docker::resolve(&ct.docker_bin, &ct.kind)
                .await
                .ok()
                .and_then(|ids| ids.into_iter().next())
            else {
                return (None, None);
            };
            // `sh -c` not `-lc`: match ssh's non-login remote shell
            (
                ct.docker_bin.clone(),
                vec!["exec".to_string(), id, "sh".to_string(), "-c".to_string(), cmd],
            )
        }
        None => {
            let mut args = Vec::new();
            // reuse the daemon's ControlMaster when given so the poll skips the
            // handshake; `-S` without `-M` uses an existing master or, if the socket
            // isn't there, connects directly — so this degrades gracefully
            if let Some(path) = ctl_path {
                args.push("-S".to_string());
                args.push(path.to_string());
            }
            args.extend(SSH_COMMON_OPTS.iter().map(|s| s.to_string()));
            args.push(ssh_target.to_string());
            args.push(cmd);
            ("ssh".to_string(), args)
        }
    };
    let out = ssh_with_program(OsStr::new(&program), &args, FG_POLL_TIMEOUT_MS).await;
    if out.code != 0 {
        return (None, None);
    }
    let text = out.out.as_str();
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

    #[tokio::test(flavor = "current_thread")]
    async fn selected_api_metadata_never_falls_through_to_ssh() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let dir = std::env::temp_dir().join(format!("fg-api-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("api.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut lines = BufReader::new(read).lines();
                let line = lines.next_line().await.unwrap().unwrap();
                let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                let id = request["id"].clone();
                let result = match request["method"].as_str().unwrap() {
                    "pane.get" => serde_json::json!({
                        "pane": { "pane_id": "w1:p1", "agent": "codex", "revision": 42 }
                    }),
                    "pane.process_info" => serde_json::json!({
                        "process_info": { "foreground_processes": [{ "name": "node" }] }
                    }),
                    other => panic!("unexpected method {other}"),
                };
                let response = serde_json::json!({ "id": id, "result": result }).to_string() + "\n";
                write.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let socket_text = socket.to_string_lossy().into_owned();
        let (fg, revision) = poll(
            "ssh-must-not-run.invalid",
            None,
            None,
            "w1:p1",
            None,
            Some(&socket_text),
            None,
        )
        .await;
        server.await.unwrap();
        assert_eq!(fg, Some(Fg::Agent));
        assert_eq!(revision, Some(42));
        let _ = std::fs::remove_dir_all(dir);
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

        let started = Instant::now();
        let (fg, revision) = poll("unused", None, None, "w1:p1", None, None, Some(&container)).await;
        let took = started.elapsed();

        // the deadline held: it returned, and near the ceiling rather than at
        // the stub's own 600s
        assert!(
            took < Duration::from_millis(FG_POLL_TIMEOUT_MS + 4_000),
            "poll ran {took:?}, so nothing bounded it"
        );
        assert!(took >= Duration::from_millis(FG_POLL_TIMEOUT_MS - 1_000));
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
    use std::sync::Arc;
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

    fn container(docker_bin: String) -> crate::pane::ContainerArg {
        crate::pane::ContainerArg {
            kind: crate::config::HostKind::DockerContainer("whatever".into()),
            docker_bin,
        }
    }

    /// What the pane does: run under a permit, and do one more pass if a
    /// refresh was asked for while this one ran.
    fn spawn_under_gate(
        gate: &PollGate,
        ct: Arc<crate::pane::ContainerArg>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let permit = gate.begin()?;
        Some(tokio::spawn(async move {
            let permit = permit;
            loop {
                let _ = poll("unused", None, None, "w1:p1", None, None, Some(ct.as_ref())).await;
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
        let ct = Arc::new(container(recording_docker(&dir, "600")));
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
        let ct = Arc::new(container(recording_docker(&dir, "600")));
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
