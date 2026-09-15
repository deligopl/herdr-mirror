// Foreground-process detection for the mirror streamer.
//
// herdr strips the mouse-mode DECSET from the frames the plugin observes, so the
// streamer can't tell whether the remote pane's app wants the mouse. As a proxy,
// query the remote pane's foreground process (`herdr pane process-info`) and
// classify it: a plain shell at a prompt never enables mouse reporting, so mouse
// events should stay local (no garbage in the prompt); anything else is treated
// as a possible mouse-aware TUI and clicks are forwarded. This is a heuristic
// stand-in until herdr exposes the pane's mouse-reporting state through the API.

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
pub async fn poll(
    poller: &Shared,
    remote_bin: Option<&str>,
    session: Option<&str>,
    pane: &str,
) -> (Option<Fg>, Option<u64>) {
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
}
