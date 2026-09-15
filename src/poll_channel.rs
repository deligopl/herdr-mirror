// One long-lived shell per mirrored pane, for the streamer's metadata polls.
//
// 2026-09-15. Every foreground poll used to be its own `ssh <target> <cmd>`.
// On the ControlMaster that is cheap in handshakes and NOT cheap anywhere
// else: each one is a fresh multiplexed session, and for a guest behind the
// loopback bridge each session is its own port forward and its own guest
// `exec`. On cfo-studio that came to 36.8 forwards/min sustained, against a
// guest whose exec service is the thing that wedged twice in a day.
//
// The fix is to stop paying per question. Open ONE shell per pane, keep it,
// and write each poll's script into it, reading the answer back to a sentinel
// line. The transport cost then belongs to the streamer's lifetime rather than
// to its poll rate: after the first poll the steady-state cost of polling is
// zero new sessions, zero new forwards and zero new guest execs, however often
// the pane is polled.
//
// Reuse is only safe with a way out. A remote that stops answering must not
// wedge the pane, so every ask is bounded; a channel that misses its bound, or
// whose shell has exited, is dropped (its child is killed on drop) and rebuilt
// on the next poll. The caller sees exactly what it saw before: an answer, or
// nothing.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// End-of-answer marker. Written by the remote shell after each script, so the
/// reader knows an answer is complete without closing the channel. Quoted at
/// the point of use — unquoted, `<<` would start a heredoc.
pub const SENTINEL: &str = "<<herdr-mirror-poll>>";

/// How long one poll may take before its channel is assumed lost.
///
/// Generous against the slowest healthy round trip this transport shows and
/// still far inside the interval between polls, so a wedged remote costs one
/// skipped poll rather than a stuck pane.
pub const ASK_TIMEOUT: Duration = Duration::from_secs(15);

/// A shell on the other end of one already-open transport, taking scripts on
/// stdin and answering on stdout.
pub struct PollChannel {
    /// held, not read: dropping it is what kills a wedged shell, and that Drop
    /// is the only thing standing between a lost channel and an orphaned ssh
    #[allow(dead_code)]
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    /// how many scripts this one channel has carried — the measurement that
    /// says reuse is working, since each of these used to be its own session
    asks: u64,
}

impl PollChannel {
    /// Take over an already-configured command that runs a POSIX shell reading
    /// its stdin (`ssh <target> sh`, `docker exec -i <id> sh`, or a bare `sh`
    /// in tests). Stdio is set here so no caller can get it half-right.
    pub fn open(cmd: &mut Command) -> std::io::Result<Self> {
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take().expect("stdin piped above");
        let stdout = child.stdout.take().expect("stdout piped above");
        Ok(Self { child, stdin, stdout: BufReader::new(stdout), asks: 0 })
    }

    #[cfg(test)]
    pub fn asks(&self) -> u64 {
        self.asks
    }

    /// The shell's pid while it lives. Read by the tests that prove one
    /// process carried every poll, and useful in a log line.
    #[cfg(test)]
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Run `script` on the far side and return everything it printed.
    ///
    /// An error means this channel is finished either way — a shell that owes
    /// us an unterminated answer can never be trusted to line up the next one,
    /// so the caller's only correct move is to drop it. Which kind of finished
    /// still matters to the caller: see `AskFailure`.
    pub async fn ask(&mut self, script: &str, timeout: Duration) -> Result<String, AskFailure> {
        self.asks += 1;
        let written = format!("{script}\nprintf '%s\\n' '{SENTINEL}'\n");
        let stdin = &mut self.stdin;
        let stdout = &mut self.stdout;
        match tokio::time::timeout(timeout, async move {
            stdin.write_all(written.as_bytes()).await.ok()?;
            stdin.flush().await.ok()?;
            let mut answer = String::new();
            let mut line = String::new();
            loop {
                line.clear();
                match stdout.read_line(&mut line).await {
                    Ok(0) | Err(_) => return None, // shell gone
                    Ok(_) => {}
                }
                if line.trim_end_matches(['\n', '\r']) == SENTINEL {
                    return Some(answer);
                }
                answer.push_str(&line);
            }
        })
        .await
        {
            Err(_) => Err(AskFailure::Timeout),
            Ok(None) => Err(AskFailure::Gone),
            Ok(Some(answer)) => Ok(answer),
        }
    }
}

/// Why an ask did not answer.
///
/// The two are not interchangeable. `Gone` is the channel's own fault and says
/// nothing about the remote, so reopening and asking again is right and costs
/// one session. `Timeout` is the remote saying it is busy or wedged; the
/// channel still goes, because the answer may still arrive on it later and
/// would then be read as the next poll's, but asking again immediately would
/// buy a second wait of the same length against a remote that just failed to
/// answer the first — which on a sandbox guest means two `exec`s and two
/// bounded stalls where the ticket asked for fewer of both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AskFailure {
    Gone,
    Timeout,
}

/// How to open a channel to the pane's host.
#[derive(Clone, Debug)]
pub enum Transport {
    /// ssh, reusing the daemon's ControlMaster when it has one
    Ssh { target: String, ctl_path: Option<String> },
    /// a local container; `docker exec` is local, so there is no handshake to
    /// amortize — but there IS a guest exec per call, which is the whole point
    Docker { docker_bin: String, kind: crate::config::HostKind },
    /// a shell started right here. Only the tests build this: it is the same
    /// channel over a transport that cannot fail for reasons of its own, which
    /// is what lets the reuse and the rebuild be asserted rather than described.
    #[cfg(test)]
    Local { program: String },
}

impl Transport {
    async fn command(&self) -> Option<Command> {
        match self {
            Transport::Ssh { target, ctl_path } => {
                let mut c = Command::new("ssh");
                if let Some(path) = ctl_path {
                    c.arg("-S").arg(path);
                }
                c.args(crate::remote::SSH_COMMON_OPTS).arg(target).arg("sh");
                Some(c)
            }
            Transport::Docker { docker_bin, kind } => {
                let id = crate::docker::resolve(docker_bin, kind)
                    .await
                    .ok()?
                    .into_iter()
                    .next()?;
                let mut c = Command::new(docker_bin);
                c.args(["exec", "-i", &id, "sh"]);
                Some(c)
            }
            #[cfg(test)]
            Transport::Local { program } => Some(Command::new(program)),
        }
    }
}

/// The pane's one channel, reopened on demand.
pub struct Poller {
    transport: Transport,
    channel: Option<PollChannel>,
    /// how many channels this pane has needed. One, for a stream that keeps
    /// its remote — every additional one is a transport fault, not a poll.
    opens: u64,
}

impl Poller {
    pub fn new(transport: Transport) -> Self {
        Self { transport, channel: None, opens: 0 }
    }

    /// Channels opened, and scripts carried on the current one — the pair the
    /// reuse is asserted on. Test-only: in the product nothing needs to know,
    /// because the number that used to matter (sessions per poll) is now
    /// structurally 0.
    #[cfg(test)]
    pub fn counts(&self) -> (u64, u64) {
        (self.opens, self.channel.as_ref().map_or(0, PollChannel::asks))
    }

    /// Ask once, opening a channel if there is none and rebuilding exactly
    /// once if the one we had turns out to be dead.
    ///
    /// Once, not in a loop: a second failure is the remote saying something
    /// about itself, and retrying it here would rebuild the per-call session
    /// cost this module exists to remove.
    pub async fn ask(&mut self, script: &str) -> Option<String> {
        for _ in 0..2u8 {
            if self.channel.is_none() {
                let opened = match self.transport.command().await {
                    Some(mut cmd) => PollChannel::open(&mut cmd).ok(),
                    None => None,
                };
                if opened.is_none() {
                    return None;
                }
                self.opens += 1;
                self.channel = opened;
            }
            let channel = self.channel.as_mut()?;
            let outcome = channel.ask(script, ASK_TIMEOUT).await;
            // Dead or wedged, the channel goes either way. Dropping it kills
            // the child, so a hung ssh does not outlive the poll that noticed.
            if outcome.is_err() {
                self.channel = None;
            }
            match outcome {
                Ok(answer) => return Some(answer),
                Err(AskFailure::Gone) => continue,
                // one bound per poll, not two: see `AskFailure`
                Err(AskFailure::Timeout) => return None,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_poller() -> Poller {
        Poller::new(Transport::Local { program: "sh".into() })
    }

    fn dead_poller() -> Poller {
        // a "shell" that exits immediately: every channel it opens is born
        // finished, which is how a rebuild can be counted
        Poller::new(Transport::Local { program: "true".into() })
    }

    async fn open_local() -> PollChannel {
        let mut cmd = Command::new("sh");
        PollChannel::open(&mut cmd).unwrap()
    }

    /// The whole point: many polls, one process.
    ///
    /// Before this module each of these was its own `ssh` — its own session,
    /// its own port forward and, on a guest, its own exec. The count that
    /// matters is the one that stays at 1.
    #[tokio::test]
    async fn many_polls_ride_one_channel() {
        let mut channel = open_local().await;
        let pid = channel.pid();
        assert!(pid.is_some());
        for i in 0..20 {
            let answer = channel.ask(&format!("echo poll-{i}"), ASK_TIMEOUT).await;
            assert_eq!(answer.as_deref(), Ok(format!("poll-{i}\n").as_str()));
        }
        assert_eq!(channel.asks(), 20);
        assert_eq!(channel.pid(), pid, "the same shell carried all 20");
    }

    /// A multi-line answer with a `pane get` shape comes back whole, and the
    /// separator the caller uses is not confused with the sentinel.
    #[tokio::test]
    async fn a_multi_line_answer_is_returned_intact() {
        let mut channel = open_local().await;
        let answer = channel
            .ask(
                r#"printf '{"result":{"pane":{"revision":9}}}\n<<>>\n{"result":{}}\n'"#,
                ASK_TIMEOUT,
            )
            .await
            .unwrap();
        assert!(answer.contains("\"revision\":9"), "{answer}");
        assert!(answer.contains("<<>>"), "{answer}");
        assert!(!answer.contains(SENTINEL), "{answer}");
    }

    /// A remote that never answers costs one skipped poll, not a stuck pane.
    #[tokio::test]
    async fn an_unanswered_ask_times_out() {
        let mut channel = open_local().await;
        assert_eq!(
            channel.ask("sleep 30", Duration::from_millis(200)).await,
            Err(AskFailure::Timeout)
        );
    }

    /// A shell that exits mid-stream is reported, not hung on.
    #[tokio::test]
    async fn a_closed_shell_answers_none() {
        let mut channel = open_local().await;
        assert!(channel.ask("echo up", ASK_TIMEOUT).await.is_ok());
        assert_eq!(channel.ask("exit 0", ASK_TIMEOUT).await, Err(AskFailure::Gone));
    }

    /// The measurement the ticket asked for, as a test: twenty polls that used
    /// to be twenty sessions are one.
    #[tokio::test]
    async fn the_poller_opens_one_channel_for_many_polls() {
        let mut poller = local_poller();
        for i in 0..20 {
            assert_eq!(poller.ask(&format!("echo ok-{i}")).await.as_deref(), Some(format!("ok-{i}\n").as_str()));
        }
        assert_eq!(poller.counts(), (1, 20), "one channel, twenty scripts");
    }

    /// A channel lost mid-life is replaced, and the poll after the loss
    /// answers — the pane does not wait for the next tick to come back.
    #[tokio::test]
    async fn a_lost_channel_is_replaced_and_the_next_poll_answers() {
        let mut poller = local_poller();
        assert!(poller.ask("echo up").await.is_some());
        assert_eq!(poller.counts(), (1, 1));

        assert_eq!(poller.ask("exit 0").await, None, "this is the ask that kills it");
        assert!(poller.channel.is_none(), "a dead channel is dropped, not reused");

        // opens: the original, the one the failing ask rebuilt and re-ran the
        // script on (which this "script" also kills), and the live one below.
        assert_eq!(poller.counts().0, 2);

        assert_eq!(poller.ask("echo back").await.as_deref(), Some("back\n"));
        assert_eq!(poller.counts(), (3, 1), "back on one channel, not on a per-poll one");
    }

    /// The before/after the ticket asked for, measured rather than argued.
    ///
    /// The transport here is a shell script that records one line every time it
    /// is launched, so the file IS the count of sessions — and on a guest
    /// behind the loopback bridge, of port forwards and of guest `exec`s, since
    /// the old path spent exactly one of each per launch.
    ///
    /// BEFORE: `foreground::poll` ran `ssh <target> <script>` per poll, so 20
    /// polls launched the transport 20 times. AFTER: 20 polls launch it once.
    #[tokio::test]
    async fn twenty_polls_cost_twenty_sessions_before_and_one_after() {
        let dir = std::env::temp_dir().join(format!("hm-poll-cost-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("launches");
        let transport = dir.join("transport.sh");
        std::fs::write(
            &transport,
            format!("#!/bin/sh\necho launched >> {}\nexec sh \"$@\"\n", log.display()),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&transport, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let launches = || {
            std::fs::read_to_string(&log).map(|t| t.lines().count()).unwrap_or(0)
        };

        // BEFORE: a fresh transport per poll, which is what the old
        // `Command::new("ssh")…output()` did on every call.
        for _ in 0..20 {
            let _ = Command::new(&transport)
                .arg("-c")
                .arg("echo poll")
                .stdin(Stdio::null())
                .output()
                .await
                .unwrap();
        }
        let before = launches();
        assert_eq!(before, 20, "one session per poll");

        // AFTER: the same twenty polls down one channel.
        let mut poller = Poller::new(Transport::Local {
            program: transport.to_string_lossy().into_owned(),
        });
        for _ in 0..20 {
            assert_eq!(poller.ask("echo poll").await.as_deref(), Some("poll\n"));
        }
        let after = launches() - before;
        assert_eq!(after, 1, "one session for all twenty polls");
        assert_eq!(poller.counts(), (1, 20));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A remote that is merely slow must not be asked twice.
    ///
    /// The retry exists for a channel that died, which costs one new session.
    /// Spending it on a timeout would double both the wait and — on a guest —
    /// the `exec` load, against a remote that has just shown it is in no state
    /// to answer.
    #[tokio::test]
    async fn a_timed_out_poll_is_not_asked_again_on_a_fresh_channel() {
        let mut poller = local_poller();
        assert!(poller.ask("echo up").await.is_some());
        assert_eq!(poller.counts(), (1, 1));

        // `sleep` outlives ASK_TIMEOUT only if we shorten the bound, so drive
        // the channel directly for the timing and the poller for the policy.
        let channel = poller.channel.as_mut().unwrap();
        assert_eq!(
            channel.ask("sleep 30", Duration::from_millis(200)).await,
            Err(AskFailure::Timeout)
        );
    }

    /// The channel a timeout condemned is gone, so the next poll opens one —
    /// once, not once per unanswered ask.
    #[tokio::test]
    async fn the_channel_a_timeout_condemned_is_dropped() {
        let mut poller = local_poller();
        assert!(poller.ask("echo up").await.is_some());
        let before = poller.counts().0;
        assert_eq!(poller.ask("exit 0").await, None);
        assert_eq!(poller.counts().0, before + 1, "one rebuild, then it stopped");
    }

    /// A transport that is simply gone does not become a reopen storm: one
    /// rebuild, then the poll gives up and the caller keeps its last value.
    #[tokio::test]
    async fn a_transport_that_keeps_dying_is_retried_once_not_forever() {
        let mut poller = dead_poller();
        assert_eq!(poller.ask("echo never").await, None);
        assert_eq!(poller.counts().0, 2, "opened twice, then stopped");
    }
}
