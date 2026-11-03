use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_TEST: AtomicU64 = AtomicU64::new(0);
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM); }
        let _ = self.0.wait();
    }
}

fn test_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-mirror-pause-process-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
        NEXT_TEST.fetch_add(1, Ordering::Relaxed)
    ))
}

fn call_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

fn matching_calls(path: &Path, needle: &str) -> usize {
    fs::read_to_string(path)
        .map(|text| text.lines().filter(|line| line.contains(needle)).count())
        .unwrap_or(0)
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("condition was not met within {timeout:?}");
}

#[test]
fn pause_quiesces_a_real_supervised_stream_and_start_resumes_once() {
    let root = test_dir();
    let bin_dir = root.join("bin");
    let state_dir = root.join("state");
    let calls = root.join("ssh-calls");
    fs::create_dir_all(&bin_dir).expect("fake bin dir");
    fs::create_dir_all(&state_dir).expect("state dir");

    let fake_ssh = bin_dir.join("odx-connect");
    fs::write(
        &fake_ssh,
        r#"#!/bin/sh
case "$*" in
  *"ps -o args="*) printf 'cleanup\n' >> "$HERDR_MIRROR_TEST_SSH_CALLS"; exit 0 ;;
  *"terminal session"*) printf 'attach\n' >> "$HERDR_MIRROR_TEST_SSH_CALLS" ;;
  *) printf 'poll\n' >> "$HERDR_MIRROR_TEST_SSH_CALLS"; exit 0 ;;
esac
printf 'herdr-mirror-remote-pid %s\n' "$$"
trap 'exit 0' TERM INT HUP
while :; do sleep 1; done
"#,
    )
    .expect("fake ssh");
    fs::set_permissions(&fake_ssh, fs::Permissions::from_mode(0o755)).expect("executable fake ssh");

    let inherited_path = std::env::var_os("PATH").unwrap_or_default();
    let mut supervisor = ProcessGuard(
        Command::new(env!("CARGO_BIN_EXE_herdr-mirror"))
            .args(["pane", "fake-host", "remote:p1", "--remote-bin", "herdr"])
            .env("HERDR_MIRROR_STATE_DIR", &state_dir)
            // This id hashes to resume slot zero, keeping the process test fast.
            .env("HERDR_PANE_ID", "wtest:p40")
            .env("HERDR_MIRROR_TEST_SSH_CALLS", &calls)
            .env("PATH", std::env::join_paths(std::iter::once(bin_dir.clone()).chain(std::env::split_paths(&inherited_path))).expect("PATH"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("supervisor"),
    );

    wait_until(Duration::from_secs(3), || matching_calls(&calls, "attach") >= 1);
    assert_eq!(matching_calls(&calls, "attach"), 1, "one active attach before pause");

    fs::write(state_dir.join("pause").join("daemon.paused"), b"paused\n").expect("pause marker");
    // The second call is the existing bounded, identity-guarded cleanup. Once
    // it completes, multiple pause ticks must not open a transport or poll.
    wait_until(Duration::from_secs(4), || matching_calls(&calls, "cleanup") >= 1);
    let paused_calls = call_count(&calls);
    thread::sleep(Duration::from_millis(650));
    assert_eq!(call_count(&calls), paused_calls, "pause must quiesce transport attempts");
    assert!(supervisor.0.try_wait().expect("supervisor status").is_none(), "stable supervisor keeps the local pane alive");

    fs::remove_file(state_dir.join("pause").join("daemon.paused")).expect("resume marker");
    wait_until(Duration::from_secs(2), || matching_calls(&calls, "attach") > 1);
    assert_eq!(matching_calls(&calls, "attach"), 2, "resume starts exactly one stream");

    unsafe { libc::kill(supervisor.0.id() as i32, libc::SIGTERM); }
    let _ = supervisor.0.wait();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn stopped_refusal_waits_for_running_projection_then_reattaches() {
    let root = test_dir();
    let bin = root.join("bin");
    let state = root.join("state");
    let config = root.join("config");
    fs::create_dir_all(&bin).unwrap();
    fs::create_dir_all(&state).unwrap();
    fs::create_dir_all(&config).unwrap();
    let calls = root.join("calls");
    let running = root.join("running");
    let script = bin.join("odx-connect");
    fs::write(&script, r#"#!/bin/sh
case "$*" in
  *"terminal session"*) ;;
  *) exit 0 ;;
esac
printf 'attach\n' >> "$TEST_CALLS"
if [ ! -f "$TEST_RUNNING" ]; then
    printf 'odx-connect: workspace fake-host desired state is stopped\n' >&2
    exit 2
fi
printf 'herdr-mirror-remote-pid %s\n' "$$"
trap 'exit 0' TERM INT HUP
while :; do sleep 1; done
"#).unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    // A refusal may precede the local projection update. It must stay latched
    // even while the existing projection still says running.
    let projection = config.join("hosts.toml");
    fs::write(&projection, "[hosts.fake-host]\ntarget = 'fake-host'\ndesired_state = 'running'\n").unwrap();
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let mut pane = ProcessGuard(Command::new(env!("CARGO_BIN_EXE_herdr-mirror"))
        .args(["pane", "fake-host", "remote:p1", "--remote-bin", "herdr", "--always-control"])
        .env("HERDR_MIRROR_STATE_DIR", &state)
        .env("HERDR_PLUGIN_CONFIG_DIR", &config)
        .env("HERDR_PANE_ID", "wtest:p40")
        .env("TEST_CALLS", &calls)
        .env("TEST_RUNNING", &running)
        .env("PATH", std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(&inherited))).unwrap())
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().unwrap());
    wait_until(Duration::from_secs(3), || call_count(&calls) == 1);
    thread::sleep(Duration::from_millis(2300));
    assert_eq!(call_count(&calls), 1, "stale running projection must not retry a refusal");
    fs::write(&projection, "[hosts.fake-host]\ntarget = 'fake-host'\ndesired_state = 'stopped'\n").unwrap();
    thread::sleep(Duration::from_millis(1200));
    assert_eq!(call_count(&calls), 1, "stopped projection must keep transport quiet");
    fs::write(&running, "yes").unwrap();
    fs::write(&projection, "[hosts.fake-host]\ntarget = 'fake-host'\ndesired_state = 'running'\n").unwrap();
    wait_until(Duration::from_secs(4), || call_count(&calls) == 2);
    assert!(pane.0.try_wait().unwrap().is_none(), "same tile remains alive");
    unsafe { libc::kill(pane.0.id() as i32, libc::SIGTERM); }
    let _ = pane.0.wait();
    let _ = fs::remove_dir_all(root);
}
