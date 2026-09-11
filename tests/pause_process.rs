use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn test_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-mirror-pause-process-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
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

    let fake_ssh = bin_dir.join("ssh");
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

    fs::write(state_dir.join("daemon.paused"), b"paused\n").expect("pause marker");
    // The second call is the existing bounded, identity-guarded cleanup. Once
    // it completes, multiple pause ticks must not open a transport or poll.
    wait_until(Duration::from_secs(4), || matching_calls(&calls, "cleanup") >= 1);
    let paused_calls = call_count(&calls);
    thread::sleep(Duration::from_millis(650));
    assert_eq!(call_count(&calls), paused_calls, "pause must quiesce transport attempts");
    assert!(supervisor.0.try_wait().expect("supervisor status").is_none(), "stable supervisor keeps the local pane alive");

    fs::remove_file(state_dir.join("daemon.paused")).expect("resume marker");
    wait_until(Duration::from_secs(2), || matching_calls(&calls, "attach") > 1);
    assert_eq!(matching_calls(&calls, "attach"), 2, "resume starts exactly one stream");

    let _ = supervisor.0.kill();
    let _ = supervisor.0.wait();
    let _ = fs::remove_dir_all(root);
}
