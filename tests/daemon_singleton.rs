use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
static NEXT_TEST: AtomicU64 = AtomicU64::new(0);
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
    children: Vec<Child>,
    detached_pids: Vec<i32>,
}
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(format!(
            "/tmp/hm-lock-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEST.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("hosts.toml"), "[hosts.test]\ntarget = 'unused'\n").unwrap();
        fs::write(root.join("test.hidden"), "hidden").unwrap();
        let listener = UnixListener::bind(root.join("api.sock")).unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                thread::spawn(move || {
                    let mut line = String::new();
                    if BufReader::new(stream.try_clone().unwrap())
                        .read_line(&mut line)
                        .is_err()
                    {
                        return;
                    }
                    let Ok(v) = serde_json::from_str::<Value>(&line) else {
                        return;
                    };
                    let result = if v["method"] == "session.snapshot" {
                        json!({"snapshot":{"workspaces":[],"tabs":[],"panes":[],"agents":[]}})
                    } else {
                        json!({})
                    };
                    let _ = writeln!(stream, "{}", json!({"id":v["id"],"result":result}));
                    if v["method"] == "events.subscribe" {
                        let mut rest = String::new();
                        let _ = BufReader::new(stream).read_line(&mut rest);
                    }
                });
            }
        });
        Self {
            root,
            children: vec![],
            detached_pids: vec![],
        }
    }
    fn command(&self, action: &str) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_herdr-mirror"));
        c.arg(action)
            .env("HERDR_MIRROR_STATE_DIR", &self.root)
            .env("HERDR_PLUGIN_CONFIG_DIR", &self.root)
            .env("HERDR_SOCKET_PATH", self.root.join("api.sock"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        c
    }
    fn spawn(&mut self, action: &str) -> usize {
        let child = self.command(action).spawn().unwrap();
        self.children.push(child);
        self.children.len() - 1
    }
    fn wait_pid(&self, pid: u32) {
        wait(|| {
            fs::read_to_string(self.root.join("daemon.pid"))
                .ok()
                .as_deref()
                == Some(&pid.to_string())
        });
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for pid in &self.detached_pids {
            unsafe {
                libc::kill(*pid, libc::SIGTERM);
            }
        }
        for c in &mut self.children {
            if matches!(c.try_wait(), Ok(None)) {
                unsafe { libc::kill(c.id() as i32, libc::SIGTERM); }
                let deadline = Instant::now() + Duration::from_secs(2);
                while matches!(c.try_wait(), Ok(None)) && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(20));
                }
                let _ = c.kill();
            }
            let _ = c.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn wait(mut check: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        if check() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("condition not reached within five seconds");
}

#[test]
fn second_direct_daemon_is_refused_even_if_pidfile_is_lost() {
    let mut f = Fixture::new();
    let first = f.spawn("daemon");
    let pid = f.children[first].id();
    f.wait_pid(pid);
    fs::remove_file(f.root.join("daemon.pid")).unwrap();
    let second = f.spawn("run");
    wait(|| f.children[second].try_wait().unwrap().is_some());
    assert!(
        !f.children[second].wait().unwrap().success(),
        "second daemon must refuse shared state"
    );
    assert!(f.children[first].try_wait().unwrap().is_none());
    assert!(
        !f.root.join("daemon.pid").exists(),
        "loser must not publish its PID"
    );
}

#[test]
fn exiting_daemon_preserves_a_pidfile_it_does_not_own() {
    let mut f = Fixture::new();
    let first = f.spawn("daemon");
    let pid = f.children[first].id();
    f.wait_pid(pid);
    // Wait for signal handlers / API startup, independent of pid publication.
    thread::sleep(Duration::from_millis(150));
    fs::write(f.root.join("daemon.pid"), "123456789").unwrap();
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
    wait(|| f.children[first].try_wait().unwrap().is_some());
    assert_eq!(
        fs::read_to_string(f.root.join("daemon.pid")).unwrap(),
        "123456789"
    );
}

#[test]
fn start_cannot_bypass_the_owner_when_pidfile_is_missing() {
    let mut f = Fixture::new();
    let first = f.spawn("daemon");
    let pid = f.children[first].id();
    f.wait_pid(pid);
    fs::remove_file(f.root.join("daemon.pid")).unwrap();
    let start = f.spawn("start");
    wait(|| f.children[start].try_wait().unwrap().is_some());
    assert!(f.children[start].wait().unwrap().success());
    assert!(
        !f.root.join("daemon.pid").exists(),
        "start must not launch or publish another daemon"
    );
    assert!(f.children[first].try_wait().unwrap().is_none());
}

#[test]
fn crashed_owner_releases_lock_and_stale_pid_does_not_block_restart() {
    let mut f = Fixture::new();
    let first = f.spawn("daemon");
    let pid = f.children[first].id();
    f.wait_pid(pid);
    f.children[first].kill().unwrap();
    f.children[first].wait().unwrap();
    let second = f.spawn("daemon");
    let next = f.children[second].id();
    f.wait_pid(next);
    assert!(f.children[second].try_wait().unwrap().is_none());
}

#[test]
fn startup_failure_does_not_publish_a_pid_or_keep_the_lock() {
    let mut f = Fixture::new();
    let mut c = f.command("daemon");
    c.env("HERDR_SOCKET_PATH", f.root.join("missing.sock"));
    let mut failed = c.spawn().unwrap();
    wait(|| failed.try_wait().unwrap().is_some());
    assert!(!failed.wait().unwrap().success());
    assert!(!f.root.join("daemon.pid").exists());
    let next = f.spawn("daemon");
    f.wait_pid(f.children[next].id());
}

#[test]
fn one_shot_cannot_write_maps_while_daemon_owns_them() {
    let mut f = Fixture::new();
    let first = f.spawn("daemon");
    let pid = f.children[first].id();
    f.wait_pid(pid);
    let once = f.spawn("once");
    wait(|| f.children[once].try_wait().unwrap().is_some());
    assert!(!f.children[once].wait().unwrap().success());
    f.wait_pid(pid);
}

#[test]
fn detached_start_publishes_owner_and_pause_allows_restart() {
    let mut f = Fixture::new();
    let starter = f.spawn("start");
    wait(|| f.children[starter].try_wait().unwrap().is_some());
    assert!(f.children[starter].wait().unwrap().success());
    let pid: i32 = fs::read_to_string(f.root.join("daemon.pid"))
        .unwrap()
        .parse()
        .unwrap();
    f.detached_pids.push(pid);
    let repeated = f.spawn("start");
    wait(|| f.children[repeated].try_wait().unwrap().is_some());
    assert!(f.children[repeated].wait().unwrap().success());
    f.wait_pid(pid as u32);
    let pause = f.spawn("pause");
    wait(|| f.children[pause].try_wait().unwrap().is_some());
    wait(|| !f.root.join("daemon.pid").exists());
    let restart = f.spawn("start");
    wait(|| f.children[restart].try_wait().unwrap().is_some());
    assert!(f.children[restart].wait().unwrap().success());
    let next: i32 = fs::read_to_string(f.root.join("daemon.pid"))
        .unwrap()
        .parse()
        .unwrap();
    f.detached_pids.push(next);
    assert_ne!(pid, next);
}

#[test]
fn concurrent_restore_processes_keep_both_map_updates() {
    use std::os::fd::AsRawFd;
    let f = Fixture::new();
    fs::write(f.root.join("test-map.json"), serde_json::to_vec(&json!({
        "panes": {
            "first": {"localId":"local-first", "tombstone":true},
            "second": {"localId":"local-second", "tombstone":true},
            "keep": {"localId":"local-keep"}
        }
    })).unwrap()).unwrap();
    // Gate both real CLI writers behind the same native lock before allowing
    // either to load the map. The lock is separate from the replaced map inode.
    let lock = fs::OpenOptions::new().create(true).truncate(false).read(true).write(true)
        .open(f.root.join("test-map.lock")).unwrap();
    assert_eq!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) }, 0);
    let mut first = f.command("restore").args(["test", "first"]).spawn().unwrap();
    let mut second = f.command("restore").args(["test", "second"]).spawn().unwrap();
    thread::sleep(Duration::from_millis(200));
    let first_waiting = first.try_wait().unwrap().is_none();
    let second_waiting = second.try_wait().unwrap().is_none();
    drop(lock);
    let first_status = first.wait().unwrap();
    let second_status = second.wait().unwrap();
    assert!(first_waiting && second_waiting, "restore bypassed the host map lock");
    assert!(first_status.success() && second_status.success());
    let state: Value = serde_json::from_slice(&fs::read(f.root.join("test-map.json")).unwrap()).unwrap();
    assert_eq!(state["panes"], json!({"keep":{"localId":"local-keep", "seq":0}}));
}

#[test]
fn owner_refusal_quiesces_controls_and_owner_return_reconnects_without_a_poke() {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new();
    fs::remove_file(f.root.join("test.hidden")).unwrap();
    let projection = f.root.join("hosts.toml");
    fs::write(&projection, "[hosts.test]\ntarget = 'demo'\ndesired_state = 'running'\n").unwrap();
    let bin = f.root.join("bin");
    fs::create_dir(&bin).unwrap();
    let transport = bin.join("odx-connect");
    fs::write(&transport, r#"#!/usr/bin/env python3
import json, os, sys, time
from pathlib import Path
root = Path(os.environ['TEST_OWNER_ROOT'])
with (root / 'calls').open('a') as log:
    log.write(json.dumps(sys.argv[1:]) + '\n')
assert len(sys.argv) == 3 and sys.argv[1] == 'demo', sys.argv
if not (root / 'running').exists():
    print('odx-connect: workspace demo desired state is stopped', file=sys.stderr)
    sys.exit(2)
if (root / 'offline').exists():
    print('owner unavailable', file=sys.stderr)
    sys.exit(1)
command = sys.argv[2]
if 'status --json' in command:
    print(json.dumps({'server': {'running': True, 'version': '0.8.0', 'socket': '/tmp/herdr.sock'}}))
elif command == 'command -v socat || true':
    print('/usr/bin/socat')
elif command.startswith('socat - UNIX-CONNECT:'):
    request = json.loads(sys.stdin.readline())
    result = {'snapshot': {'workspaces': [], 'tabs': [], 'panes': [], 'agents': []}} if request['method'] == 'session.snapshot' else {}
    print(json.dumps({'id': request['id'], 'result': result}), flush=True)
    if request['method'] == 'events.subscribe':
        while not (root / 'offline').exists():
            time.sleep(0.02)
else:
    raise AssertionError(command)
"#).unwrap();
    fs::set_permissions(&transport, fs::Permissions::from_mode(0o755)).unwrap();
    // Any accidental SSH launch is visible, including a detached master.
    fs::write(bin.join("ssh"), "#!/bin/sh\necho forbidden >> \"$TEST_OWNER_ROOT/ssh-calls\"\nexit 99\n").unwrap();
    fs::set_permissions(bin.join("ssh"), fs::Permissions::from_mode(0o755)).unwrap();
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let child = f.command("daemon")
        .env("TEST_OWNER_ROOT", &f.root)
        .env("PATH", std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(&inherited))).unwrap())
        .spawn().unwrap();
    f.children.push(child);
    let health = f.root.join("host-health/test.json");
    wait(|| fs::read_to_string(&health).unwrap_or_default().contains("waiting for running projection"));
    let stopped: Value = serde_json::from_str(&fs::read_to_string(&health).unwrap()).unwrap();
    assert!(stopped.get("nextRetryUnix").is_none(), "a stopped workspace has no retry deadline");
    thread::sleep(Duration::from_millis(1200));
    assert_eq!(fs::read_to_string(f.root.join("calls")).unwrap().lines().count(), 1,
        "owner refusal is not retried, including the status retry");
    fs::write(f.root.join("running"), "yes").unwrap();
    fs::write(&projection, "[hosts.test]\ntarget = 'demo'\ndesired_state = 'running'\n\n").unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !fs::read_to_string(&health).unwrap_or_default().contains("connected and synced") {
        assert!(Instant::now() < until, "health: {:?}; log: {:?}; calls: {:?}",
            fs::read_to_string(&health), fs::read_to_string(f.root.join("daemon.log")), fs::read_to_string(f.root.join("calls")));
        thread::sleep(Duration::from_millis(20));
    }
    fs::write(f.root.join("offline"), "offline").unwrap();
    wait(|| fs::read_to_string(&health).unwrap_or_default().contains("disconnected"));
    fs::remove_file(f.root.join("offline")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !fs::read_to_string(&health).unwrap_or_default().contains("connected and synced") {
        assert!(Instant::now() < deadline, "owner stream must reconnect without manual wake");
        thread::sleep(Duration::from_millis(20));
    }
    let calls = fs::read_to_string(f.root.join("calls")).unwrap();
    assert!(calls.contains("status --json") && calls.contains("socat - UNIX-CONNECT:/tmp/herdr.sock"));
    assert!(!f.root.join("ssh-calls").exists(), "no OpenSSH process is launched");
}
