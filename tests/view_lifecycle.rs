//! Exercise the public view command in a tile (PTY), with only SSH and the
//! local Herdr socket replaced. No owner panes, daemon or hosts are touched.
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    os::{
        fd::FromRawFd,
        unix::{fs::PermissionsExt, net::UnixListener},
    },
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Fixture {
    root: PathBuf,
    children: Vec<Child>,
    screen: Arc<Mutex<String>>,
}
impl Fixture {
    fn new() -> Self {
        // Short enough for the view's Unix input socket on macOS.
        let root = PathBuf::from(format!(
            "/tmp/hmv620-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        fs::create_dir_all(root.join("bin")).unwrap();
        fs::create_dir_all(root.join("daemon-config")).unwrap();
        fs::write(
            root.join("hosts.toml"),
            "[hosts.test]\ntarget = 'fake-host'\nremote_bin = 'herdr'\nalways_control = true\n",
        )
        .unwrap();
        // The real daemon owns its ordinary lifetime/lock/pause state, but
        // reconciles only an isolated, hidden control host. The view's map
        // stays fixed so remote API emulation cannot manufacture recovery.
        fs::write(
            root.join("daemon-config/hosts.toml"),
            "[hosts.control]\ntarget = 'unused'\n",
        )
        .unwrap();
        fs::write(root.join("control.hidden"), "hidden").unwrap();
        fs::write(
            root.join("test-map.json"),
            r#"{"panes":{"remote:p1":{"localId":"sidebar:p1"}}}"#,
        )
        .unwrap();
        fs::write(root.join("bin/ssh"), r#"#!/bin/sh
printf '%s\n' "$*" >> "$HM_CALLS"
case "$*" in
  *"ps -o args="*) printf 'cleanup\n' >> "$HM_EVENTS"; exit 0 ;;
  *"terminal session"*) ;;
  *) exec /bin/sh ;;
esac
if test -f "$HM_ROOT/unreachable"; then echo 'guest unreachable' >&2; exit 1; fi
printf 'attach %s\n' "$$" >> "$HM_EVENTS"
printf 'herdr-mirror-remote-pid %s\n' "$$"
if ! test -f "$HM_ROOT/hold-frame"; then
  printf '{"type":"terminal.frame","full":true,"width":80,"height":24,"bytes":"U1RSRUFNLVJFQURZ"}\n'
fi
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$HM_INPUT"
  case "$line" in
    *terminal.input*) printf '{"type":"terminal.frame","full":true,"width":80,"height":24,"bytes":"SU5QVVQtREVMSVZFUkVE"}\n' ;;
    *terminal.release*) exit 0 ;;
  esac
done
"#).unwrap();
        fs::set_permissions(root.join("bin/ssh"), fs::Permissions::from_mode(0o755)).unwrap();
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
                        json!({"workspaces":[],"tabs":[],"panes":[],"agents":[]})
                    } else {
                        json!({})
                    };
                    let _ = writeln!(stream, "{}", json!({"id":v["id"],"result":result}));
                    if v["method"] == "events.subscribe" {
                        let _ = BufReader::new(stream).read_line(&mut String::new());
                    }
                });
            }
        });
        Self {
            root,
            children: vec![],
            screen: Arc::default(),
        }
    }
    fn command(&self, action: &str) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_herdr-mirror"));
        let path = std::env::var_os("PATH").unwrap_or_default();
        c.arg(action)
            .env("HERDR_MIRROR_STATE_DIR", &self.root)
            .env("HERDR_PLUGIN_CONFIG_DIR", &self.root)
            .env("HERDR_SOCKET_PATH", self.root.join("api.sock"))
            .env("HOME", &self.root)
            .env("HERDR_PANE_ID", "tile:p1")
            .env("HM_ROOT", &self.root)
            .env("HM_CALLS", self.root.join("calls"))
            .env("HM_EVENTS", self.root.join("events"))
            .env("HM_INPUT", self.root.join("input"))
            .env(
                "PATH",
                std::env::join_paths(
                    std::iter::once(self.root.join("bin")).chain(std::env::split_paths(&path)),
                )
                .unwrap(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        c
    }
    fn daemon(&mut self) -> usize {
        let mut command = self.command("daemon");
        command.env("HERDR_PLUGIN_CONFIG_DIR", self.root.join("daemon-config"));
        let child = command.spawn().unwrap();
        let pid = child.id();
        self.children.push(child);
        wait(|| fs::read_to_string(self.root.join("daemon.pid")).ok() == Some(pid.to_string()));
        self.children.len() - 1
    }
    fn action(&self, action: &str, args: &[&str]) {
        let out = self.command(action).args(args).output().unwrap();
        assert!(
            out.status.success(),
            "{action}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    fn view(&mut self) -> usize {
        let (mut master, mut slave) = (-1, -1);
        let mut size = libc::winsize {
            ws_row: 30,
            ws_col: 100,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut size,
                )
            },
            0
        );
        let mut reader = unsafe { File::from_raw_fd(master) };
        let slave = unsafe { File::from_raw_fd(slave) };
        let child = self
            .command("view")
            .arg("sidebar:p1")
            .stdin(Stdio::piped())
            .stdout(slave)
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let screen = self.screen.clone();
        thread::spawn(move || {
            let mut buf = [0; 4096];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 {
                    break;
                }
                screen
                    .lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(&buf[..n]));
            }
        });
        self.children.push(child);
        self.children.len() - 1
    }
    fn text(&self, file: &str) -> String {
        fs::read_to_string(self.root.join(file)).unwrap_or_default()
    }
    fn seen(&self, text: &str) -> bool {
        self.screen.lock().unwrap().contains(text)
    }
    fn clear_screen(&self) {
        self.screen.lock().unwrap().clear();
    }
    fn assert_live(&mut self, tile: usize) {
        assert!(
            self.children[tile].try_wait().unwrap().is_none(),
            "same view process must survive"
        );
    }
    fn input(&mut self, tile: usize) {
        self.clear_screen();
        let before = self.text("input").len();
        self.children[tile]
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"hello\r")
            .unwrap();
        wait(|| self.seen("INPUT-DELIVERED"));
        // Expected bytes are independent of the fixture's frame response.
        assert!(
            self.text("input")[before..].contains("aGVsbG8N"),
            "input reached the remote terminal"
        );
    }
    fn attach_count(&self) -> usize {
        self.text("events")
            .lines()
            .filter(|l| l.starts_with("attach "))
            .count()
    }
    fn disconnect_transport(&self) {
        let events = self.text("events");
        let pid: i32 = events
            .lines()
            .filter_map(|l| l.strip_prefix("attach "))
            .next_back()
            .unwrap()
            .parse()
            .unwrap();
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for child in &mut self.children {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
        }
        for child in &mut self.children {
            let _ = child.wait();
        }
        if thread::panicking() {
            eprintln!(
                "screen: {}\nevents: {}\ninput: {}\ncalls: {}",
                self.screen.lock().unwrap(),
                self.text("events"),
                self.text("input"),
                self.text("calls")
            );
            for child in &mut self.children {
                if let Some(mut err) = child.stderr.take() {
                    let mut text = String::new();
                    let _ = err.read_to_string(&mut text);
                    eprintln!("stderr: {text}");
                }
            }
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}
#[track_caller]
fn wait(mut predicate: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("condition not reached within five seconds");
}

#[test]
fn one_view_survives_suspend_show_without_dialing_the_hidden_host() {
    let mut f = Fixture::new();
    f.daemon();
    let tile = f.view();
    wait(|| f.seen("STREAM-READY"));
    f.input(tile);
    let claim = f.text("view-claims/test--remote_p1.json");
    assert!(!claim.is_empty(), "view owns the remote-pane claim");
    wait(|| {
        f.root
            .join("remote-clients/fake_host_view--remote_p1.pid")
            .exists()
    });
    f.clear_screen();
    f.action("suspend", &["test"]);
    wait(|| f.seen("host suspended"));
    assert!(
        f.text("events").contains("cleanup"),
        "suspend reaps the separate view client record"
    );
    assert!(!f
        .root
        .join("remote-clients/fake_host_view--remote_p1.pid")
        .exists());
    let calls = f.text("calls");
    f.children[tile]
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"ignored\r")
        .unwrap();
    // Multiple reconnect deadlines, with input, cannot open a hidden host.
    thread::sleep(Duration::from_millis(1200));
    assert_eq!(
        f.text("calls"),
        calls,
        "no stream, metadata or cleanup dial while suspended"
    );
    f.assert_live(tile);
    assert_eq!(f.text("view-claims/test--remote_p1.json"), claim);
    f.clear_screen();
    f.action("show", &["test"]);
    wait(|| f.seen("STREAM-READY"));
    assert_eq!(f.attach_count(), 2);
    f.input(tile);
    f.assert_live(tile);
}

#[test]
fn one_view_survives_daemon_restart_and_keeps_disconnection_visible_until_a_frame() {
    let mut f = Fixture::new();
    let daemon = f.daemon();
    let tile = f.view();
    wait(|| f.seen("STREAM-READY"));
    f.input(tile);
    f.clear_screen();
    f.action("pause", &[]);
    f.children[daemon].wait().unwrap();
    wait(|| f.seen("Mirror paused"));
    f.assert_live(tile);
    fs::write(f.root.join("hold-frame"), "hold").unwrap();
    fs::remove_file(f.root.join("pause/daemon.paused")).unwrap();
    let replacement = f.daemon();
    wait(|| f.attach_count() == 2);
    // A spawned SSH process is not proof of a connected terminal. Resizing
    // repaints the status even when the server has sent no frame yet.
    f.clear_screen();
    unsafe {
        libc::kill(f.children[tile].id() as i32, libc::SIGWINCH);
    }
    wait(|| f.seen("Mirror paused"));
    f.input(tile); // a real frame now clears the persistent reason
    f.clear_screen();
    unsafe {
        libc::kill(f.children[tile].id() as i32, libc::SIGWINCH);
    }
    thread::sleep(Duration::from_millis(100));
    assert!(!f.seen("Mirror paused"));
    fs::write(f.root.join("unreachable"), "offline").unwrap();
    f.disconnect_transport();
    wait(|| f.seen("guest unreachable"));
    f.assert_live(tile);
    f.clear_screen();
    fs::remove_file(f.root.join("unreachable")).unwrap();
    fs::remove_file(f.root.join("hold-frame")).unwrap();
    wait(|| f.seen("STREAM-READY"));
    f.input(tile);
    f.assert_live(tile);
    // An abrupt daemon replacement has no pause marker. A lost data-plane
    // connection still reconnects without releasing the tile's claim.
    let claim = f.text("view-claims/test--remote_p1.json");
    f.children[replacement].kill().unwrap();
    f.children[replacement].wait().unwrap();
    f.daemon();
    f.clear_screen();
    f.disconnect_transport();
    wait(|| f.seen("STREAM-READY"));
    f.input(tile);
    f.assert_live(tile);
    assert_eq!(f.text("view-claims/test--remote_p1.json"), claim);
}

#[test]
fn a_view_born_suspended_waits_for_show_and_consumes_daemon_health_without_dialing() {
    let mut f = Fixture::new();
    f.daemon();
    fs::write(f.root.join("test.hidden"), "hidden").unwrap();
    let tile = f.view();
    wait(|| f.seen("host suspended"));
    assert_eq!(
        f.text("calls"),
        "",
        "even the first attach is forbidden while hidden"
    );
    f.assert_live(tile);
    fs::create_dir_all(f.root.join("host-health")).unwrap();
    fs::write(f.root.join("host-health/test.json"),
        r#"{"summary":"disconnected (connection refused)","atIso":"2026-11-01T12:00:00Z","nextRetryUnix":9999999999.0}"#).unwrap();
    f.clear_screen();
    f.action("show", &["test"]);
    wait(|| f.seen("guest unreachable"));
    thread::sleep(Duration::from_millis(1200));
    assert_eq!(f.text("calls"), "", "the daemon owns the health trial");
    f.clear_screen();
    fs::remove_file(f.root.join("host-health/test.json")).unwrap();
    wait(|| f.seen("STREAM-READY"));
    f.input(tile);
    // The health gate can also withdraw an already streaming view.
    fs::write(f.root.join("host-health/test.tmp"),
        r#"{"summary":"disconnected (connection refused)","atIso":"2026-11-01T12:00:00Z","nextRetryUnix":9999999999.0}"#).unwrap();
    f.clear_screen();
    fs::rename(
        f.root.join("host-health/test.tmp"),
        f.root.join("host-health/test.json"),
    )
    .unwrap();
    wait(|| f.seen("guest unreachable"));
    let calls = f.text("calls");
    thread::sleep(Duration::from_millis(1200));
    assert_eq!(f.text("calls"), calls);
    f.clear_screen();
    fs::remove_file(f.root.join("host-health/test.json")).unwrap();
    wait(|| f.seen("STREAM-READY"));
    f.input(tile);
    f.assert_live(tile);
}
