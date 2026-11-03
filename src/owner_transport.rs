//! Workspace transport and local retry admission.
//!
//! `target` in hosts.toml is the canonical workspace accepted by odx-connect.
//! The projection writer publishes `desired_state = "running" | "stopped"`
//! atomically and retains the host row while stopped. Daemon and standalone
//! pane processes reread this file without contacting the workspace. After a
//! named stopped refusal, only a newer running publication releases the latch.
use tokio::process::Command;

pub const PROGRAM: &str = "odx-connect";

pub fn command(workspace: &str, command: &str) -> Command {
    let mut child = Command::new(PROGRAM);
    child.arg(workspace).arg(command);
    child
}

pub fn stopped_refusal(reason: &str) -> bool {
    reason
        .lines()
        .any(|line| line.contains("workspace ") && line.ends_with(" desired state is stopped"))
}

/// A refusal is released only by a newer, running local projection. Keeping
/// the native file revision avoids treating a stale `running` as a restart.
#[derive(Default)]
pub struct Admission {
    refused_revision: Option<Option<std::time::SystemTime>>,
}

fn projection(workspace: &str) -> (Option<bool>, Option<std::time::SystemTime>) {
    for directory in crate::util::config_candidates() {
        let path = directory.join("hosts.toml");
        use std::io::Read;
        let Ok(mut file) = std::fs::File::open(&path) else {
            continue;
        };
        let revision = file.metadata().ok().and_then(|m| m.modified().ok());
        let mut text = String::new();
        if file.read_to_string(&mut text).is_err() {
            return (None, revision);
        }
        let running = text.parse::<toml::Value>().ok().and_then(|config| {
            let hosts = config.get("hosts")?.as_table()?;
            let host = hosts.get(workspace).or_else(|| {
                hosts.values().find(|host| {
                    host.get("target").and_then(toml::Value::as_str) == Some(workspace)
                })
            })?;
            match host.get("desired_state")?.as_str()? {
                "running" => Some(true),
                "stopped" => Some(false),
                _ => None,
            }
        });
        return (running, revision);
    }
    (None, None)
}

impl Admission {
    pub fn refuse(&mut self, workspace: &str) {
        self.refused_revision = Some(projection(workspace).1);
    }

    pub fn may_connect(&self, workspace: &str) -> bool {
        let (running, revision) = projection(workspace);
        match self.refused_revision {
            Some(refused) => running == Some(true) && revision != refused,
            None => running != Some(false),
        }
    }
}
