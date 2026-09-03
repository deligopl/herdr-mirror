use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use chrono::{TimeZone, Utc};
use rosemary_core::daemon::registry::RegistrySnapshot;
use rosemary_core::db::authored::Binding;
use rosemary_core::herdr::{
    EndpointObservation, HerdrPresence, HerdrReachability, HerdrStatus, WorkspaceObservation,
    SUPPORTED_PROTOCOL,
};
use rosemary2_server::roster::{derive_observed, HerdrProject};
use serde::Serialize;

#[derive(Clone, Serialize)]
struct Inputs {
    project_member: bool,
    endpoint_count: usize,
    endpoint_present: bool,
    connected: bool,
    protocol_compatible: bool,
    active_binding: bool,
    interactive_ready: bool,
    presence: &'static str,
}

#[derive(Serialize)]
struct Case {
    name: &'static str,
    inputs: Inputs,
    available_conductors: usize,
}

fn registry() -> RegistrySnapshot {
    RegistrySnapshot {
        connections: Vec::new(),
        stray_proxies: Default::default(),
        agents: HashMap::new(),
        presence: HashMap::new(),
        models_by_transport: HashMap::new(),
    }
}

fn derive_case(name: &'static str, inputs: Inputs) -> Case {
    let now = Utc.with_ymd_and_hms(2026, 9, 3, 12, 0, 0).unwrap();
    let workspace_id = "fixture-workspace";
    let conductor = "fixture-conductor-rosie";
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "rosemary_project".to_string(),
        if inputs.project_member { "garden" } else { "other" }.to_string(),
    );
    let workspace = WorkspaceObservation {
        workspace_id: workspace_id.into(),
        label: "fixture".into(),
        metadata,
        worktree: None,
    };
    let presence = match inputs.presence {
        "idle" => HerdrPresence::Idle,
        "done" => HerdrPresence::Done,
        "working" => HerdrPresence::Working,
        other => panic!("unsupported fixture presence {other}"),
    };
    let endpoints = (0..inputs.endpoint_count)
        .map(|index| EndpointObservation {
            pane_id: format!("fixture-pane-{index}"),
            workspace_id: workspace_id.into(),
            name: Some(conductor.into()),
            harness: Some("codex".into()),
            harness_session: Some(format!("fixture-session-{index}")),
            interactive_ready: inputs.interactive_ready,
            presence,
            state_change_seq: 1,
            metadata: BTreeMap::new(),
            observed_at: now,
            present: inputs.endpoint_present,
        })
        .collect();
    let herdr = HerdrStatus {
        herdr_dir: PathBuf::from("/fixture/herdr"),
        socket_path: PathBuf::from("/fixture/herdr.sock"),
        reachability: if inputs.connected {
            HerdrReachability::Connected
        } else {
            HerdrReachability::Unreachable
        },
        compatible: inputs.protocol_compatible,
        protocol: Some(SUPPORTED_PROTOCOL),
        observed_at: now,
        snapshot_revision: 1,
        connected_at: inputs.connected.then_some(now),
        last_handshake_at: Some(now),
        unreachable_since: (!inputs.connected).then_some(now),
        error: None,
        workspaces: vec![workspace],
        endpoints,
    };
    let project = HerdrProject {
        name: "garden".into(),
        repo_key: None,
        checkouts: BTreeSet::new(),
    };
    let bindings = inputs.active_binding.then(|| Binding {
        binding_id: "fixture-binding".into(),
        ticket: "fixture-ticket".into(),
        agent_name: conductor.into(),
        bound_at: now,
    });
    let roster = derive_observed(
        registry(),
        bindings.as_ref().map(std::slice::from_ref).unwrap_or_default(),
        &herdr,
        &project,
    );
    Case {
        name,
        inputs,
        available_conductors: roster
            .herdr_workspaces
            .iter()
            .map(|workspace| workspace.available_conductors)
            .sum(),
    }
}

fn main() {
    let available = Inputs {
        project_member: true,
        endpoint_count: 1,
        endpoint_present: true,
        connected: true,
        protocol_compatible: true,
        active_binding: false,
        interactive_ready: true,
        presence: "idle",
    };
    let mut cases = vec![
        derive_case("available_idle", available.clone()),
        derive_case("available_done", Inputs { presence: "done", ..available.clone() }),
        derive_case("wrong_project", Inputs { project_member: false, ..available.clone() }),
        derive_case("missing_endpoint", Inputs { endpoint_count: 0, ..available.clone() }),
        derive_case("endpoint_not_present", Inputs { endpoint_present: false, ..available.clone() }),
        derive_case("disconnected", Inputs { connected: false, ..available.clone() }),
        derive_case("protocol_incompatible", Inputs { protocol_compatible: false, ..available.clone() }),
        derive_case("ambiguous_endpoints", Inputs { endpoint_count: 2, ..available.clone() }),
        derive_case("active_binding", Inputs { active_binding: true, ..available.clone() }),
        derive_case("not_ready", Inputs { interactive_ready: false, ..available.clone() }),
        derive_case("working_not_idle_or_done", Inputs { presence: "working", ..available }),
    ];
    cases.sort_by_key(|case| case.name);
    let fixture = serde_json::json!({
        "source": {
            "repository": "https://github.com/deligopl/rosemary2.git",
            "commit": std::env::var("ROSEMARY_SOURCE_COMMIT").unwrap(),
            "derivation": "crates/server/src/roster.rs::derive_observed",
            "derivation_file_sha256": std::env::var("ROSEMARY_ROSTER_SHA256").unwrap(),
            "generator": "tools/rosemary-roster-contract/main.rs"
        },
        "cases": cases
    });
    println!("{}", serde_json::to_string_pretty(&fixture).unwrap());
}
