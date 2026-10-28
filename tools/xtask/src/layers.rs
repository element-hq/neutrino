//! The crate layering described in ARCHITECTURE.md, checked against
//! `cargo metadata` by `cargo test`.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

/// Each workspace member and exactly the members it depends on. Dev-dependencies
/// are unrestricted.
const ALLOWED: &[(&str, &[&str])] = &[
    ("neutrino-ctl", &[]),
    ("neutrino-event", &[]),
    ("neutrino-lb", &[]),
    ("neutrino-testkit", &[]),
    ("neutrino-room", &["neutrino-event"]),
    ("neutrino-store", &["neutrino-event", "neutrino-room"]),
    (
        "neutrino-store-sqlite",
        &["neutrino-event", "neutrino-room", "neutrino-store"],
    ),
    (
        "neutrino-engine",
        &["neutrino-event", "neutrino-room", "neutrino-store"],
    ),
    (
        "neutrino-http",
        &[
            "neutrino-ctl",
            "neutrino-event",
            "neutrino-room",
            "neutrino-store",
            "neutrino-engine",
        ],
    ),
    (
        "neutrino-main",
        &[
            "neutrino-ctl",
            "neutrino-event",
            "neutrino-http",
            "neutrino-lb",
            "neutrino-store",
            "neutrino-store-sqlite",
        ],
    ),
    ("neutrino-ffi", &["neutrino-main"]),
    ("neutrino", &["neutrino-main"]),
    ("converge", &["neutrino-testkit"]),
    ("xtask", &[]),
    ("uniffi-bindgen", &[]),
];

/// Workspace member → the members it depends on (normal and build deps).
type Graph = BTreeMap<String, BTreeSet<String>>;

fn violations(graph: &Graph) -> Vec<String> {
    let allowed: BTreeMap<&str, &[&str]> = ALLOWED.iter().copied().collect();
    let mut out = Vec::new();
    for (krate, deps) in graph {
        let Some(ok) = allowed.get(krate.as_str()) else {
            out.push(format!("{krate} has no entry in the layering table"));
            continue;
        };
        for dep in deps.iter().filter(|d| !ok.contains(&d.as_str())) {
            out.push(format!("{krate} must not depend on {dep}"));
        }
        for dep in ok.iter().filter(|d| !deps.contains(**d)) {
            out.push(format!(
                "{krate} no longer depends on {dep}; drop it from the table"
            ));
        }
    }
    for krate in allowed.keys().filter(|k| !graph.contains_key(**k)) {
        out.push(format!(
            "{krate} is in the layering table but not the workspace"
        ));
    }
    out
}

fn workspace_graph() -> Graph {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run cargo metadata");
    assert!(out.status.success(), "cargo metadata failed");
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout).expect("metadata JSON");
    let packages = meta["packages"].as_array().expect("packages array");
    let members: BTreeSet<&str> = packages.iter().filter_map(|p| p["name"].as_str()).collect();
    packages
        .iter()
        .filter_map(|p| {
            let deps = p["dependencies"]
                .as_array()?
                .iter()
                .filter(|d| d["kind"].as_str() != Some("dev"))
                .filter_map(|d| d["name"].as_str())
                .filter(|name| members.contains(name))
                .map(str::to_owned)
                .collect();
            Some((p["name"].as_str()?.to_owned(), deps))
        })
        .collect()
}

#[test]
fn workspace_respects_layering() {
    let found = violations(&workspace_graph());
    assert!(found.is_empty(), "{}", found.join("\n"));
}

#[test]
fn violations_reports_each_kind() {
    let mut graph: Graph = ALLOWED
        .iter()
        .map(|(k, deps)| (k.to_string(), deps.iter().map(|d| d.to_string()).collect()))
        .collect();
    assert!(violations(&graph).is_empty());

    graph
        .get_mut("neutrino-room")
        .expect("table entry")
        .insert("neutrino-engine".into());
    graph
        .get_mut("neutrino-ffi")
        .expect("table entry")
        .remove("neutrino-main");
    graph.insert("neutrino-new".into(), BTreeSet::new());
    graph.remove("xtask");
    assert_eq!(
        violations(&graph),
        [
            "neutrino-ffi no longer depends on neutrino-main; drop it from the table",
            "neutrino-new has no entry in the layering table",
            "neutrino-room must not depend on neutrino-engine",
            "xtask is in the layering table but not the workspace",
        ]
    );
}
