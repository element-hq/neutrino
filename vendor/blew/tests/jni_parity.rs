//! Verify that every Kotlin `external fun` declaration in `BleCentralManager.kt`
//! and `BlePeripheralManager.kt` has a matching `#[unsafe(no_mangle)] extern "C"`
//! function in `jni_hooks.rs`, and vice-versa.
//!
//! This catches JNI linkage mismatches at test time rather than at Android runtime.

use std::collections::BTreeSet;

const JNI_HOOKS_RS: &str = include_str!("../src/platform/android/jni_hooks.rs");
const CENTRAL_KT: &str =
    include_str!("../android/src/main/java/org/jakebot/blew/BleCentralManager.kt");
const PERIPHERAL_KT: &str =
    include_str!("../android/src/main/java/org/jakebot/blew/BlePeripheralManager.kt");

/// Extract Kotlin `external fun` names from a `.kt` file.
fn kotlin_external_funs(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !trimmed.starts_with("external fun ") {
                return None;
            }
            // "external fun nativeFoo(" -> "nativeFoo"
            let rest = trimmed.strip_prefix("external fun ")?;
            let name = rest.split('(').next()?;
            Some(name.trim().to_string())
        })
        .collect()
}

/// Extract Rust JNI symbol names from `jni_hooks.rs`, keyed by Kotlin class.
/// Returns `(class_name, method_name)` pairs.
///
/// Pattern: `Java_org_jakebot_blew_{ClassName}_{methodName}`
fn rust_jni_symbols(source: &str) -> BTreeSet<(String, String)> {
    source
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let sym = trimmed
                .strip_prefix("pub unsafe extern \"C\" fn ")?
                .split('(')
                .next()?;
            let stem = sym.strip_prefix("Java_org_jakebot_blew_")?;
            let underscore = stem.find('_')?;
            let class = &stem[..underscore];
            let method = &stem[underscore + 1..];
            Some((class.to_string(), method.to_string()))
        })
        .collect()
}

fn rust_symbols_for_class(all: &BTreeSet<(String, String)>, class: &str) -> BTreeSet<String> {
    all.iter()
        .filter(|(c, _)| c == class)
        .map(|(_, m)| m.clone())
        .collect()
}

#[test]
fn central_jni_parity() {
    let kt_funs = kotlin_external_funs(CENTRAL_KT);
    let rust_all = rust_jni_symbols(JNI_HOOKS_RS);
    let rust_funs = rust_symbols_for_class(&rust_all, "BleCentralManager");

    let kt_only: Vec<_> = kt_funs.difference(&rust_funs).collect();
    let rust_only: Vec<_> = rust_funs.difference(&kt_funs).collect();

    let mut failures = Vec::new();
    if !kt_only.is_empty() {
        failures.push(format!(
            "Kotlin `external fun` with no Rust implementation: {kt_only:?}"
        ));
    }
    if !rust_only.is_empty() {
        failures.push(format!(
            "Rust JNI symbol with no Kotlin `external fun`: {rust_only:?}"
        ));
    }
    assert!(
        failures.is_empty(),
        "BleCentralManager:\n{}",
        failures.join("\n")
    );
}

#[test]
fn peripheral_jni_parity() {
    let kt_funs = kotlin_external_funs(PERIPHERAL_KT);
    let rust_all = rust_jni_symbols(JNI_HOOKS_RS);
    let rust_funs = rust_symbols_for_class(&rust_all, "BlePeripheralManager");

    let kt_only: Vec<_> = kt_funs.difference(&rust_funs).collect();
    let rust_only: Vec<_> = rust_funs.difference(&kt_funs).collect();

    let mut failures = Vec::new();
    if !kt_only.is_empty() {
        failures.push(format!(
            "Kotlin `external fun` with no Rust implementation: {kt_only:?}"
        ));
    }
    if !rust_only.is_empty() {
        failures.push(format!(
            "Rust JNI symbol with no Kotlin `external fun`: {rust_only:?}"
        ));
    }
    assert!(
        failures.is_empty(),
        "BlePeripheralManager:\n{}",
        failures.join("\n")
    );
}
