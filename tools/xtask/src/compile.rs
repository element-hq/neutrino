use clap::Args;

use crate::sh::{self, Cmd};

/// Android ABIs cargo-ndk builds by default (matches the old compile.sh).
const DEFAULT_ABIS: &[&str] = &["armeabi-v7a", "arm64-v8a", "x86", "x86_64"];

#[derive(Args, Default)]
pub struct CompileArgs {
    /// Android ABI to build (repeatable). Defaults to all four.
    #[arg(short = 't', long = "target")]
    pub targets: Vec<String>,
}

/// Shared-library filename uniffi-bindgen loads, per host OS.
pub fn lib_filename() -> &'static str {
    if cfg!(target_os = "macos") {
        "libneutrino.dylib"
    } else {
        "libneutrino.so"
    }
}

pub fn run(args: &CompileArgs) -> Result<(), sh::Error> {
    let root = sh::workspace_root();
    let abis: Vec<&str> = if args.targets.is_empty() {
        DEFAULT_ABIS.to_vec()
    } else {
        args.targets.iter().map(String::as_str).collect()
    };

    // 1. Host build so uniffi-bindgen can load the cdylib. Must match the
    //    Android feature set (step 2): bindings are generated from THIS cdylib
    //    (step 3), so a feature whose exports are missing here would be missing
    //    from the Kotlin bindings even though the device .so had it.
    //    (The iroh/BLE medium builds its own .aar out of tree — see iroh_repo.)
    Cmd::new("cargo", &root)
        .args(["build", "-p", "neutrino-ffi", "--release"])
        .run()?;

    // 2. Android targets via cargo-ndk → jniLibs.
    let mut ndk = Cmd::new("cargo", &root).args([
        "ndk",
        "-o",
        "./bindings/src/main/jniLibs",
        "--manifest-path",
        "./Cargo.toml",
    ]);
    for abi in &abis {
        ndk = ndk.args(["-t", abi]);
    }
    ndk.args(["build", "-p", "neutrino-ffi", "--release"])
        .run()?;

    // 3. Generate the Kotlin bindings from the host cdylib.
    let lib = format!("./target/release/{}", lib_filename());
    Cmd::new("cargo", &root)
        .args(["run", "--bin", "uniffi-bindgen", "generate", "--library"])
        .arg(lib)
        .args([
            "--language",
            "kotlin",
            "--out-dir",
            "./bindings/src/main/java",
        ])
        .run()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lib_filename_matches_host() {
        if cfg!(target_os = "macos") {
            assert_eq!(lib_filename(), "libneutrino.dylib");
        } else {
            assert_eq!(lib_filename(), "libneutrino.so");
        }
    }
}
