// The `links = "blew"` key requires a build script. Upstream blew used this to
// export its `android/` gradle-module path (`cargo:android_dir=…`) for
// tauri-plugin-blew-style consumers; the neutrino fork bundles the Kotlin from
// `bindings/` instead and removed the `android/` module, so there is nothing to
// export. Kept as a no-op to satisfy the `links` requirement.
fn main() {}
