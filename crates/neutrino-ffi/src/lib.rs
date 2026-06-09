use std::sync::Mutex;

uniffi::setup_scaffolding!("neutrino");

/// FFI-facing server configuration. Mirrors `neutrino_common::Config` so EX
/// Android can fully configure the embedded homeserver. Kept here (not on the
/// common `Config`) so UniFFI stays out of the common crates — see the
/// crate-structure rule in CLAUDE.md. All fields are required; defaults live
/// in `Config::default`/`from_env` for the dev binary.
#[derive(uniffi::Record)]
pub struct NeutrinoConfig {
    pub server_name: String,
    pub bind_addr: String,
    pub localpart: String,
    /// Absolute path to a writable directory the host owns (e.g. Android's
    /// `context.filesDir`). The DB is `<storage_dir>/neutrino.db`.
    pub storage_dir: String,
    pub outbound_concurrency: u32,
}

impl From<NeutrinoConfig> for neutrino_main::Config {
    fn from(c: NeutrinoConfig) -> Self {
        neutrino_main::Config {
            server_name: c.server_name,
            bind_addr: c.bind_addr,
            localpart: c.localpart,
            storage_dir: std::path::PathBuf::from(c.storage_dir),
            // Zero is meaningless for the sender semaphore; clamp to 1, matching
            // `Config::from_env`'s outbound-concurrency floor.
            outbound_concurrency: (c.outbound_concurrency.max(1)) as usize,
        }
    }
}

#[derive(uniffi::Object)]
pub struct NeutrinoHandle {
    tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[uniffi::export]
impl NeutrinoHandle {
    pub fn shutdown(&self) {
        if let Some(tx) = self.tx.lock().unwrap().take() {
            tx.send(()).unwrap()
        }
    }
}

/// Spawn the Tokio runtime and begin polling the server entrypoint with the
/// supplied configuration. Returns a handle that can gracefully shut the
/// server down.
#[uniffi::export]
pub fn start(config: NeutrinoConfig) -> NeutrinoHandle {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let config: neutrino_main::Config = config.into();
    std::thread::spawn(move || {
        let rt = async_compat::get_runtime_handle();
        rt.block_on(async {
            tokio::select! {
                res = neutrino_main::entrypoint(config) => {
                    if let Err(e) = res {
                        eprintln!("entrypoint exited: {e}");
                    }
                },
                _ = rx => {}
            }
        });
    });
    NeutrinoHandle {
        tx: Mutex::new(Some(tx)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutrino_config_maps_to_internal_config() {
        let nc = NeutrinoConfig {
            server_name: "hs.example".to_string(),
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 0, // must clamp to 1
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.server_name, "hs.example");
        assert_eq!(cfg.bind_addr, "127.0.0.1:8008");
        assert_eq!(cfg.localpart, "alice");
        assert_eq!(cfg.storage_dir, std::path::PathBuf::from("/data/neutrino"));
        assert_eq!(cfg.outbound_concurrency, 1);
    }

    #[test]
    fn neutrino_config_passes_through_nonzero_concurrency() {
        // Guards against a regression where the clamp emits a constant 1
        // rather than flooring: a non-zero value must pass through unchanged.
        let nc = NeutrinoConfig {
            server_name: "hs.example".to_string(),
            bind_addr: "127.0.0.1:8008".to_string(),
            localpart: "alice".to_string(),
            storage_dir: "/data/neutrino".to_string(),
            outbound_concurrency: 4,
        };
        let cfg: neutrino_main::Config = nc.into();
        assert_eq!(cfg.outbound_concurrency, 4);
    }
}
