use std::sync::Mutex;

use neutrino_common::Config;

uniffi::setup_scaffolding!("neutrino");

/// Server configuration supplied by the embedding app. Mirrors
/// [`neutrino_common::Config`]'s user-facing fields; the embedder must provide a
/// persistent `storage_path` (its app data directory) so rooms survive restarts.
#[derive(uniffi::Record)]
pub struct NeutrinoConfig {
    /// Directory the SQLite database is stored in (e.g. the app's `filesDir`).
    /// Created if absent; the server owns the filename within it.
    pub storage_path: String,
    /// This homeserver's name (the `:server` part of user/room IDs).
    pub server_name: String,
    /// Localpart of the single configured user (`@<localpart>:<server_name>`).
    pub localpart: String,
    /// Address to bind the HTTP listener to, e.g. `127.0.0.1:8008`.
    pub bind_addr: String,
    /// Max concurrent in-flight outbound federation transactions. Clamped to
    /// ≥ 1 (0 is meaningless for the sender pool's semaphore).
    pub outbound_concurrency: u32,
}

impl From<NeutrinoConfig> for Config {
    fn from(c: NeutrinoConfig) -> Self {
        Config {
            server_name: c.server_name,
            bind_addr: c.bind_addr,
            localpart: c.localpart,
            outbound_concurrency: (c.outbound_concurrency as usize).max(1),
            storage_path: c.storage_path.into(),
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

/// Spawn the Tokio runtime and run the server with the supplied `config`.
/// Returns a handle that can be used to gracefully shut the server down.
#[uniffi::export]
pub fn start(config: NeutrinoConfig) -> NeutrinoHandle {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let config: Config = config.into();
    std::thread::spawn(move || {
        let rt = async_compat::get_runtime_handle();
        rt.block_on(async {
            tokio::select! {
                res = neutrino_main::entrypoint(config) => {
                    if let Err(e) = res {
                        eprintln!("server exited: {e}");
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
    fn neutrino_config_maps_to_common_config() {
        let cfg: Config = NeutrinoConfig {
            storage_path: "/data/app".to_string(),
            server_name: "example.org".to_string(),
            localpart: "bob".to_string(),
            bind_addr: "127.0.0.1:9999".to_string(),
            outbound_concurrency: 5,
        }
        .into();
        assert_eq!(cfg.storage_path, std::path::PathBuf::from("/data/app"));
        assert_eq!(cfg.server_name, "example.org");
        assert_eq!(cfg.localpart, "bob");
        assert_eq!(cfg.bind_addr, "127.0.0.1:9999");
        assert_eq!(cfg.outbound_concurrency, 5);
    }

    #[test]
    fn outbound_concurrency_zero_clamps_to_one() {
        // 0 is meaningless for the sender pool's semaphore; the mapping floors it.
        let cfg: Config = NeutrinoConfig {
            storage_path: "/d".to_string(),
            server_name: "s".to_string(),
            localpart: "u".to_string(),
            bind_addr: "a".to_string(),
            outbound_concurrency: 0,
        }
        .into();
        assert_eq!(cfg.outbound_concurrency, 1);
    }
}
