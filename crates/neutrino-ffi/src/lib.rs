use std::sync::Mutex;

uniffi::setup_scaffolding!("neutrino");

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

/// Spawn the Tokio runtime and begin polling the server entrypoint. Returns a
/// handle that can be used to gracefully shutdown the server.
#[uniffi::export]
pub fn start() -> NeutrinoHandle {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(|| {
        let rt = async_compat::get_runtime_handle();
        rt.block_on(async {
            tokio::select! {
                _ = neutrino_main::entrypoint() => {},
                _ = rx => {}
            }
        });
    });
    NeutrinoHandle {
        tx: Mutex::new(Some(tx)),
    }
}
