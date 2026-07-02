//! Global state for Android L2CAP channels.
//!
//! Each channel uses a `tokio::io::DuplexStream` pair -- one half becomes the
//! `L2capChannel`, the other is bridged via JNI to Kotlin's BluetoothSocket.
//!
//! **Kotlin->Rust data path:** Kotlin read thread -> JNI `nativeOnL2capChannelData`
//! -> `mpsc::UnboundedSender` -> tokio drain task -> DuplexStream writer -> L2capChannel read.
//!
//! **Rust->Kotlin data path:** L2capChannel write -> DuplexStream -> tokio read task
//! -> JNI `writeL2cap` -> Kotlin OutputStream.

use std::collections::HashMap;
use std::sync::OnceLock;

use jni::{jni_sig, jni_str};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::error::{BlewError, BlewResult};
use crate::l2cap::L2capChannel;
use crate::l2cap::types::Psm;
use crate::types::DeviceId;

use super::jni_globals::{central_class, jvm, peripheral_class};

const DUPLEX_BUF_SIZE: usize = 65536;
const L2CAP_READ_BUF_SIZE: usize = 4096;

struct L2capState {
    pending_server: Mutex<Option<oneshot::Sender<BlewResult<Psm>>>>,
    pending_open: Mutex<HashMap<String, oneshot::Sender<BlewResult<L2capChannel>>>>,
    accept_tx: Mutex<Option<mpsc::UnboundedSender<BlewResult<(DeviceId, L2capChannel)>>>>,
    data_tx: Mutex<HashMap<i32, mpsc::UnboundedSender<Vec<u8>>>>,
}

static STATE: OnceLock<L2capState> = OnceLock::new();
static TOKIO_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

fn state() -> &'static L2capState {
    STATE.get().expect("L2CAP state not initialized")
}

pub(crate) fn tokio_handle() -> &'static tokio::runtime::Handle {
    TOKIO_HANDLE.get().expect("tokio handle not initialized")
}

pub(crate) fn init_statics() {
    let _ = STATE.set(L2capState {
        pending_server: Mutex::new(None),
        pending_open: Mutex::new(HashMap::new()),
        accept_tx: Mutex::new(None),
        data_tx: Mutex::new(HashMap::new()),
    });
    let _ = TOKIO_HANDLE.set(tokio::runtime::Handle::current());
}

pub(crate) fn set_pending_server(tx: oneshot::Sender<BlewResult<Psm>>) {
    *state().pending_server.lock() = Some(tx);
}

pub(crate) fn complete_server_open(result: BlewResult<Psm>) {
    if let Some(s) = STATE.get() {
        if let Some(tx) = s.pending_server.lock().take() {
            let _ = tx.send(result);
        }
    }
}

pub(crate) fn set_accept_tx(tx: mpsc::UnboundedSender<BlewResult<(DeviceId, L2capChannel)>>) {
    *state().accept_tx.lock() = Some(tx);
}

pub(crate) fn set_pending_open(addr: String, tx: oneshot::Sender<BlewResult<L2capChannel>>) {
    state().pending_open.lock().insert(addr, tx);
}

fn close_socket(socket_id: i32, is_server: bool) {
    let _ = jvm().attach_current_thread(|env| {
        let class = if is_server {
            peripheral_class()
        } else {
            central_class()
        };
        let _ = env.call_static_method(
            class,
            jni_str!("closeL2cap"),
            jni_sig!("(I)V"),
            &[socket_id.into()],
        );
        Ok::<_, jni::errors::Error>(())
    });
}

pub(crate) fn on_channel_opened(device_addr: &str, socket_id: i32, from_server: bool) {
    let (app_half, bridge_half) = tokio::io::duplex(DUPLEX_BUF_SIZE);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge_half);

    let (data_tx, mut data_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    if let Some(s) = STATE.get() {
        s.data_tx.lock().insert(socket_id, data_tx);
    }

    let handle = TOKIO_HANDLE.get().expect("tokio handle not initialized");

    handle.spawn(async move {
        while let Some(data) = data_rx.recv().await {
            if bridge_writer.write_all(&data).await.is_err() {
                break;
            }
        }
    });

    let is_server = from_server;
    handle.spawn(async move {
        let mut buf = vec![0u8; L2CAP_READ_BUF_SIZE];
        loop {
            match bridge_reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let data = buf[..n].to_vec();
                    let result = jvm().attach_current_thread(|env| {
                        let j_data = env.byte_array_from_slice(&data)?;
                        let class = if is_server {
                            peripheral_class()
                        } else {
                            central_class()
                        };
                        env.call_static_method(
                            class,
                            jni_str!("writeL2cap"),
                            jni_sig!("(I[B)V"),
                            &[socket_id.into(), (&j_data).into()],
                        )?;
                        Ok::<_, jni::errors::Error>(())
                    });
                    if result.is_err() {
                        break;
                    }
                }
            }
        }
        close_socket(socket_id, is_server);
    });

    let channel = L2capChannel::from_duplex_with_close_hook(app_half, move || {
        close_socket(socket_id, from_server);
    });

    if from_server {
        if let Some(s) = STATE.get() {
            if let Some(tx) = s.accept_tx.lock().as_ref() {
                let device_id = DeviceId(device_addr.to_string());
                if tx.send(Ok((device_id, channel))).is_err() {
                    tracing::warn!(
                        socket_id,
                        "L2CAP accept receiver dropped, discarding channel"
                    );
                }
            }
        }
    } else if let Some(s) = STATE.get() {
        if let Some(tx) = s.pending_open.lock().remove(device_addr) {
            let _ = tx.send(Ok(channel));
        }
    }
}

pub(crate) fn on_channel_data(socket_id: i32, data: &[u8]) {
    if let Some(s) = STATE.get() {
        if let Some(tx) = s.data_tx.lock().get(&socket_id) {
            let _ = tx.send(data.to_vec());
        }
    }
}

pub(crate) fn on_channel_closed(socket_id: i32) {
    if let Some(s) = STATE.get() {
        s.data_tx.lock().remove(&socket_id);
    }
}

pub(crate) fn on_channel_error(device_addr: &str, error: String) {
    if let Some(s) = STATE.get() {
        if let Some(tx) = s.pending_open.lock().remove(device_addr) {
            let _ = tx.send(Err(BlewError::L2cap {
                source: error.into(),
            }));
        }
    }
}
