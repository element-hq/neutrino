//! Integration-test peripheral (fixture for `integration_central`).
//!
//! Advertises a fixed service with three characteristics and an L2CAP
//! listener:
//!
//! * `STATUS_CHAR` (READ)        -- responds with `b"BLEW-OK"` on read
//! * `ECHO_CHAR` (WRITE, NOTIFY) -- echoes each write back via notify
//! * `PSM_CHAR` (READ)           -- returns the assigned L2CAP PSM (u16 LE)
//!
//! Plus an L2CAP CoC that echoes every byte received.
//!
//! Exists as the real-hardware counterpart to `integration_central`. Start
//! this on one host, then run the central on a second host. See the
//! `Testing on real hardware` section of the README.
//!
//! ```sh
//! cargo run --example integration_peripheral -p blew
//! ```
//!
//! Add `--keep-alive` to keep advertising after the first successful
//! integration run instead of exiting.

use blew::Peripheral;
use blew::gatt::props::{AttributePermissions, CharacteristicProperties};
use blew::gatt::service::{GattCharacteristic, GattService};
use blew::peripheral::{AdvertisingConfig, PeripheralRequest};
use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use uuid::Uuid;

const SVC_UUID: Uuid = Uuid::from_u128(0x626c_6577_0000_0000_0000_0000_0000_0001);
const STATUS_CHAR_UUID: Uuid = Uuid::from_u128(0x626c_6577_0000_0000_0000_0000_0000_0002);
const ECHO_CHAR_UUID: Uuid = Uuid::from_u128(0x626c_6577_0000_0000_0000_0000_0000_0003);
const PSM_CHAR_UUID: Uuid = Uuid::from_u128(0x626c_6577_0000_0000_0000_0000_0000_0004);
const STATUS_VALUE: &[u8] = b"BLEW-OK";
const BIDIRECTIONAL_SPEEDTEST_BYTES: usize = 500 * 1024;
const SPEEDTEST_CHUNK_SIZE: usize = 4096;
const UPLOAD_PROGRESS_INTERVAL: usize = 64 * 1024;
const CMD_ECHO: u8 = 0x01;
const CMD_UPLOAD: u8 = 0x02;
const CMD_DOWNLOAD: u8 = 0x03;
const CMD_BIDIRECTIONAL: u8 = 0x04;
const CENTRAL_PATTERN: u8 = 0xC1;
const PERIPHERAL_PATTERN: u8 = 0xD2;

type DynError = Box<dyn std::error::Error + Send + Sync>;

async fn read_command_header(ch: &mut blew::L2capChannel) -> Result<Option<(u8, usize)>, DynError> {
    let mut cmd = [0_u8; 1];
    match ch.read(&mut cmd).await {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("single-byte buffer read exceeded length"),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let mut len = [0_u8; 4];
    ch.read_exact(&mut len).await?;
    Ok(Some((cmd[0], u32::from_le_bytes(len) as usize)))
}

async fn send_pattern(
    ch: &mut blew::L2capChannel,
    total_len: usize,
    pattern: u8,
) -> Result<(), DynError> {
    let chunk = [pattern; SPEEDTEST_CHUNK_SIZE];
    let mut remaining = total_len;
    while remaining > 0 {
        let n = remaining.min(SPEEDTEST_CHUNK_SIZE);
        ch.write_all(&chunk[..n]).await?;
        remaining -= n;
    }
    Ok(())
}

async fn drain_exact_pattern_with_progress(
    ch: &mut blew::L2capChannel,
    total_len: usize,
    expected: u8,
) -> Result<(), DynError> {
    let mut remaining = total_len;
    let mut total_read = 0_usize;
    let mut last_reported = 0_usize;
    let mut buf = vec![0_u8; SPEEDTEST_CHUNK_SIZE];
    while remaining > 0 {
        let n = ch
            .read(&mut buf[..remaining.min(SPEEDTEST_CHUNK_SIZE)])
            .await?;
        if n == 0 {
            return Err("L2CAP speedtest hit EOF early".into());
        }
        if buf[..n].iter().any(|&b| b != expected) {
            return Err("L2CAP speedtest received unexpected payload bytes".into());
        }
        remaining -= n;
        total_read += n;
        if total_read - last_reported >= UPLOAD_PROGRESS_INTERVAL || remaining == 0 {
            ch.write_all(
                &u32::try_from(total_read)
                    .expect("progress fits in u32")
                    .to_le_bytes(),
            )
            .await?;
            last_reported = total_read;
        }
    }
    Ok(())
}

async fn serve_l2cap_protocol(mut ch: blew::L2capChannel) -> Result<(), DynError> {
    while let Some((cmd, len)) = read_command_header(&mut ch).await? {
        match cmd {
            CMD_ECHO => {
                let mut buf = vec![0_u8; len];
                ch.read_exact(&mut buf).await?;
                ch.write_all(&buf).await?;
            }
            CMD_UPLOAD => {
                drain_exact_pattern_with_progress(&mut ch, len, CENTRAL_PATTERN).await?;
            }
            CMD_DOWNLOAD => {
                send_pattern(&mut ch, len, PERIPHERAL_PATTERN).await?;
            }
            CMD_BIDIRECTIONAL => {
                if len != BIDIRECTIONAL_SPEEDTEST_BYTES {
                    return Err(format!("unexpected bidirectional size: {len}").into());
                }
                let (mut reader, mut writer) = tokio::io::split(ch);
                let sender = async move {
                    let chunk = [PERIPHERAL_PATTERN; SPEEDTEST_CHUNK_SIZE];
                    let mut remaining = len;
                    while remaining > 0 {
                        let n = remaining.min(SPEEDTEST_CHUNK_SIZE);
                        writer.write_all(&chunk[..n]).await?;
                        remaining -= n;
                    }
                    Ok::<_, std::io::Error>(())
                };
                let receiver = async move {
                    let mut remaining = len;
                    let mut buf = vec![0_u8; SPEEDTEST_CHUNK_SIZE];
                    while remaining > 0 {
                        let n = reader
                            .read(&mut buf[..remaining.min(SPEEDTEST_CHUNK_SIZE)])
                            .await?;
                        if n == 0 {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "bidirectional speedtest hit EOF early",
                            ));
                        }
                        if buf[..n].iter().any(|&b| b != CENTRAL_PATTERN) {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "bidirectional speedtest received unexpected payload bytes",
                            ));
                        }
                        remaining -= n;
                    }
                    Ok::<_, std::io::Error>(())
                };
                tokio::try_join!(sender, receiver)?;
                return Ok(());
            }
            _ => return Err(format!("unknown L2CAP command: {cmd}").into()),
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let keep_alive = env::args().skip(1).any(|arg| arg == "--keep-alive");
    let peripheral: Peripheral = Peripheral::new().await?;
    peripheral
        .add_service(&GattService {
            uuid: SVC_UUID,
            primary: true,
            characteristics: vec![
                GattCharacteristic {
                    uuid: STATUS_CHAR_UUID,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                },
                GattCharacteristic {
                    uuid: ECHO_CHAR_UUID,
                    properties: CharacteristicProperties::WRITE | CharacteristicProperties::NOTIFY,
                    permissions: AttributePermissions::WRITE,
                    value: vec![],
                    descriptors: vec![],
                },
                GattCharacteristic {
                    uuid: PSM_CHAR_UUID,
                    properties: CharacteristicProperties::READ,
                    permissions: AttributePermissions::READ,
                    value: vec![],
                    descriptors: vec![],
                },
            ],
        })
        .await?;

    let mut requests = peripheral.take_requests().expect("requests already taken");
    let (psm, mut l2cap_channels) = peripheral.l2cap_listener().await?;
    let (l2cap_done_tx, mut l2cap_done_rx) = mpsc::unbounded_channel();
    let psm_bytes = psm.value().to_le_bytes().to_vec();
    println!("L2CAP PSM = {}", psm.value());

    peripheral
        .start_advertising(&AdvertisingConfig {
            local_name: "blew-integration".into(),
            service_uuids: vec![SVC_UUID],
        })
        .await?;
    if keep_alive {
        println!("Advertising as \"blew-integration\" ... (Ctrl-C to stop)");
    } else {
        println!("Advertising as \"blew-integration\" for one integration run ...");
    }

    loop {
        tokio::select! {
            request = requests.next() => {
                let Some(request) = request else { break };
                match request {
                    PeripheralRequest::Read { char_uuid, responder, .. } => {
                        match char_uuid {
                            u if u == STATUS_CHAR_UUID => responder.respond(STATUS_VALUE.to_vec()),
                            u if u == PSM_CHAR_UUID => responder.respond(psm_bytes.clone()),
                            _ => responder.error(),
                        }
                    }
                    PeripheralRequest::Write {
                        client_id, char_uuid, value, responder, ..
                    } if char_uuid == ECHO_CHAR_UUID => {
                        if let Some(responder) = responder {
                            responder.success();
                        }
                        println!("  echo: {} bytes from {client_id}", value.len());
                        if let Err(e) = peripheral
                            .notify_characteristic(&client_id, ECHO_CHAR_UUID, value)
                            .await
                        {
                            eprintln!("  notify failed: {e}");
                        }
                    }
                    PeripheralRequest::Write { responder, .. } => {
                        if let Some(responder) = responder { responder.error(); }
                    }
                }
            }
            incoming = l2cap_channels.next() => {
                let Some(result) = incoming else { break };
                match result {
                    Ok((device_id, ch)) => {
                        println!("  L2CAP accept from {device_id}");
                        let l2cap_done_tx = l2cap_done_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = serve_l2cap_protocol(ch).await {
                                eprintln!("  L2CAP session failed: {e}");
                            }
                            let _ = l2cap_done_tx.send(());
                        });
                    }
                    Err(e) => eprintln!("  L2CAP accept error: {e}"),
                }
            }
            done = l2cap_done_rx.recv(), if !keep_alive => {
                if done.is_some() {
                    println!("Integration run complete, shutting down peripheral.");
                }
                break;
            }
        }
    }

    peripheral.stop_advertising().await?;
    Ok(())
}
