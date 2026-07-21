#[cfg(feature = "log")]
macro_rules! coap_log {
    (info, $($arg:expr),*) => { log::info!($($arg),*); };
    (debug, $($arg:expr),*) => { log::debug!($($arg),*); };
}

#[cfg(not(feature = "log"))]
macro_rules! coap_log {
    ($level:ident, $($arg:expr),*) => { $( let _ = $arg; )* }
}

macro_rules! coap_info {
    ($($arg:expr),*) => (coap_log!(info, $($arg),*));
}

macro_rules! coap_debug {
    ($($arg:expr),*) => (coap_log!(debug, $($arg),*));
}
