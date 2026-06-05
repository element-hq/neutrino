use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(not(target_os = "android"))]
const DEFAULT_FILTER: &str =
    "neutrino_common=info,neutrino_http=info,neutrino_main=info,neutrino_ffi=info,tower_http=info";

// On Android we hand everything *neutrino* emits (all crates at TRACE) to the
// writer and let Logcat do the filtering by tag / priority — a developer reading
// Logcat isn't blind to logs the desktop INFO filter would have dropped. We don't
// blanket-`trace` the world: that buries the neutrino logs under dependency noise
// (hyper, rusqlite, tokio, …), so dependencies stay scoped to `tower_http=info`
// (the request/response lines). `RUST_LOG` still overrides.
#[cfg(target_os = "android")]
const ANDROID_FILTER: &str = "neutrino_common=trace,neutrino_state=trace,neutrino_store=trace,neutrino_store_sqlite=trace,neutrino_http=trace,neutrino_main=trace,neutrino_ffi=trace,tower_http=info";

pub fn init_tracing() {
    #[cfg(not(target_os = "android"))]
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_FILTER.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    #[cfg(target_os = "android")]
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| ANDROID_FILTER.into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_level(false)
                .with_writer(paranoid_android::AndroidLogMakeWriter::new(
                    "io.element.neutrino".to_owned(),
                )),
        )
        .init();
}
