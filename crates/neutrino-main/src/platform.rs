use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_FILTER: &str = "neutrino_core=info,neutrino_sqlite=info,neutrino_http=info,neutrino_main=info,neutrino_ffi=info";

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
                .unwrap_or_else(|_| DEFAULT_FILTER.into()),
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
