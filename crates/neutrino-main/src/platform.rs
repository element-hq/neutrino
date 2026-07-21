use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(not(target_os = "android"))]
const DEFAULT_FILTER: &str =
    "neutrino_event=info,neutrino_http=info,neutrino_main=info,neutrino_ffi=info";

// Compact, Logcat-friendly event formatter, ported from matrix-rust-sdk's
// `EventFormatter::for_logcat`. Logcat already records the timestamp and
// priority for every line, so we omit both and emit just:
//   `target: message | file:line | spans: a{..} > b{..}`
// Paths under the crates.io registry are shortened to `<crates.io>/…` so a
// dependency's frame doesn't drown the line in the cargo cache prefix.
#[cfg(target_os = "android")]
mod logcat_format {
    use tracing::{Event, Subscriber};
    use tracing_subscriber::{
        fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields, format::Writer},
        registry::LookupSpan,
    };

    pub(crate) struct LogcatEventFormatter;

    impl LogcatEventFormatter {
        fn write_filename(writer: &mut Writer<'_>, filename: &str) -> std::fmt::Result {
            const CRATES_IO_PATH_MATCHER: &str = ".cargo/registry/src/index.crates.io";
            let crates_io_filename = filename
                .split_once(CRATES_IO_PATH_MATCHER)
                .and_then(|(_, rest)| rest.split_once('/').map(|(_, rest)| rest));

            if let Some(filename) = crates_io_filename {
                writer.write_str("<crates.io>/")?;
                writer.write_str(filename)
            } else {
                writer.write_str(filename)
            }
        }
    }

    impl<S, N> FormatEvent<S, N> for LogcatEventFormatter
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        fn format_event(
            &self,
            ctx: &FmtContext<'_, S, N>,
            mut writer: Writer<'_>,
            event: &Event<'_>,
        ) -> std::fmt::Result {
            let meta = event.metadata();

            write!(writer, "{}: ", meta.target())?;

            ctx.format_fields(writer.by_ref(), event)?;

            if let Some(filename) = meta.file() {
                writer.write_str(" | ")?;
                Self::write_filename(&mut writer, filename)?;
                if let Some(line_number) = meta.line() {
                    write!(writer, ":{line_number}")?;
                }
            }

            if let Some(scope) = ctx.event_scope() {
                writer.write_str(" | spans: ")?;

                let mut first = true;

                for span in scope.from_root() {
                    if !first {
                        writer.write_str(" > ")?;
                    }

                    first = false;

                    write!(writer, "{}", span.name())?;

                    if let Some(fields) = &span.extensions().get::<FormattedFields<N>>()
                        && !fields.is_empty()
                    {
                        write!(writer, "{{{fields}}}")?;
                    }
                }
            }

            writeln!(writer)
        }
    }
}

pub fn init_tracing() {
    // `try_init` (not `init`): idempotent, so a second `entrypoint` in the same
    // process — two nodes in a test, or a re-entrant embed — is a no-op rather
    // than a panic on the already-set global subscriber.
    #[cfg(not(target_os = "android"))]
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_FILTER.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    #[cfg(target_os = "android")]
    {
        use tracing_subscriber::{Layer, filter::FilterExt};

        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .event_format(logcat_format::LogcatEventFormatter)
            .with_writer(paranoid_android::AndroidLogMakeWriter::new(
                "io.element.neutrino".to_owned(),
            ));

        // Default (no RUST_LOG): every level from every `neutrino*` target — all of
        // our crates (`neutrino_event`, `neutrino_ffi`, …) plus any custom `target:`
        // — PLUS the federation transport stack we depend on. A raw string prefix is
        // the only way to express the `neutrino*` rule: EnvFilter/Targets match on
        // `::`-delimited path segments, so a `neutrino` directive would NOT match
        // `neutrino_ffi`. A new neutrino crate is picked up automatically.
        //
        // The embedded federation medium is injected from out-of-tree crates
        // (`iroh_ble_transport`, `blew`, `iroh` — the iroh/BLE composition).
        // Without their targets, BLE advertise/scan/discovery/connect failures
        // are completely invisible — the entire transport is silent, which is
        // exactly the class of failure we must never hide. The prefixes below
        // are plain strings, not dependencies: they cost nothing when the
        // medium isn't linked in, and keep it debuggable when it is. Include
        // the BLE stack down to DEBUG and the QUIC layer at INFO (its
        // DEBUG/TRACE is per-packet noise). Note `Level::TRACE > … > Level::ERROR`, so `level <= DEBUG` keeps
        // everything except TRACE. `RUST_LOG` still overrides the whole thing.
        // `boxed()` is disambiguated via UFCS: both `FilterExt` and `Layer` define it
        // for these types (EnvFilter/FilterFn are each both a filter and a layer), so
        // method syntax is ambiguous — we want the `Filter` one.
        let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
            Ok(env) => FilterExt::boxed(env),
            Err(_) => FilterExt::boxed(tracing_subscriber::filter::filter_fn(|meta| {
                let target = meta.target();
                if target.starts_with("neutrino") {
                    true
                } else if target.starts_with("iroh_ble_transport") || target.starts_with("blew") {
                    *meta.level() <= tracing::Level::DEBUG
                } else if target.starts_with("iroh") {
                    *meta.level() <= tracing::Level::INFO
                } else {
                    false
                }
            })),
        };

        let _ = tracing_subscriber::registry()
            .with(fmt_layer.with_filter(filter))
            .try_init();
    }

    install_panic_logger();
}

/// Route panics through the tracing subscriber installed above (and so to logcat
/// on Android), once. Without this a panic on a server task — an `unwrap` in a
/// serve future, say — unwinds to Rust's default hook, which writes to stderr;
/// Android discards stderr, so the server would vanish (its listener dropped)
/// with nothing in the log. The previous hook is chained so any host-installed
/// reporting (e.g. matrix-rust-sdk's) still runs.
fn install_panic_logger() {
    use std::sync::Once;
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info
                .location()
                .map(|l| l.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            let message = info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            // `target: "neutrino_main"` so the Android filter (targets starting
            // with `neutrino`) captures it.
            tracing::error!(target: "neutrino_main", %location, "panic in neutrino server: {message}");
            previous(info);
        }));
    });
}
