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
                .event_format(logcat_format::LogcatEventFormatter)
                .with_writer(paranoid_android::AndroidLogMakeWriter::new(
                    "io.element.neutrino".to_owned(),
                )),
        )
        .init();
}
