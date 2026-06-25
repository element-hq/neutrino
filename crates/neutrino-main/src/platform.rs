use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(not(target_os = "android"))]
const DEFAULT_FILTER: &str =
    "neutrino_common=info,neutrino_http=info,neutrino_main=info,neutrino_ffi=info";

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
        // our crates (`neutrino_common`, `neutrino_ffi`, …) plus any custom `target:`
        // — and nothing from dependencies. A raw string prefix is the only way to
        // express that in one rule: EnvFilter/Targets match on `::`-delimited path
        // segments, so a `neutrino` directive would NOT match `neutrino_ffi`. A new
        // neutrino crate is picked up automatically. `RUST_LOG` still overrides.
        let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
            Ok(env) => env.boxed(),
            Err(_) => {
                tracing_subscriber::filter::filter_fn(|meta| meta.target().starts_with("neutrino"))
                    .boxed()
            }
        };

        let _ = tracing_subscriber::registry()
            .with(fmt_layer.with_filter(filter))
            .try_init();
    }
}
